//! Shopping Sessions. One static-style pipeline: parse the CSVs in place,
//! dedup, union linked ids, sessionize, attribute clicks, write Parquet.

mod id;
mod parquet;
mod radix;
mod time;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::thread;
use std::time::Instant;

use id::{is_slow, slow_index, slow_key, try_pack, write_id};
use memmap2::Mmap;
use time::{DAY_MS, GAP_MS, LATE_MS, WEEK_MS, days_from_civil, format_day, parse_ts};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const EVENT_SHARDS: usize = 32;
const CLICK_SHARDS: usize = 8;
const MAX_CATS: usize = 64;

struct Window {
    start: i64,
    end: i64,
    click_start: i64,
    start_day: i32,
}

#[derive(Clone, Copy)]
struct EventRec {
    id: u64,
    user: u64,
    ts: i64,
    ingest: i64,
    price: u32,
    cat: u8,
    kind: u8,
}

#[derive(Clone, Copy)]
struct ClickRec {
    ts: i64,
    user: u64,
    id: u64,
    camp: u16,
}

#[derive(Clone, Copy)]
struct Ev {
    person: u32,
    ts: i64,
    price: u32,
    cat: u8,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Cl {
    person: u32,
    ts: i64,
    id: u64,
    camp: u16,
}

struct Surv {
    user: u64,
    ts: i64,
    price: u32,
    cat: u8,
    kind: u8,
}

struct UserRow {
    key: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: u16,
}

struct CampRow {
    camp: u16,
    day: i32,
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

struct Partial {
    rows: Vec<UserRow>,
    sessions: Vec<[u32; 14]>,
    purchasing: Vec<[u32; 14]>,
    revenue: Vec<[u64; 14]>,
    assisted: Vec<[u32; 14]>,
}

struct Camp {
    name: Vec<u8>,
    channel: Vec<u8>,
}

struct SlowIntern {
    map: HashMap<Vec<u8>, u32>,
    strings: Vec<Vec<u8>>,
}

impl SlowIntern {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            strings: Vec::new(),
        }
    }

    fn intern(&mut self, s: &[u8]) -> u64 {
        if let Some(&i) = self.map.get(s) {
            return slow_key(i);
        }
        let i = self.strings.len() as u32;
        self.map.insert(s.to_vec(), i);
        self.strings.push(s.to_vec());
        slow_key(i)
    }
}

struct Shared {
    slow: Mutex<SlowIntern>,
}

struct ThreadOut {
    events: Vec<Vec<EventRec>>,
    clicks: Vec<Vec<ClickRec>>,
    links: Vec<(u64, u64)>,
}

impl ThreadOut {
    fn new() -> Self {
        Self {
            events: (0..256).map(|_| Vec::new()).collect(),
            clicks: (0..256).map(|_| Vec::new()).collect(),
            links: Vec::new(),
        }
    }
}

struct Ctx<'a> {
    prod_cat: &'a [u8],
    unknown: u8,
    prod_other: &'a HashMap<Vec<u8>, u8>,
    camp_by_num: &'a [i32],
    camp_other: &'a HashMap<Vec<u8>, u16>,
    window: &'a Window,
}

#[derive(Clone, Copy)]
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

impl<T> SendPtr<T> {
    unsafe fn write(self, index: usize, value: T) {
        unsafe { self.0.add(index).write(value) };
    }
    unsafe fn slice<'a>(self, start: usize, len: usize) -> &'a mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.0.add(start), len) }
    }
}

struct Tables {
    users: Vec<UserRow>,
    camps_out: Vec<CampRow>,
    camps: Vec<Camp>,
    cat_names: Vec<Vec<u8>>,
    slow: SlowIntern,
    organic: u16,
}

fn main() {
    let data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());
    let out = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".to_string());
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or_else(|| thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    if let Err(err) = run(Path::new(&data), Path::new(&out), threads) {
        eprintln!("solve: {err}");
        std::process::exit(1);
    }
}

fn run(data: &Path, out: &Path, threads: usize) -> Result<(), Box<dyn std::error::Error>> {
    let t0 = Instant::now();
    let tables = compute(data, threads)?;
    let t1 = Instant::now();
    write_tables(out, &tables)?;
    let t2 = Instant::now();
    eprintln!(
        "rows user={} camp={} compute_ms={} write_ms={} total_ms={}",
        tables.users.len(),
        tables.camps_out.len(),
        (t1 - t0).as_millis(),
        (t2 - t1).as_millis(),
        (t2 - t0).as_millis()
    );
    Ok(())
}

fn compute(data: &Path, threads: usize) -> Result<Tables, Box<dyn std::error::Error>> {
    let threads = threads.max(1);
    let t_stage = Instant::now();
    let window = load_window(&data.join("manifest.json"))?;
    let (prod_cat, unknown, prod_other, cat_names) = load_products(&data.join("products.csv"))?;
    let (camp_by_num, mut camps, camp_other) = load_campaigns(&data.join("campaigns.csv"))?;
    let organic = camps.len() as u16;
    camps.push(Camp {
        name: b"organic".to_vec(),
        channel: b"organic".to_vec(),
    });
    let n_camps = camps.len();
    let n_real = organic as usize;

    let mut files: Vec<(u8, Mmap)> = Vec::new();
    for (kind, folder) in [(0u8, "events"), (1, "clicks"), (2, "links")] {
        for path in list_csv(&data.join(folder)) {
            let file = File::open(&path)?;
            let len = file.metadata()?.len();
            if len == 0 {
                continue;
            }
            if len > usize::MAX as u64 {
                return Err(format!("file too large: {}", path.display()).into());
            }
            let map = unsafe { Mmap::map(&file)? };
            files.push((kind, map));
        }
    }

    struct Chunk {
        file: u16,
        kind: u8,
        start: usize,
        end: usize,
    }
    let mut chunks = Vec::new();
    for (idx, (kind, map)) in files.iter().enumerate() {
        chunks.extend(split_chunks(idx as u16, *kind, map));
    }

    let t_mmap = t_stage.elapsed();
    let t_stage = Instant::now();
    let shared = Shared {
        slow: Mutex::new(SlowIntern::new()),
    };
    let ctx = Ctx {
        prod_cat: &prod_cat,
        unknown,
        prod_other: &prod_other,
        camp_by_num: &camp_by_num,
        camp_other: &camp_other,
        window: &window,
    };
    let mut outs: Vec<ThreadOut> = (0..threads).map(|_| ThreadOut::new()).collect();
    let next = AtomicUsize::new(0);
    thread::scope(|scope| {
        for out in &mut outs {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(chunk) = chunks.get(i) else { break };
                    let buf = &files[chunk.file as usize].1[chunk.start..chunk.end];
                    match chunk.kind {
                        0 => parse_events(buf, &ctx, &shared, out),
                        1 => parse_clicks(buf, &ctx, &shared, out),
                        _ => parse_links(buf, &shared, out),
                    }
                }
            });
        }
    });
    let t_parse = t_stage.elapsed();
    let t_stage = Instant::now();
    let slow = shared.slow.into_inner().unwrap_or_else(|e| e.into_inner());

    let mut event_buckets: Vec<Vec<EventRec>> = (0..256).map(|_| Vec::new()).collect();
    let mut click_buckets: Vec<Vec<ClickRec>> = (0..256).map(|_| Vec::new()).collect();
    let mut links = Vec::new();
    for out in &mut outs {
        for (bucket, rows) in out.events.iter_mut().enumerate() {
            event_buckets[bucket].append(rows);
        }
        for (bucket, rows) in out.clicks.iter_mut().enumerate() {
            click_buckets[bucket].append(rows);
        }
        links.append(&mut out.links);
    }
    drop(outs);
    drop(files);

    let events_surv = dedup_event_buckets(event_buckets, threads);
    let clicks_surv = dedup_click_buckets(click_buckets, threads);

    let t_dedup = t_stage.elapsed();
    let t_stage = Instant::now();
    let (user_map, keys) = radix_user_ids(&events_surv, &clicks_surv, &links, threads);

    let n_users = keys.len();
    let mut parent: Vec<u32> = (0..n_users as u32).collect();
    let mut rank = vec![0u8; n_users];
    let mut min_key = keys.clone();
    for &(a, b) in &links {
        let Some(ia) = user_map.get(a) else { continue };
        let Some(ib) = user_map.get(b) else { continue };
        union(&mut parent, &mut rank, &mut min_key, ia, ib, &slow);
    }
    for i in 0..n_users {
        find(&mut parent, i as u32);
    }

    let t_union = t_stage.elapsed();
    let t_stage = Instant::now();
    let evs = remap_events(&events_surv, &user_map, &parent, threads);
    let cls = remap_clicks(&clicks_surv, &user_map, &parent, threads);
    drop(user_map);
    drop(parent);
    drop(links);

    let n_cats = cat_names.len();
    if n_cats > MAX_CATS {
        return Err(format!("too many categories: {n_cats}").into());
    }

    let (mut evs, ev_starts) = group_parallel(evs, n_users, threads, |e| e.person as usize);
    let (mut cls, cl_starts) = group_parallel(cls, n_users, threads, |c| c.person as usize);
    let agg = AggCtx {
        min_key: &min_key,
        slow: &slow,
        cat_names: &cat_names,
        n_users,
        n_real,
        organic,
        start_day: window.start_day,
        n_cats,
    };
    let pts = balance_ranges(&ev_starts, threads);
    struct Job {
        p0: usize,
        p1: usize,
        partial: Partial,
    }
    let mut jobs: Vec<Job> = pts
        .windows(2)
        .map(|w| Job {
            p0: w[0],
            p1: w[1],
            partial: empty_partial(n_camps),
        })
        .collect();
    let ev_ptr = SendPtr(evs.as_mut_ptr());
    let cl_ptr = SendPtr(cls.as_mut_ptr());
    let ev_starts_ref = &ev_starts;
    let cl_starts_ref = &cl_starts;
    let agg_ref = &agg;
    thread::scope(|scope| {
        for job in &mut jobs {
            scope.spawn(move || {
                let p0 = job.p0;
                let p1 = job.p1;
                let e0 = ev_starts_ref[p0] as usize;
                let e1 = ev_starts_ref[p1] as usize;
                let c0 = cl_starts_ref[p0] as usize;
                let c1 = cl_starts_ref[p1] as usize;
                let ev = unsafe { ev_ptr.slice(e0, e1 - e0) };
                let cl = unsafe { cl_ptr.slice(c0, c1 - c0) };
                aggregate_owned(
                    ev,
                    e0,
                    ev_starts_ref,
                    cl,
                    c0,
                    cl_starts_ref,
                    p0,
                    p1,
                    agg_ref,
                    &mut job.partial,
                );
            });
        }
    });

    let mut users = Vec::new();
    let mut sessions = vec![[0u32; 14]; n_camps];
    let mut purchasing = vec![[0u32; 14]; n_camps];
    let mut revenue = vec![[0u64; 14]; n_camps];
    let mut assisted = vec![[0u32; 14]; n_camps];
    for work in jobs {
        users.extend(work.partial.rows);
        for c in 0..n_camps {
            for d in 0..14 {
                sessions[c][d] += work.partial.sessions[c][d];
                purchasing[c][d] += work.partial.purchasing[c][d];
                revenue[c][d] += work.partial.revenue[c][d];
                assisted[c][d] += work.partial.assisted[c][d];
            }
        }
    }
    let mut camps_out = Vec::new();
    let mut user_sessions = 0i64;
    let mut user_revenue = 0i64;
    for row in &users {
        user_sessions += row.sessions;
        user_revenue += row.revenue;
    }
    let mut camp_sessions = 0i64;
    let mut camp_revenue = 0i64;
    let mut non_org = 0i64;
    let mut assist_sum = 0i64;
    for c in 0..n_camps {
        for d in 0..14 {
            let s = sessions[c][d] as i64;
            let a = assisted[c][d] as i64;
            if s == 0 && a == 0 {
                continue;
            }
            let rev = revenue[c][d] as i64;
            camps_out.push(CampRow {
                camp: c as u16,
                day: window.start_day + d as i32,
                sessions: s,
                purchasing: purchasing[c][d] as i64,
                revenue: rev,
                assisted: a,
            });
            camp_sessions += s;
            camp_revenue += rev;
            assist_sum += a;
            if c != organic as usize {
                non_org += s;
            }
        }
    }
    let t_agg = t_stage.elapsed();
    eprintln!(
        "stages ms mmap={} parse={} dedup={} union={} agg={}",
        t_mmap.as_millis(),
        t_parse.as_millis(),
        t_dedup.as_millis(),
        t_union.as_millis(),
        t_agg.as_millis()
    );
    if user_sessions != camp_sessions || user_revenue != camp_revenue || assist_sum < non_org {
        return Err(format!(
            "invariant failed: sessions {user_sessions} vs {camp_sessions}, revenue {user_revenue} vs {camp_revenue}, assisted {assist_sum} vs non-organic {non_org}"
        )
        .into());
    }

    Ok(Tables {
        users,
        camps_out,
        camps,
        cat_names,
        slow,
        organic,
    })
}

struct Work {
    events: Vec<Ev>,
    clicks: Vec<Cl>,
    partial: Partial,
}

struct AggCtx<'a> {
    min_key: &'a [u64],
    slow: &'a SlowIntern,
    cat_names: &'a [Vec<u8>],
    n_users: usize,
    n_real: usize,
    organic: u16,
    start_day: i32,
    n_cats: usize,
}

fn dedup_shards(shards: &mut [(Vec<u64>, Vec<EventRec>, Vec<Surv>)], threads: usize) {
    fn go<'a>(
        scope: &'a thread::Scope<'a, '_>,
        shards: &'a mut [(Vec<u64>, Vec<EventRec>, Vec<Surv>)],
        threads: usize,
    ) {
        if shards.is_empty() {
            return;
        }
        if threads <= 1 || shards.len() == 1 {
            scope.spawn(|| {
                for shard in shards.iter_mut() {
                    shard.2 = dedup_events(&shard.0, &shard.1);
                    shard.0.clear();
                    shard.1.clear();
                }
            });
            return;
        }
        let mid = shards.len() / 2;
        let (left, right) = shards.split_at_mut(mid);
        let lt = (threads / 2).max(1);
        go(scope, left, lt);
        go(scope, right, threads - lt);
    }
    thread::scope(|scope| go(scope, shards, threads.max(1)));
}

fn dedup_click_shards(shards: &mut [(Vec<u64>, Vec<ClickRec>, Vec<ClickRec>)], threads: usize) {
    fn go<'a>(
        scope: &'a thread::Scope<'a, '_>,
        shards: &'a mut [(Vec<u64>, Vec<ClickRec>, Vec<ClickRec>)],
        threads: usize,
    ) {
        if shards.is_empty() {
            return;
        }
        if threads <= 1 || shards.len() == 1 {
            scope.spawn(|| {
                for shard in shards.iter_mut() {
                    shard.2 = dedup_clicks(&shard.0, &shard.1);
                }
            });
            return;
        }
        let mid = shards.len() / 2;
        let (left, right) = shards.split_at_mut(mid);
        let lt = (threads / 2).max(1);
        go(scope, left, lt);
        go(scope, right, threads - lt);
    }
    thread::scope(|scope| go(scope, shards, threads.max(1)));
}

fn build_user_ids(
    events: &[Surv],
    clicks: &[ClickRec],
    links: &[(u64, u64)],
    threads: usize,
) -> (U64Map, Vec<u64>) {
    let threads = threads.max(1).min(events.len().max(clicks.len()).max(links.len()).max(1));
    let mut locals: Vec<U64Map> = (0..threads)
        .map(|_| U64Map::with_capacity(events.len() / threads / 4 + 1024))
        .collect();
    let ev_n = events.len();
    let cl_n = clicks.len();
    let lk_n = links.len();
    thread::scope(|scope| {
        for (t, local) in locals.iter_mut().enumerate() {
            let ev = &events[t * ev_n / threads..(t + 1) * ev_n / threads];
            let cl = &clicks[t * cl_n / threads..(t + 1) * cl_n / threads];
            let lk = &links[t * lk_n / threads..(t + 1) * lk_n / threads];
            scope.spawn(move || {
                for e in ev {
                    if e.user != 0 {
                        local.insert(e.user, 1);
                    }
                }
                for c in cl {
                    if c.user != 0 {
                        local.insert(c.user, 1);
                    }
                }
                for &(a, b) in lk {
                    if a != 0 {
                        local.insert(a, 1);
                    }
                    if b != 0 {
                        local.insert(b, 1);
                    }
                }
            });
        }
    });
    let mut keys = Vec::new();
    let mut map = U64Map::with_capacity(events.len() / 16 + 1024);
    for local in &locals {
        for (k, _) in local.iter() {
            if map.get(k).is_none() {
                let id = keys.len() as u32;
                map.insert(k, id);
                keys.push(k);
            }
        }
    }
    (map, keys)
}

fn balance_ranges(starts: &[u32], threads: usize) -> Vec<usize> {
    let n = starts.len().saturating_sub(1);
    if n == 0 {
        return vec![0];
    }
    let total = starts[n];
    let threads = threads.max(1).min(n);
    if total == 0 {
        return vec![0, n];
    }
    let per = (total / threads as u32).max(1);
    let mut pts = vec![0usize];
    let mut target = per;
    for p in 0..n {
        if pts.len() < threads && starts[p] >= target {
            pts.push(p);
            target = target.saturating_add(per);
        }
    }
    if *pts.last().unwrap() != n {
        pts.push(n);
    }
    pts.dedup();
    pts
}

fn group_parallel<T: Copy + Send + Sync>(
    items: Vec<T>,
    n_bins: usize,
    threads: usize,
    bin: impl Fn(&T) -> usize + Sync,
) -> (Vec<T>, Vec<u32>) {
    let mut starts = vec![0u32; n_bins + 1];
    if items.is_empty() || n_bins == 0 {
        return (Vec::new(), starts);
    }
    let threads = threads.max(1).min(items.len());
    let mut local: Vec<Vec<u32>> = (0..threads).map(|_| vec![0u32; n_bins]).collect();
    let n = items.len();
    thread::scope(|scope| {
        for (t, counts) in local.iter_mut().enumerate() {
            let sl = &items[t * n / threads..(t + 1) * n / threads];
            let bin = &bin;
            scope.spawn(move || {
                for it in sl {
                    counts[bin(it)] += 1;
                }
            });
        }
    });
    let mut cursor: Vec<Vec<u32>> = (0..threads).map(|_| vec![0u32; n_bins]).collect();
    let mut run = 0u32;
    for p in 0..n_bins {
        starts[p] = run;
        for t in 0..threads {
            cursor[t][p] = run;
            run += local[t][p];
        }
    }
    starts[n_bins] = run;
    drop(local);
    let mut out: Vec<T> = Vec::with_capacity(items.len());
    unsafe {
        out.set_len(items.len());
    }
    let dst = SendPtr(out.as_mut_ptr());
    thread::scope(|scope| {
        for (t, curs) in cursor.iter_mut().enumerate() {
            let sl = &items[t * n / threads..(t + 1) * n / threads];
            let bin = &bin;
            let dst = dst;
            scope.spawn(move || {
                for it in sl {
                    let b = bin(it);
                    let at = curs[b] as usize;
                    curs[b] += 1;
                    unsafe { dst.write(at, *it) };
                }
            });
        }
    });
    (out, starts)
}

fn empty_partial(n_camps: usize) -> Partial {
    Partial {
        rows: Vec::new(),
        sessions: vec![[0; 14]; n_camps],
        purchasing: vec![[0; 14]; n_camps],
        revenue: vec![[0; 14]; n_camps],
        assisted: vec![[0; 14]; n_camps],
    }
}

fn aggregate_owned(
    events: &mut [Ev],
    ev_base: usize,
    ev_starts: &[u32],
    clicks: &mut [Cl],
    cl_base: usize,
    cl_starts: &[u32],
    p0: usize,
    p1: usize,
    ctx: &AggCtx<'_>,
    out: &mut Partial,
) {
    let mut counts = vec![0u32; ctx.n_real];
    let mut pos = vec![0u32; ctx.n_real];
    let mut active: Vec<u16> = Vec::new();
    let mut day_sessions = [0u32; 14];
    let mut day_events = [0u32; 14];
    let mut day_revenue = [0u64; 14];
    let mut day_cat = vec![vec![0u64; ctx.n_cats]; 14];
    let mut touched: Vec<usize> = Vec::with_capacity(14);

    for p in p0..p1 {
        let e0 = ev_starts[p] as usize - ev_base;
        let e1 = ev_starts[p + 1] as usize - ev_base;
        if e0 == e1 {
            continue;
        }
        events[e0..e1].sort_unstable_by_key(|e| e.ts);
        let c0 = cl_starts[p] as usize - cl_base;
        let c1 = cl_starts[p + 1] as usize - cl_base;
        clicks[c0..c1].sort_unstable_by(|a, b| {
            a.ts.cmp(&b.ts)
                .then_with(|| cmp_keys(a.id, b.id, ctx.slow))
        });
        counts.fill(0);
        active.clear();
        touched.clear();
        let ev = &events[e0..e1];
        let cl = &clicks[c0..c1];
        let mut lo = 0usize;
        let mut hi = 0usize;
        let mut best: Option<usize> = None;
        let mut i = 0usize;
        while i < ev.len() {
            let start_ts = ev[i].ts;
            let mut last = start_ts;
            let mut j = i + 1;
            while j < ev.len() {
                let ts = ev[j].ts;
                if ts != last {
                    if ts - last > GAP_MS {
                        break;
                    }
                    last = ts;
                }
                j += 1;
            }
            let left_bound = start_ts - WEEK_MS;
            // Clicks are sorted, and session starts only increase, so both
            // edges move forward. Add first, then drop clicks that are now
            // more than 7 days before this session. A click can be added and
            // removed in the same session when it is <= start but too old.
            while hi < cl.len() && cl[hi].ts <= start_ts {
                add_camp(&mut counts, &mut active, &mut pos, cl[hi].camp);
                match best {
                    None => best = Some(hi),
                    Some(b) if cl[hi].ts > cl[b].ts => best = Some(hi),
                    _ => {}
                }
                hi += 1;
            }
            while lo < hi && cl[lo].ts < left_bound {
                remove_camp(&mut counts, &mut active, &mut pos, cl[lo].camp);
                if best == Some(lo) {
                    best = None;
                }
                lo += 1;
            }
            if best.is_none() {
                best = best_of(cl, lo, hi);
            }
            let day = (start_ts.div_euclid(DAY_MS) - ctx.start_day as i64) as usize;
            if day < 14 {
                if day_sessions[day] == 0 {
                    day_events[day] = 0;
                    day_revenue[day] = 0;
                    day_cat[day].fill(0);
                    touched.push(day);
                }
                day_sessions[day] += 1;
                let mut session_rev = 0u64;
                let mut has_purchase = false;
                for e in &ev[i..j] {
                    day_events[day] += 1;
                    if e.kind == 2 {
                        has_purchase = true;
                        let price = e.price as u64;
                        session_rev += price;
                        day_cat[day][e.cat as usize] += price;
                    }
                }
                day_revenue[day] += session_rev;
                let credited = best.map(|b| cl[b].camp).unwrap_or(ctx.organic);
                let ci = credited as usize;
                out.sessions[ci][day] += 1;
                if has_purchase {
                    out.purchasing[ci][day] += 1;
                    out.revenue[ci][day] += session_rev;
                }
                for &camp in &active {
                    out.assisted[camp as usize][day] += 1;
                }
            }
            i = j;
        }
        let name = ctx.min_key[p];
        for &d in &touched {
            let mut best_i: Option<usize> = None;
            let mut best_rev = 0u64;
            for (ci, &rev) in day_cat[d].iter().enumerate() {
                if rev == 0 {
                    continue;
                }
                let take = match best_i {
                    None => true,
                    Some(b) => {
                        rev > best_rev || (rev == best_rev && ctx.cat_names[ci][..] < ctx.cat_names[b][..])
                    }
                };
                if take {
                    best_i = Some(ci);
                    best_rev = rev;
                }
            }
            out.rows.push(UserRow {
                key: name,
                day: ctx.start_day + d as i32,
                sessions: day_sessions[d] as i64,
                events: day_events[d] as i64,
                revenue: day_revenue[d] as i64,
                top: best_i.map(|c| c as u16).unwrap_or(u16::MAX),
            });
            day_sessions[d] = 0;
        }
    }
}

fn add_camp(counts: &mut [u32], active: &mut Vec<u16>, pos: &mut [u32], camp: u16) {
    let c = camp as usize;
    if counts[c] == 0 {
        pos[c] = active.len() as u32;
        active.push(camp);
    }
    counts[c] += 1;
}

fn remove_camp(counts: &mut [u32], active: &mut Vec<u16>, pos: &mut [u32], camp: u16) {
    let c = camp as usize;
    counts[c] -= 1;
    if counts[c] == 0 {
        let p = pos[c] as usize;
        let last = active.pop().unwrap();
        if p < active.len() {
            active[p] = last;
            pos[last as usize] = p as u32;
        }
    }
}

fn best_of(cl: &[Cl], lo: usize, hi: usize) -> Option<usize> {
    if lo >= hi {
        return None;
    }
    let ts = cl[hi - 1].ts;
    let mut i = hi - 1;
    while i > lo && cl[i - 1].ts == ts {
        i -= 1;
    }
    Some(i)
}

fn group_by<T: Copy>(items: Vec<T>, n_bins: usize, mut bin: impl FnMut(&T) -> usize) -> (Vec<T>, Vec<u32>) {
    let mut starts = vec![0u32; n_bins + 1];
    if items.is_empty() || n_bins == 0 {
        return (Vec::new(), starts);
    }
    let mut counts = vec![0u32; n_bins];
    for it in &items {
        counts[bin(it)] += 1;
    }
    let mut sum = 0u32;
    for (i, c) in counts.iter().enumerate() {
        starts[i] = sum;
        sum += *c;
    }
    starts[n_bins] = sum;
    let mut cursor = starts.clone();
    let mut out: Vec<T> = Vec::with_capacity(items.len());
    unsafe {
        out.set_len(items.len());
        for it in items {
            let b = bin(&it);
            let at = cursor[b] as usize;
            cursor[b] += 1;
            std::ptr::write(out.as_mut_ptr().add(at), it);
        }
    }
    (out, starts)
}

fn dedup_events(ids: &[u64], recs: &[EventRec]) -> Vec<Surv> {
    if ids.is_empty() {
        return Vec::new();
    }
    let mut map = U64Map::with_capacity(ids.len());
    let mut stored: Vec<EventRec> = Vec::with_capacity(ids.len());
    for (id, rec) in ids.iter().zip(recs) {
        if let Some(idx) = map.get(*id) {
            if rec.ingest < stored[idx as usize].ingest {
                stored[idx as usize] = *rec;
            }
        } else {
            let idx = stored.len() as u32;
            stored.push(*rec);
            map.insert(*id, idx);
        }
    }
    stored
        .into_iter()
        .filter(|r| r.kind != 255 && r.user != 0)
        .map(|r| Surv {
            user: r.user,
            ts: r.ts,
            price: r.price,
            cat: r.cat,
            kind: r.kind,
        })
        .collect()
}

fn dedup_clicks(ids: &[u64], recs: &[ClickRec]) -> Vec<ClickRec> {
    let mut map = U64Map::with_capacity(ids.len().max(1));
    let mut out = Vec::with_capacity(ids.len());
    for (id, rec) in ids.iter().zip(recs) {
        if map.get(*id).is_some() {
            continue;
        }
        map.insert(*id, 0);
        out.push(ClickRec {
            ts: rec.ts,
            user: rec.user,
            id: rec.id,
            camp: rec.camp,
        });
    }
    out
}

struct U64Map {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
    len: usize,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        Self {
            keys: vec![0; cap],
            vals: vec![0; cap],
            mask: cap - 1,
            len: 0,
        }
    }

    fn hash(key: u64) -> usize {
        let mut x = key.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 32;
        x as usize
    }

    fn grow(&mut self) {
        let old_keys = std::mem::take(&mut self.keys);
        let old_vals = std::mem::take(&mut self.vals);
        let cap = (old_keys.len() * 2).max(16);
        self.keys = vec![0; cap];
        self.vals = vec![0; cap];
        self.mask = cap - 1;
        self.len = 0;
        for (k, v) in old_keys.into_iter().zip(old_vals) {
            if k != 0 {
                self.insert(k, v);
            }
        }
    }

    fn insert(&mut self, key: u64, val: u32) {
        debug_assert!(key != 0);
        if self.len * 10 >= self.keys.len() * 7 {
            self.grow();
        }
        let mut i = Self::hash(key) & self.mask;
        loop {
            let k = self.keys[i];
            if k == 0 {
                self.keys[i] = key;
                self.vals[i] = val;
                self.len += 1;
                return;
            }
            if k == key {
                self.vals[i] = val;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }

    fn iter(&self) -> impl Iterator<Item = (u64, u32)> + '_ {
        self.keys
            .iter()
            .zip(self.vals.iter())
            .filter_map(|(&k, &v)| (k != 0).then_some((k, v)))
    }

    fn get(&self, key: u64) -> Option<u32> {
        if key == 0 || self.keys.is_empty() {
            return None;
        }
        let mut i = Self::hash(key) & self.mask;
        loop {
            let k = self.keys[i];
            if k == 0 {
                return None;
            }
            if k == key {
                return Some(self.vals[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    let mut root = x;
    while parent[root as usize] != root {
        root = parent[root as usize];
    }
    while parent[x as usize] != root {
        let next = parent[x as usize];
        parent[x as usize] = root;
        x = next;
    }
    root
}

fn union(parent: &mut [u32], rank: &mut [u8], min_key: &mut [u64], a: u32, b: u32, slow: &SlowIntern) {
    let mut a = find(parent, a);
    let mut b = find(parent, b);
    if a == b {
        return;
    }
    if rank[a as usize] < rank[b as usize] {
        std::mem::swap(&mut a, &mut b);
    }
    parent[b as usize] = a;
    if rank[a as usize] == rank[b as usize] {
        rank[a as usize] = rank[a as usize].saturating_add(1);
    }
    if cmp_keys(min_key[b as usize], min_key[a as usize], slow) == Ordering::Less {
        min_key[a as usize] = min_key[b as usize];
    }
}

fn cmp_keys(a: u64, b: u64, slow: &SlowIntern) -> Ordering {
    let mut ba = [0u8; 24];
    let mut bb = [0u8; 24];
    let (sa, sb): (&[u8], &[u8]) = match (is_slow(a), is_slow(b)) {
        (false, false) => {
            let na = write_id(a, &mut ba);
            let nb = write_id(b, &mut bb);
            (&ba[..na], &bb[..nb])
        }
        (true, true) => (&slow.strings[slow_index(a)], &slow.strings[slow_index(b)]),
        (true, false) => {
            let nb = write_id(b, &mut bb);
            (&slow.strings[slow_index(a)], &bb[..nb])
        }
        (false, true) => {
            let na = write_id(a, &mut ba);
            (&ba[..na], &slow.strings[slow_index(b)])
        }
    };
    sa.cmp(sb)
}

fn with_key_bytes(key: u64, slow: &SlowIntern, mut f: impl FnMut(&[u8])) {
    if is_slow(key) {
        f(&slow.strings[slow_index(key)]);
    } else {
        let mut buf = [0u8; 24];
        let n = write_id(key, &mut buf);
        f(&buf[..n]);
    }
}

fn shard_of(id: u64, bits: u32) -> usize {
    (id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - bits)) as usize
}

fn id_key(shared: &Shared, s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    if let Some(p) = try_pack(s) {
        return Some(p);
    }
    Some(shared.slow.lock().unwrap().intern(s))
}

fn user_key(shared: &Shared, s: &[u8]) -> u64 {
    if s.is_empty() {
        return 0;
    }
    if let Some(p) = try_pack(s) {
        return p;
    }
    shared.slow.lock().unwrap().intern(s)
}

fn is_late(ts: i64, ingest: i64) -> bool {
    (ingest as i128) - (ts as i128) > LATE_MS as i128
}

fn kind_of(s: &[u8]) -> Option<u8> {
    match s {
        b"view" => Some(0),
        b"cart" => Some(1),
        b"purchase" => Some(2),
        _ => None,
    }
}

fn parse_u32_nolead(s: &[u8]) -> Option<u32> {
    if s.is_empty() || (s.len() > 1 && s[0] == b'0') {
        return None;
    }
    let mut v: u32 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u32)?;
    }
    Some(v)
}

fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u32 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u32)?;
    }
    if (1..=10_000_000).contains(&v) { Some(v) } else { None }
}

fn product_cat(ctx: &Ctx<'_>, s: &[u8]) -> u8 {
    if s.len() >= 2 && s[0] == b'p' {
        if let Some(n) = parse_u32_nolead(&s[1..]) {
            if (n as usize) < ctx.prod_cat.len() {
                let c = ctx.prod_cat[n as usize];
                if c != 255 {
                    return c;
                }
            }
            return ctx.unknown;
        }
    }
    ctx.prod_other.get(s).copied().unwrap_or(ctx.unknown)
}

fn campaign_of(ctx: &Ctx<'_>, s: &[u8]) -> Option<u16> {
    if s.len() >= 3 && s[0] == b'c' && s[1] == b'_' {
        if let Some(n) = parse_u32_nolead(&s[2..]) {
            if (n as usize) < ctx.camp_by_num.len() {
                let idx = ctx.camp_by_num[n as usize];
                if idx >= 0 {
                    return Some(idx as u16);
                }
            }
        }
    }
    ctx.camp_other.get(s).copied()
}

fn clean<'a>(buf: &'a [u8], start: u32, end: u32, quote: bool) -> Option<&'a [u8]> {
    let f = &buf[start as usize..end as usize];
    if !quote {
        return Some(f);
    }
    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        let inner = &f[1..f.len() - 1];
        if inner.contains(&b'"') {
            return None;
        }
        return Some(inner);
    }
    None
}

#[inline(always)]
fn scan(buf: &[u8], mut f: impl FnMut(&[u32; 8], &[u32; 8], &[bool; 8], usize)) {
    if buf.is_empty() {
        return;
    }
    let mut starts = [0u32; 8];
    let mut ends = [0u32; 8];
    let mut quotes = [false; 8];
    let mut n = 0usize;
    let mut bad = false;
    let mut fs = 0usize;
    let mut saw_q = false;
    let mut i = 0usize;
    let len = buf.len();
    while i < len {
        let c = buf[i];
        if c == b'"' {
            saw_q = true;
            i += 1;
            continue;
        }
        if c == b',' || c == b'\n' {
            if !bad && n < 8 {
                starts[n] = fs as u32;
                ends[n] = i as u32;
                quotes[n] = saw_q;
                n += 1;
            } else {
                bad = true;
            }
            if c == b'\n' {
                if !bad && n > 0 {
                    f(&starts, &ends, &quotes, n);
                }
                n = 0;
                bad = false;
            }
            i += 1;
            fs = i;
            saw_q = false;
            continue;
        }
        i += 1;
    }
    if fs < len && !bad && n < 8 {
        starts[n] = fs as u32;
        ends[n] = len as u32;
        quotes[n] = saw_q;
        n += 1;
        f(&starts, &ends, &quotes, n);
    }
}

fn mix_id(id: u64) -> u64 {
    let x = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 32)
}

fn dedup_event_buckets(mut buckets: Vec<Vec<EventRec>>, threads: usize) -> Vec<Surv> {
    let threads = threads.max(1).min(buckets.len());
    let width = buckets.len().div_ceil(threads);
    let mut parts: Vec<Vec<Surv>> = std::iter::repeat_with(Vec::new).take(threads).collect();
    thread::scope(|scope| {
        let mut groups = buckets.chunks_mut(width);
        for part in &mut parts {
            let Some(slice) = groups.next() else { break };
            scope.spawn(move || {
                for bucket in slice.iter_mut() {
                    if bucket.is_empty() {
                        continue;
                    }
                    bucket.sort_unstable_by(|a, b| a.id.cmp(&b.id).then(a.ingest.cmp(&b.ingest)));
                    let mut i = 0;
                    while i < bucket.len() {
                        let rec = bucket[i];
                        let mut j = i + 1;
                        while j < bucket.len() && bucket[j].id == rec.id {
                            j += 1;
                        }
                        if rec.kind != 255 && rec.user != 0 {
                            part.push(Surv {
                                user: rec.user,
                                ts: rec.ts,
                                price: rec.price,
                                cat: rec.cat,
                                kind: rec.kind,
                            });
                        }
                        i = j;
                    }
                }
            });
        }
    });
    let mut out = Vec::new();
    for part in parts {
        out.extend(part);
    }
    out
}

fn dedup_click_buckets(mut buckets: Vec<Vec<ClickRec>>, threads: usize) -> Vec<ClickRec> {
    let threads = threads.max(1).min(buckets.len());
    let width = buckets.len().div_ceil(threads);
    let mut parts: Vec<Vec<ClickRec>> = std::iter::repeat_with(Vec::new).take(threads).collect();
    thread::scope(|scope| {
        let mut groups = buckets.chunks_mut(width);
        for part in &mut parts {
            let Some(slice) = groups.next() else { break };
            scope.spawn(move || {
                for bucket in slice.iter_mut() {
                    if bucket.is_empty() {
                        continue;
                    }
                    bucket.sort_unstable_by_key(|rec| rec.id);
                    let mut i = 0;
                    while i < bucket.len() {
                        let id = bucket[i].id;
                        part.push(bucket[i]);
                        i += 1;
                        while i < bucket.len() && bucket[i].id == id {
                            i += 1;
                        }
                    }
                }
            });
        }
    });
    let mut out = Vec::new();
    for part in parts {
        out.extend(part);
    }
    out
}

fn dedup_events_bucket(recs: Vec<EventRec>, threads: usize) -> Vec<Surv> {
    const BUCKETS: usize = 256;
    let n = recs.len();
    if n == 0 {
        return Vec::new();
    }
    let threads = threads.max(1).min(n);
    let mut counts = vec![vec![0u32; BUCKETS]; threads];
    thread::scope(|scope| {
        for (t, count) in counts.iter_mut().enumerate() {
            let sl = &recs[t * n / threads..(t + 1) * n / threads];
            scope.spawn(move || {
                for rec in sl {
                    count[(mix_id(rec.id) as usize) & 255] += 1;
                }
            });
        }
    });
    let mut bucket_at = vec![0u32; BUCKETS + 1];
    let mut cursor = vec![vec![0u32; BUCKETS]; threads];
    let mut run = 0u32;
    for bucket in 0..BUCKETS {
        bucket_at[bucket] = run;
        for t in 0..threads {
            let count = counts[t][bucket];
            cursor[t][bucket] = run;
            run += count;
        }
    }
    bucket_at[BUCKETS] = run;
    let mut placed = Vec::<EventRec>::with_capacity(n);
    unsafe { placed.set_len(n) };
    let dst = SendPtr(placed.as_mut_ptr());
    thread::scope(|scope| {
        for (t, cur) in cursor.iter_mut().enumerate() {
            let sl = &recs[t * n / threads..(t + 1) * n / threads];
            let dst = dst;
            scope.spawn(move || {
                for rec in sl {
                    let bucket = (mix_id(rec.id) as usize) & 255;
                    let at = cur[bucket] as usize;
                    cur[bucket] += 1;
                    unsafe { dst.write(at, *rec) };
                }
            });
        }
    });
    drop(recs);
    let mut parts = std::iter::repeat_with(Vec::new).take(threads).collect::<Vec<_>>();
    let ptr = SendPtr(placed.as_mut_ptr());
    let bucket_at_ref = &bucket_at;
    thread::scope(|scope| {
        for (t, part) in parts.iter_mut().enumerate() {
            let b0 = t * BUCKETS / threads;
            let b1 = (t + 1) * BUCKETS / threads;
            scope.spawn(move || {
                for bucket in b0..b1 {
                    let start = bucket_at_ref[bucket] as usize;
                    let len = bucket_at_ref[bucket + 1] as usize - start;
                    if len == 0 {
                        continue;
                    }
                    let sl = unsafe { ptr.slice(start, len) };
                    sl.sort_unstable_by(|a, b| a.id.cmp(&b.id).then(a.ingest.cmp(&b.ingest)));
                    let mut i = 0;
                    while i < sl.len() {
                        let rec = sl[i];
                        let mut j = i + 1;
                        while j < sl.len() && sl[j].id == rec.id {
                            j += 1;
                        }
                        if rec.kind != 255 && rec.user != 0 {
                            part.push(Surv {
                                user: rec.user,
                                ts: rec.ts,
                                price: rec.price,
                                cat: rec.cat,
                                kind: rec.kind,
                            });
                        }
                        i = j;
                    }
                }
            });
        }
    });
    let mut out = Vec::new();
    for part in parts {
        out.extend(part);
    }
    out
}

fn dedup_clicks_bucket(recs: Vec<ClickRec>, threads: usize) -> Vec<ClickRec> {
    const BUCKETS: usize = 256;
    let n = recs.len();
    if n == 0 {
        return Vec::new();
    }
    let threads = threads.max(1).min(n);
    let mut counts = vec![vec![0u32; BUCKETS]; threads];
    thread::scope(|scope| {
        for (t, count) in counts.iter_mut().enumerate() {
            let sl = &recs[t * n / threads..(t + 1) * n / threads];
            scope.spawn(move || {
                for rec in sl {
                    count[(mix_id(rec.id) as usize) & 255] += 1;
                }
            });
        }
    });
    let mut bucket_at = vec![0u32; BUCKETS + 1];
    let mut cursor = vec![vec![0u32; BUCKETS]; threads];
    let mut run = 0u32;
    for bucket in 0..BUCKETS {
        bucket_at[bucket] = run;
        for t in 0..threads {
            let count = counts[t][bucket];
            cursor[t][bucket] = run;
            run += count;
        }
    }
    bucket_at[BUCKETS] = run;
    let mut placed = Vec::<ClickRec>::with_capacity(n);
    unsafe { placed.set_len(n) };
    let dst = SendPtr(placed.as_mut_ptr());
    thread::scope(|scope| {
        for (t, cur) in cursor.iter_mut().enumerate() {
            let sl = &recs[t * n / threads..(t + 1) * n / threads];
            let dst = dst;
            scope.spawn(move || {
                for rec in sl {
                    let bucket = (mix_id(rec.id) as usize) & 255;
                    let at = cur[bucket] as usize;
                    cur[bucket] += 1;
                    unsafe { dst.write(at, *rec) };
                }
            });
        }
    });
    drop(recs);
    let mut parts = std::iter::repeat_with(Vec::new).take(threads).collect::<Vec<_>>();
    let ptr = SendPtr(placed.as_mut_ptr());
    let bucket_at_ref = &bucket_at;
    thread::scope(|scope| {
        for (t, part) in parts.iter_mut().enumerate() {
            let b0 = t * BUCKETS / threads;
            let b1 = (t + 1) * BUCKETS / threads;
            scope.spawn(move || {
                for bucket in b0..b1 {
                    let start = bucket_at_ref[bucket] as usize;
                    let len = bucket_at_ref[bucket + 1] as usize - start;
                    if len == 0 {
                        continue;
                    }
                    let sl = unsafe { ptr.slice(start, len) };
                    sl.sort_unstable_by_key(|rec| rec.id);
                    let mut i = 0;
                    while i < sl.len() {
                        let id = sl[i].id;
                        part.push(sl[i]);
                        i += 1;
                        while i < sl.len() && sl[i].id == id {
                            i += 1;
                        }
                    }
                }
            });
        }
    });
    let mut out = Vec::new();
    for part in parts {
        out.extend(part);
    }
    out
}

fn remap_events(events: &[Surv], users: &U64Map, parent: &[u32], threads: usize) -> Vec<Ev> {
    let n = events.len();
    if n == 0 {
        return Vec::new();
    }
    let threads = threads.max(1).min(n);
    let mut parts = std::iter::repeat_with(Vec::new).take(threads).collect::<Vec<_>>();
    thread::scope(|scope| {
        for (t, part) in parts.iter_mut().enumerate() {
            let sl = &events[t * n / threads..(t + 1) * n / threads];
            scope.spawn(move || {
                part.reserve(sl.len());
                for event in sl {
                    let Some(dense) = users.get(event.user) else { continue };
                    part.push(Ev {
                        person: parent[dense as usize],
                        ts: event.ts,
                        price: event.price,
                        cat: event.cat,
                        kind: event.kind,
                    });
                }
            });
        }
    });
    let mut out = Vec::with_capacity(n);
    for part in parts {
        out.extend(part);
    }
    out
}

fn remap_clicks(clicks: &[ClickRec], users: &U64Map, parent: &[u32], threads: usize) -> Vec<Cl> {
    let n = clicks.len();
    if n == 0 {
        return Vec::new();
    }
    let threads = threads.max(1).min(n);
    let mut parts = std::iter::repeat_with(Vec::new).take(threads).collect::<Vec<_>>();
    thread::scope(|scope| {
        for (t, part) in parts.iter_mut().enumerate() {
            let sl = &clicks[t * n / threads..(t + 1) * n / threads];
            scope.spawn(move || {
                part.reserve(sl.len());
                for click in sl {
                    let Some(dense) = users.get(click.user) else { continue };
                    part.push(Cl {
                        person: parent[dense as usize],
                        ts: click.ts,
                        id: click.id,
                        camp: click.camp,
                    });
                }
            });
        }
    });
    let mut out = Vec::with_capacity(n);
    for part in parts {
        out.extend(part);
    }
    out
}

fn dedup_sorted_events(recs: Vec<EventRec>) -> Vec<Surv> {
    let mut out = Vec::with_capacity(recs.len());
    let mut i = 0usize;
    let n = recs.len();
    while i < n {
        let id = recs[i].id;
        let mut best = i;
        let mut j = i + 1;
        while j < n && recs[j].id == id {
            if recs[j].ingest < recs[best].ingest {
                best = j;
            }
            j += 1;
        }
        let rec = recs[best];
        if rec.kind != 255 && rec.user != 0 {
            out.push(Surv {
                user: rec.user,
                ts: rec.ts,
                price: rec.price,
                cat: rec.cat,
                kind: rec.kind,
            });
        }
        i = j;
    }
    out
}

fn dedup_sorted_clicks(recs: Vec<ClickRec>) -> Vec<ClickRec> {
    let mut out = Vec::with_capacity(recs.len());
    let mut i = 0usize;
    let n = recs.len();
    while i < n {
        let id = recs[i].id;
        out.push(recs[i]);
        i += 1;
        while i < n && recs[i].id == id {
            i += 1;
        }
    }
    out
}

fn radix_user_ids(
    events: &[Surv],
    clicks: &[ClickRec],
    links: &[(u64, u64)],
    threads: usize,
) -> (U64Map, Vec<u64>) {
    let mut keys = Vec::with_capacity(events.len() + clicks.len() + links.len() * 2);
    for event in events {
        keys.push(event.user);
    }
    for click in clicks {
        keys.push(click.user);
    }
    for &(a, b) in links {
        keys.push(a);
        keys.push(b);
    }
    let keys = radix::par_radix_u64(keys, threads, |key| *key);
    let mut uniq = Vec::with_capacity(keys.len() / 8 + 1);
    let mut prev = 0u64;
    for key in keys {
        if key != 0 && key != prev {
            uniq.push(key);
            prev = key;
        }
    }
    let mut map = U64Map::with_capacity(uniq.len().max(16));
    for (index, key) in uniq.iter().enumerate() {
        map.insert(*key, index as u32);
    }
    (map, uniq)
}

#[inline(always)]
unsafe fn next_mark(p: *const u8, end: *const u8) -> *const u8 {
    let len = unsafe { end.offset_from(p) } as usize;
    let slice = unsafe { std::slice::from_raw_parts(p, len) };
    match memchr::memchr3(b',', b'\n', b'"', slice) {
        Some(index) => unsafe { p.add(index) },
        None => end,
    }
}

fn for_lines(buf: &[u8], mut on_line: impl FnMut(&[u8])) {
    let mut i = 0usize;
    while i < buf.len() {
        let rel = memchr::memchr(b'\n', &buf[i..]).unwrap_or(buf.len() - i);
        let line = &buf[i..i + rel];
        i += rel + 1;
        if !line.is_empty() {
            on_line(line);
        }
    }
}

fn split_fields<'a>(line: &'a [u8], expect: usize) -> Option<([&'a [u8]; 8], usize)> {
    let mut out = [b"".as_ref(); 8];
    let mut n = 0usize;
    let mut start = 0usize;
    let mut saw_quote = false;
    for (i, &c) in line.iter().enumerate() {
        if c == b'"' {
            saw_quote = true;
        } else if c == b',' {
            if n >= 8 {
                return None;
            }
            out[n] = field_bytes(&line[start..i], saw_quote)?;
            n += 1;
            start = i + 1;
            saw_quote = false;
        }
    }
    if n >= 8 {
        return None;
    }
    out[n] = field_bytes(&line[start..], saw_quote)?;
    n += 1;
    if n != expect {
        return None;
    }
    Some((out, n))
}

fn field_bytes(value: &[u8], saw_quote: bool) -> Option<&[u8]> {
    if !saw_quote {
        return Some(value);
    }
    if value.len() >= 2 && value[0] == b'"' && value[value.len() - 1] == b'"' && !value[1..value.len() - 1].contains(&b'"')
    {
        return Some(&value[1..value.len() - 1]);
    }
    None
}

fn parse_events(buf: &[u8], ctx: &Ctx<'_>, shared: &Shared, out: &mut ThreadOut) {
    let w = ctx.window;
    for_lines(buf, |line| {
        let Some((raw, _)) = split_fields(line, 7) else { return };
        let Some(id) = id_key(shared, raw[0]) else { return };
        let Some(ingest) = parse_ts(raw[3]) else { return };
        let user = user_key(shared, raw[1]);
        let mut kind = 255u8;
        let mut ts = 0i64;
        let mut price = 0u32;
        let mut cat = 0u8;
        if user != 0 {
            if let Some(parsed) = parse_ts(raw[2]) {
                if parsed >= w.start && parsed < w.end && !is_late(parsed, ingest) {
                    if let Some(k) = kind_of(raw[4]) {
                        if !raw[5].is_empty() {
                            if k == 2 {
                                if let Some(parsed_price) = parse_price(raw[6]) {
                                    kind = 2;
                                    ts = parsed;
                                    price = parsed_price;
                                    cat = product_cat(ctx, raw[5]);
                                }
                            } else {
                                kind = k;
                                ts = parsed;
                            }
                        }
                    }
                }
            }
        }
        out.events[(mix_id(id) as usize) & 255].push(EventRec { id, user, ts, ingest, price, cat, kind });
    });
}

unsafe fn field_at<'a>(ptr: *const u8, len: u32, quoted: bool) -> Option<&'a [u8]> {
    let len = len as usize;
    let value = unsafe { std::slice::from_raw_parts(ptr, len) };
    if !quoted {
        return Some(value);
    }
    if len >= 2 && value[0] == b'"' && value[len - 1] == b'"' && !value[1..len - 1].contains(&b'"') {
        return Some(&value[1..len - 1]);
    }
    None
}

fn parse_clicks(buf: &[u8], ctx: &Ctx<'_>, shared: &Shared, out: &mut ThreadOut) {
    let w = ctx.window;
    for_lines(buf, |line| {
        let Some((raw, _)) = split_fields(line, 4) else { return };
        let Some(id) = id_key(shared, raw[0]) else { return };
        let user = user_key(shared, raw[1]);
        if user == 0 {
            return;
        }
        let Some(ts) = parse_ts(raw[3]) else { return };
        if ts < w.click_start || ts >= w.end {
            return;
        }
        let Some(camp) = campaign_of(ctx, raw[2]) else { return };
        out.clicks[(mix_id(id) as usize) & 255].push(ClickRec { ts, user, id, camp });
    });
}

fn parse_links(buf: &[u8], shared: &Shared, out: &mut ThreadOut) {
    scan(buf, |starts, ends, quotes, n| {
        if n != 2 {
            return;
        }
        let Some(a) = clean(buf, starts[0], ends[0], quotes[0]) else { return };
        let Some(b) = clean(buf, starts[1], ends[1], quotes[1]) else { return };
        if a.is_empty() || b.is_empty() {
            return;
        }
        out.links.push((user_key(shared, a), user_key(shared, b)));
    });
}

fn split_chunks(file: u16, kind: u8, bytes: &[u8]) -> Vec<ChunkLite> {
    let mut out = Vec::new();
    let Some(nl) = bytes.iter().position(|&c| c == b'\n') else {
        return out;
    };
    let mut pos = nl + 1;
    const SZ: usize = 4 * 1024 * 1024;
    while pos < bytes.len() {
        let mut end = (pos + SZ).min(bytes.len());
        if end < bytes.len() {
            if let Some(rel) = bytes[end..].iter().position(|&c| c == b'\n') {
                end += rel + 1;
            } else {
                end = bytes.len();
            }
        }
        out.push(ChunkLite { file, kind, start: pos, end });
        pos = end;
    }
    out
}

struct ChunkLite {
    file: u16,
    kind: u8,
    start: usize,
    end: usize,
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let Ok(rd) = fs::read_dir(dir) else {
        return paths;
    };
    for ent in rd.flatten() {
        let path = ent.path();
        if path.extension().and_then(|e| e.to_str()) == Some("csv") {
            paths.push(path);
        }
    }
    paths.sort();
    paths
}

fn load_window(path: &Path) -> Result<Window, Box<dyn std::error::Error>> {
    let text = fs::read_to_string(path)?;
    let start = extract_ts(&text, "window_start").ok_or("manifest window_start")?;
    let end = extract_ts(&text, "window_end").ok_or("manifest window_end")?;
    let start_day = (start.div_euclid(DAY_MS)) as i32;
    Ok(Window {
        start,
        end,
        click_start: start - WEEK_MS,
        start_day,
    })
}

fn extract_ts(text: &str, key: &str) -> Option<i64> {
    let needle = format!("\"{key}\"");
    let i = text.find(&needle).or_else(|| text.find(key))?;
    let rest = &text[i..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let q1 = rest.find('"')?;
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"')?;
    parse_ts(rest[..q2].as_bytes())
}

fn load_products(
    path: &Path,
) -> Result<(Vec<u8>, u8, HashMap<Vec<u8>, u8>, Vec<Vec<u8>>), Box<dyn std::error::Error>> {
    let text = fs::read(path)?;
    let mut names: Vec<Vec<u8>> = Vec::new();
    let mut other: HashMap<Vec<u8>, u8> = HashMap::new();
    let mut max_n = 0usize;
    let mut rows: Vec<(Vec<u8>, u8)> = Vec::new();
    for (i, line) in split_lines(&text).into_iter().enumerate() {
        if i == 0 || line.is_empty() {
            continue;
        }
        let Some(fields) = parse_line_owned(line) else { continue };
        if fields.len() != 2 || fields[0].is_empty() {
            continue;
        }
        let cat = cat_id(&mut names, &fields[1]);
        if let Some(n) = product_number(&fields[0]) {
            max_n = max_n.max(n as usize);
        }
        rows.push((fields[0].clone(), cat));
    }
    let unknown = cat_id(&mut names, b"unknown");
    let mut prod_cat = vec![255u8; max_n + 1];
    for (id, cat) in rows {
        if let Some(n) = product_number(&id) {
            if (n as usize) < prod_cat.len() {
                prod_cat[n as usize] = cat;
                continue;
            }
        }
        other.insert(id, cat);
    }
    Ok((prod_cat, unknown, other, names))
}

fn product_number(s: &[u8]) -> Option<u32> {
    if s.len() >= 2 && s[0] == b'p' {
        parse_u32_nolead(&s[1..])
    } else {
        None
    }
}

fn cat_id(names: &mut Vec<Vec<u8>>, s: &[u8]) -> u8 {
    if let Some((i, _)) = names.iter().enumerate().find(|(_, n)| n.as_slice() == s) {
        return i as u8;
    }
    names.push(s.to_vec());
    (names.len() - 1) as u8
}

fn load_campaigns(
    path: &Path,
) -> Result<(Vec<i32>, Vec<Camp>, HashMap<Vec<u8>, u16>), Box<dyn std::error::Error>> {
    let text = fs::read(path)?;
    let mut camps = Vec::new();
    let mut other: HashMap<Vec<u8>, u16> = HashMap::new();
    let mut numbered: Vec<(u32, u16)> = Vec::new();
    let mut max_n = 0u32;
    for (i, line) in split_lines(&text).into_iter().enumerate() {
        if i == 0 || line.is_empty() {
            continue;
        }
        let Some(fields) = parse_line_owned(line) else { continue };
        if fields.len() != 2 || fields[0].is_empty() {
            continue;
        }
        let idx = camps.len() as u16;
        if let Some(n) = campaign_number(&fields[0]) {
            max_n = max_n.max(n);
            numbered.push((n, idx));
        } else {
            other.insert(fields[0].clone(), idx);
        }
        camps.push(Camp {
            name: fields[0].clone(),
            channel: fields[1].clone(),
        });
    }
    let mut by_num = vec![-1i32; max_n as usize + 1];
    for (n, idx) in numbered {
        by_num[n as usize] = idx as i32;
    }
    Ok((by_num, camps, other))
}

fn campaign_number(s: &[u8]) -> Option<u32> {
    if s.len() >= 3 && s[0] == b'c' && s[1] == b'_' {
        parse_u32_nolead(&s[2..])
    } else {
        None
    }
}

fn split_lines(bytes: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut start = 0usize;
    for (i, &c) in bytes.iter().enumerate() {
        if c == b'\n' {
            let mut end = i;
            if end > start && bytes[end - 1] == b'\r' {
                end -= 1;
            }
            out.push(&bytes[start..end]);
            start = i + 1;
        }
    }
    if start < bytes.len() {
        out.push(&bytes[start..]);
    }
    out
}

fn parse_line_owned(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut fields = Vec::new();
    let mut bad = false;
    let mut cur = Vec::new();
    let mut saw_q = false;
    let mut n = 0usize;
    let flush = |cur: &mut Vec<u8>, saw_q: bool, fields: &mut Vec<Vec<u8>>, bad: &mut bool| {
        if *bad {
            return;
        }
        if !saw_q {
            fields.push(std::mem::take(cur));
            return;
        }
        if cur.len() >= 2 && cur[0] == b'"' && *cur.last().unwrap() == b'"' && !cur[1..cur.len() - 1].contains(&b'"') {
            fields.push(cur[1..cur.len() - 1].to_vec());
            cur.clear();
        } else {
            *bad = true;
        }
    };
    for &c in line {
        if c == b'"' {
            saw_q = true;
            cur.push(c);
            continue;
        }
        if c == b',' {
            flush(&mut cur, saw_q, &mut fields, &mut bad);
            n += 1;
            saw_q = false;
            continue;
        }
        cur.push(c);
    }
    flush(&mut cur, saw_q, &mut fields, &mut bad);
    let _ = n;
    if bad { None } else { Some(fields) }
}

fn write_tables(dir: &Path, tables: &Tables) -> Result<(), Box<dyn std::error::Error>> {
    let user_dir = dir.join("user_daily");
    let camp_dir = dir.join("campaign_daily");
    fs::create_dir_all(&user_dir)?;
    fs::create_dir_all(&camp_dir)?;
    write_user_file(&user_dir.join("part-00000.parquet"), &tables.users, &tables.slow, &tables.cat_names)?;
    write_camp_file(
        &camp_dir.join("part-00000.parquet"),
        &tables.camps_out,
        &tables.camps,
    )?;
    if std::env::var_os("WRITE_CSV").is_some() {
        write_debug_csv(dir, tables)?;
    }
    Ok(())
}

fn write_user_file(
    path: &Path,
    rows: &[UserRow],
    slow: &SlowIntern,
    cat_names: &[Vec<u8>],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut arena = Vec::new();
    let mut spans = Vec::with_capacity(rows.len());
    for row in rows {
        let s = arena.len();
        with_key_bytes(row.key, slow, |b| arena.extend_from_slice(b));
        spans.push((s, arena.len()));
    }
    let user_id: Vec<&[u8]> = spans.iter().map(|&(a, b)| &arena[a..b]).collect();
    let day: Vec<i32> = rows.iter().map(|r| r.day).collect();
    let sessions: Vec<i64> = rows.iter().map(|r| r.sessions).collect();
    let events: Vec<i64> = rows.iter().map(|r| r.events).collect();
    let revenue: Vec<i64> = rows.iter().map(|r| r.revenue).collect();
    let top: Vec<Option<&[u8]>> = rows
        .iter()
        .map(|r| {
            if r.top == u16::MAX {
                None
            } else {
                Some(cat_names[r.top as usize].as_slice())
            }
        })
        .collect();
    parquet::write_user_daily(
        path,
        &parquet::UserFields { user_id, day, sessions, events, revenue, top },
    )?;
    Ok(())
}

fn write_camp_file(path: &Path, rows: &[CampRow], camps: &[Camp]) -> Result<(), Box<dyn std::error::Error>> {
    let campaign_id: Vec<&[u8]> = rows.iter().map(|r| camps[r.camp as usize].name.as_slice()).collect();
    let channel: Vec<&[u8]> = rows.iter().map(|r| camps[r.camp as usize].channel.as_slice()).collect();
    let day: Vec<i32> = rows.iter().map(|r| r.day).collect();
    let sessions: Vec<i64> = rows.iter().map(|r| r.sessions).collect();
    let purchasing: Vec<i64> = rows.iter().map(|r| r.purchasing).collect();
    let revenue: Vec<i64> = rows.iter().map(|r| r.revenue).collect();
    let assisted: Vec<i64> = rows.iter().map(|r| r.assisted).collect();
    parquet::write_campaign_daily(
        path,
        &parquet::CampFields {
            campaign_id,
            channel,
            day,
            sessions,
            purchasing,
            revenue,
            assisted,
        },
    )?;
    Ok(())
}

fn write_debug_csv(dir: &Path, tables: &Tables) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut u = File::create(dir.join("user_daily_debug.csv"))?;
    writeln!(u, "user_id,day,sessions,events,revenue_cents,top_category")?;
    for row in &tables.users {
        let mut id = [0u8; 24];
        let mut name = Vec::new();
        with_key_bytes(row.key, &tables.slow, |b| name.extend_from_slice(b));
        let _ = id;
        let mut day = [0u8; 10];
        format_day(row.day, &mut day);
        let top = if row.top == u16::MAX {
            String::new()
        } else {
            String::from_utf8_lossy(&tables.cat_names[row.top as usize]).into_owned()
        };
        writeln!(
            u,
            "{},{},{},{},{},{}",
            String::from_utf8_lossy(&name),
            std::str::from_utf8(&day).unwrap(),
            row.sessions,
            row.events,
            row.revenue,
            top
        )?;
    }
    let mut c = File::create(dir.join("campaign_daily_debug.csv"))?;
    writeln!(c, "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions")?;
    for row in &tables.camps_out {
        let mut day = [0u8; 10];
        format_day(row.day, &mut day);
        writeln!(
            c,
            "{},{},{},{},{},{},{}",
            String::from_utf8_lossy(&tables.camps[row.camp as usize].name),
            String::from_utf8_lossy(&tables.camps[row.camp as usize].channel),
            std::str::from_utf8(&day).unwrap(),
            row.sessions,
            row.purchasing,
            row.revenue,
            row.assisted
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn bench_pack_and_time() {
        let ts = b"2027-12-19T18:13:05Z";
        let f3 = b"1791071890000";
        let id = b"e42huhoug";
        let user = b"uduffvac";
        let t = Instant::now();
        let mut acc = 0u64;
        for _ in 0..4_000_000 {
            acc = acc.wrapping_add(parse_ts(std::hint::black_box(ts)).unwrap_or(0) as u64);
            acc = acc.wrapping_add(parse_ts(std::hint::black_box(f3)).unwrap_or(0) as u64);
            acc = acc.wrapping_add(try_pack(std::hint::black_box(id)).unwrap_or(0));
            acc = acc.wrapping_add(try_pack(std::hint::black_box(user)).unwrap_or(0));
        }
        eprintln!("pack+ts {} ms acc {acc}", t.elapsed().as_millis());
    }

    use super::*;
    use std::collections::BTreeSet;

    fn day(y: i64, m: i64, d: i64) -> i32 {
        days_from_civil(y, m, d).unwrap() as i32
    }

    #[test]
    fn fixture_matches_hand_rules() {
        let root = std::env::temp_dir().join(format!("ss-fix-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("events")).unwrap();
        fs::create_dir_all(root.join("clicks")).unwrap();
        fs::create_dir_all(root.join("links")).unwrap();
        fs::write(
            root.join("manifest.json"),
            "{\n  \"window_start\": \"2026-01-01T00:00:00Z\",\n  \"window_end\": \"2026-01-15T00:00:00Z\"\n}\n",
        )
        .unwrap();
        fs::write(root.join("products.csv"), "product_id,category\np1,books\np2,toys\n").unwrap();
        fs::write(root.join("campaigns.csv"), "campaign_id,channel\nc_1,search\nc_2,email\n").unwrap();
        fs::write(
            root.join("links").join("part.csv"),
            "user_id_a,user_id_b\nu2,u1\nu9,u9\nu2,U2\nu2,u1\nu1,\nonly_one\n",
        )
        .unwrap();
        let t_jan9_30 = parse_ts(b"2026-01-09T00:30:00Z").unwrap();
        let f3_split = (t_jan9_30 + GAP_MS + 1).to_string();
        let t_jan1 = parse_ts(b"2026-01-01T00:00:00Z").unwrap();
        let early = (t_jan1 - 1).to_string();
        let t_jan8 = parse_ts(b"2026-01-08T00:00:00Z").unwrap();
        let after = (t_jan8 + 1).to_string();
        let events = format!(
            "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n\
e1,u2,2026-01-01T00:00:00Z,2026-01-01T00:00:01Z,purchase,p1,0250\n\
e2,u2,2026-01-01T00:10:00Z,2026-01-01T00:10:01Z,view,p1,\n\
e3,u1,2026-01-01T00:30:00Z,2026-01-01T00:30:01Z,purchase,p2,100\n\
e4,u1,2026-01-01T23:50:00Z,2026-01-01T23:50:01Z,view,p1,\n\
e5,u2,2026-01-02T00:05:00Z,2026-01-02T00:05:01Z,view,p1,\n\
e6,u3,2026-01-03T00:00:00Z,2026-01-03T00:00:01Z,view,p999,\n\
e12,u3,2026-01-03T01:00:00Z,2026-01-04T01:00:01Z,view,p1,\n\
e13,u3,2026-01-03T02:00:00Z,2026-01-04T02:00:00Z,view,p1,\n\
e20,u3,not-a-ts,2026-01-03T03:00:01Z,view,p1,\n\
e20,u3,2026-01-03T03:00:00Z,2026-01-03T03:05:00Z,view,p1,\n\
e21,u3,2026-01-05T00:00:00Z,2026-01-05T00:00:01Z,purchase,p1,100\n\
e21,u3,2026-01-05T00:00:00Z,2026-01-05T00:10:00Z,purchase,p1,999\n\
e22,u3,2026-01-05T00:00:00Z\n\
\n\
e23,\"u3,2026-01-05T01:00:00Z,2026-01-05T01:00:01Z,view,p1,\n\
e80,u3,2026-01-05T05:00:00Z,2026-01-05T05:00:01Z,purchase,p1,0\n\
e81,u3,2026-01-05T06:00:00Z,2026-01-05T06:00:01Z,Purchase,p1,100\n\
e10,u4,2026-01-04T00:00:00Z,2026-01-04T00:00:01Z,purchase,p999,100\n\
e11,u4,2026-01-04T01:00:00Z,2026-01-04T01:00:01Z,view,p1,\n\
e40,u5,2026-01-08T00:00:00Z,2026-01-08T00:00:01Z,view,p1,\n\
e50,u6,2026-01-07T00:00:00Z,2026-01-07T00:00:01Z,purchase,p1,100\n\
e51,u6,2026-01-07T00:10:00Z,2026-01-07T00:10:01Z,purchase,p2,100\n\
e60,u7,2026-01-09T00:00:00Z,2026-01-09T00:00:01Z,view,p1,\n\
e61,u7,2026-01-09T00:30:00Z,2026-01-09T00:30:01Z,view,p1,\n\
e62,u7,{f3_split},2026-01-09T01:00:01Z,view,p1,\n\
e70,u8,2026-01-10T00:00:00Z,2026-01-10T00:00:01Z,view,p1,\n\
\"e30\",\"u9\",\"2026-01-06T00:00:00Z\",\"2026-01-06T00:00:01Z\",\"view\",\"p1\",\"\"\n\
e31,u9,2026-01-06T00:00:00Z,2026-01-06T00:00:02Z,view,p1,\n\
e92,u10,2026-01-11T00:00:00Z,2026-01-11T00:00:01Z,purchase,P1,50\n\
e90,u11,2026-01-12T03:30:00-04:00,2026-01-12T07:30:01Z,view,p1,\n\
e91,u11,2026-01-12T22:30:00-04:00,2026-01-13T02:30:01Z,view,p1,\n"
        );
        fs::write(root.join("events").join("a.csv"), events).unwrap();
        fs::write(root.join("events").join("empty.csv"), "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n").unwrap();
        let clicks = format!(
            "click_id,user_id,campaign_id,ts\n\
k1,u4,c_1,2026-01-04T00:00:00Z\n\
\"k1\",\"u4\",\"c_1\",\"2026-01-04T00:00:00Z\"\n\
k9,u4,c_2,2026-01-03T00:00:00Z\n\
k7,u5,c_1,2026-01-01T00:00:00Z\n\
k8,u5,c_2,{early}\n\
k6,u5,c_2,{after}\n\
k2,u8,c_2,2026-01-09T00:00:00Z\n\
k1b,u8,c_1,2026-01-09T00:00:00Z\n\
kbad,u8,c_9999,2026-01-09T00:00:00Z\n"
        );
        // k1 and k1b: k1 < k1b? "k1" vs "k1b": k1 is a prefix, so k1 < k1b.
        // I wanted k1 to win against k2. k1 is on u4, not u8.
        // For u8 the tie is k2 (c_2) and k1b (c_1). "k1b" < "k2" because '1' < '2'.
        // Credit c_1. Good, k1b wins.
        fs::write(root.join("clicks").join("a.csv"), clicks).unwrap();

        let tables = compute(&root, 4).unwrap();
        let mut got_u = BTreeSet::new();
        for row in &tables.users {
            let mut name = Vec::new();
            with_key_bytes(row.key, &tables.slow, |b| name.extend_from_slice(b));
            let top = if row.top == u16::MAX {
                String::new()
            } else {
                String::from_utf8_lossy(&tables.cat_names[row.top as usize]).into_owned()
            };
            got_u.insert((
                String::from_utf8(name).unwrap(),
                row.day,
                row.sessions,
                row.events,
                row.revenue,
                top,
            ));
        }
        let mut expect_u = BTreeSet::new();
        let push = |set: &mut BTreeSet<_>, name: &str, y, m, d, s, e, r, top: &str| {
            set.insert((name.into(), day(y, m, d), s, e, r, top.into()));
        };
        push(&mut expect_u, "U2", 2026, 1, 1, 2, 5, 350, "books");
        push(&mut expect_u, "u3", 2026, 1, 3, 2, 2, 0, "");
        push(&mut expect_u, "u3", 2026, 1, 5, 1, 1, 100, "books");
        push(&mut expect_u, "u4", 2026, 1, 4, 2, 2, 100, "unknown");
        push(&mut expect_u, "u5", 2026, 1, 8, 1, 1, 0, "");
        push(&mut expect_u, "u6", 2026, 1, 7, 1, 2, 200, "books");
        push(&mut expect_u, "u7", 2026, 1, 9, 2, 3, 0, "");
        push(&mut expect_u, "u8", 2026, 1, 10, 1, 1, 0, "");
        push(&mut expect_u, "u9", 2026, 1, 6, 1, 2, 0, "");
        push(&mut expect_u, "u10", 2026, 1, 11, 1, 1, 50, "unknown");
        push(&mut expect_u, "u11", 2026, 1, 12, 1, 1, 0, "");
        push(&mut expect_u, "u11", 2026, 1, 13, 1, 1, 0, "");
        assert_eq!(got_u, expect_u, "users got={got_u:?}");

        let mut got_c = BTreeSet::new();
        for row in &tables.camps_out {
            got_c.insert((
                String::from_utf8_lossy(&tables.camps[row.camp as usize].name).into_owned(),
                String::from_utf8_lossy(&tables.camps[row.camp as usize].channel).into_owned(),
                row.day,
                row.sessions,
                row.purchasing,
                row.revenue,
                row.assisted,
            ));
        }
        let mut expect_c = BTreeSet::new();
        let pushc = |set: &mut BTreeSet<_>, id: &str, ch: &str, y, m, d, s, p, r, a| {
            set.insert((id.into(), ch.into(), day(y, m, d), s, p, r, a));
        };
        // organic: U2 (2 sess, 1 purch, 350), u3 jan3 (2,0,0), u3 jan5 (1,1,100),
        // u6 (1,1,200), u7 (2,0,0), u9 (1,0,0), u10 (1,1,50), u11 two days (1,0,0) each.
        pushc(&mut expect_c, "organic", "organic", 2026, 1, 1, 2, 1, 350, 0);
        pushc(&mut expect_c, "organic", "organic", 2026, 1, 3, 2, 0, 0, 0);
        pushc(&mut expect_c, "organic", "organic", 2026, 1, 5, 1, 1, 100, 0);
        pushc(&mut expect_c, "organic", "organic", 2026, 1, 6, 1, 0, 0, 0);
        pushc(&mut expect_c, "organic", "organic", 2026, 1, 7, 1, 1, 200, 0);
        pushc(&mut expect_c, "organic", "organic", 2026, 1, 9, 2, 0, 0, 0);
        pushc(&mut expect_c, "organic", "organic", 2026, 1, 11, 1, 1, 50, 0);
        pushc(&mut expect_c, "organic", "organic", 2026, 1, 12, 1, 0, 0, 0);
        pushc(&mut expect_c, "organic", "organic", 2026, 1, 13, 1, 0, 0, 0);
        // c_1: u4 both sessions, u5, u8
        pushc(&mut expect_c, "c_1", "search", 2026, 1, 4, 2, 1, 100, 2);
        pushc(&mut expect_c, "c_1", "search", 2026, 1, 8, 1, 0, 0, 1);
        pushc(&mut expect_c, "c_1", "search", 2026, 1, 10, 1, 0, 0, 1);
        // c_2 assists u4 both sessions and u8
        pushc(&mut expect_c, "c_2", "email", 2026, 1, 4, 0, 0, 0, 2);
        pushc(&mut expect_c, "c_2", "email", 2026, 1, 10, 0, 0, 0, 1);
        assert_eq!(got_c, expect_c, "camps got={got_c:?}");
        let _ = fs::remove_dir_all(&root);
    }
}
