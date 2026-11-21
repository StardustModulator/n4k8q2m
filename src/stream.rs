//! Partition on event ID *before* dropping invalid records. Partition on the
//! linked person's name *after* choosing the earliest copy. These two shuffles
//! are sufficient; neither an all-user interner nor a global sort is needed.
use super::*;
use crate::aggregate::{Acc, Hit};
use crate::clock::Clock;
use crate::links::Aliases;

#[inline(always)]
pub(super) fn hash(x: u64) -> u64 {
    let x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[derive(Clone, Copy)]
struct Record {
    id: u64,
    user: u64, // zero is invalid; retained in the minimum-ingest contest
    at: i64,
    val: u64,  // 32-bit timestamp, 8-bit category, 24-bit price
}

#[derive(Clone, Copy)]
pub(super) struct Event {
    pub(super) key: u64,
    pub(super) val: u64,
}

#[derive(Clone, Copy)]
pub(super) struct Click {
    pub(super) key: u64,
    pub(super) hit: Hit,
}

#[derive(Default)]
pub(super) struct Histories {
    pub(super) ev: Vec<Event>,
    pub(super) cl: Vec<Click>,
}

pub(super) fn bins<T>(n: usize) -> Vec<Vec<T>> {
    (0..n).map(|_| Vec::new()).collect()
}

struct Scan {
    ev: Vec<Vec<Record>>,
    cl: Vec<Click>,
    links: Vec<(u64, u64)>,
}

#[inline(always)]
fn payload(f: &[&[u8]; 7], cx: &Ctx<'_>, clock: &Clock, at: i64, origin: i64) -> Option<u64> {
    let ts = clock.parse(f[2])?;
    if ts < cx.window.start || ts >= cx.window.end || at > ts + DAY_MS || f[5].is_empty() {
        return None;
    }
    let pay = match f[4] {
        b"view" | b"cart" => 0,
        b"purchase" => {
            let p = parse_price(f[6])?;
            p | (u32::from(product_cat(cx, f[5])) << 24)
        }
        _ => return None,
    };
    Some(((ts - origin) as u64) << 32 | u64::from(pay))
}

#[inline]
fn scan_events(buf: &[u8], cx: &Ctx<'_>, clock: &Clock, s: &Shared, out: &mut Scan, origin: i64) {
    let mask = out.ev.len() - 1;
    wire::rows::<7>(buf, |f| {
        let Some(id) = id_key(s, f[0]) else { return };
        let Some(at) = clock.ingest(f[3]) else { return };
        let (user, val) = if f[1].is_empty() {
            (0, 0)
        } else if let Some(v) = payload(&f, cx, clock, at, origin) {
            (user_key(s, f[1]), v)
        } else {
            (0, 0)
        };
        let b = (hash(id) >> 32) as usize & mask;
        out.ev[b].push(Record { id, user, at, val });
    });
}

#[inline]
fn scan_clicks(buf: &[u8], cx: &Ctx<'_>, clock: &Clock, s: &Shared, out: &mut Scan, origin: i64) {
    out.cl.reserve(buf.len() / 45);
    wire::rows::<4>(buf, |f| {
        if f[0].is_empty() || f[1].is_empty() { return; }
        let Some(ts) = clock.parse(f[3]) else { return };
        if ts < cx.window.click_start || ts >= cx.window.end { return; }
        let Some(camp) = campaign_of(cx, f[2]) else { return };
        let hit = Hit { ts: (ts - origin) as u32, id: user_key(s, f[0]), camp };
        out.cl.push(Click { key: user_key(s, f[1]), hit });
    });
}

// Only used within a bucket. Favor the largest allocation as backing storage
// and keep the copying on the worker that immediately consumes these records.
fn join_parts<T>(mut p: Vec<Vec<T>>) -> Vec<T> {
    let n: usize = p.iter().map(Vec::len).sum();
    let Some((big, _)) = p.iter().enumerate().max_by_key(|(_, s)| s.len()) else { return Vec::new() };
    let mut out = p.swap_remove(big);
    out.reserve_exact(n - out.len());
    for mut v in p { out.append(&mut v); }
    out
}

// Cache one exact alias lookup for frequently accessed devices. A miss is
// always resolved against the complete graph, including ids with no event.
struct NameCache(Vec<(u64, u64)>);
impl NameCache {
    fn new() -> Self { Self(vec![(0, 0); 8192]) }
    #[inline(always)]
    fn name(&mut self, key: u64, aliases: &Aliases) -> u64 {
        let i = hash(key) as usize & (self.0.len() - 1);
        let slot = &mut self.0[i];
        if slot.0 == key { return slot.1; }
        let v = aliases.resolve(key);
        *slot = (key, v);
        v
    }
}

fn reduce_event_ids(parts: Vec<Vec<Record>>, aliases: &Aliases, cache: &mut NameCache, dest: &mut [Histories]) {
    let mut rows = join_parts(parts);
    if rows.is_empty() { return; }
    // Two bits per hash prefix. A prefix seen once cannot be a duplicate, so
    // those rows skip the id table. A shared prefix is only a suspect: the
    // exact table still compares full ids.
    let n = rows.len();
    let words = ((n + 3) / 4).next_power_of_two().max(1);
    let word_mask = words - 1;
    let mut present = vec![0u64; words];
    let mut repeated = vec![0u64; words];
    for r in &rows {
        let h = hash(r.id);
        let bit = 1u64 << (h & 63);
        let i = ((h >> 6) as usize) & word_mask;
        let seen = present[i];
        repeated[i] |= seen & bit;
        present[i] = seen | bit;
    }
    let mut suspects = n;
    for i in 0..words {
        suspects -= (present[i] ^ repeated[i]).count_ones() as usize;
    }
    if suspects > 0 {
        let mask = (suspects * 2).next_power_of_two() - 1;
        let mut table = vec![0u32; mask + 1];
        for i in 0..n {
            let h = hash(rows[i].id);
            if ((repeated[((h >> 6) as usize) & word_mask] >> (h & 63)) & 1) == 0 {
                continue;
            }
            let mut s = hash(rows[i].id.wrapping_mul(0x9e37_79b9_7f4a_7c15)) as usize & mask;
            loop {
                let ptr = table[s];
                if ptr == 0 {
                    table[s] = (i + 1) as u32;
                    break;
                }
                let j = (ptr - 1) as usize;
                if rows[j].id == rows[i].id {
                    if rows[i].at < rows[j].at {
                        rows[j].user = 0;
                        table[s] = (i + 1) as u32;
                    } else {
                        rows[i].user = 0;
                    }
                    break;
                }
                s = (s + 1) & mask;
            }
        }
    }
    let mask = dest.len() - 1;
    for r in rows {
        if r.user != 0 {
            let key = cache.name(r.user, aliases);
            dest[(hash(key) >> 32) as usize & mask].ev.push(Event { key, val: r.val });
        }
    }
}

fn route_clicks(rows: Vec<Click>, aliases: &Aliases, cache: &mut NameCache, dest: &mut [Histories]) {
    // All copies of a click are identical. Window membership is existential
    // per campaign and credit is a min/max over keys. Both are idempotent:
    // duplicating an identical tuple cannot change either result.
    let mask = dest.len() - 1;
    for mut r in rows {
        r.key = cache.name(r.key, aliases);
        dest[(hash(r.key) >> 32) as usize & mask].cl.push(r);
    }
}

#[derive(Clone, Copy, Default)]
struct Person { key: u64, ev: u32, cl: u32 }

// Within a person shard the index and cursors fit in private caches. Store
// the local group number in place after the first lookup; scatters don't hash.
pub(super) fn histories(parts: Vec<Histories>, acc: &mut Acc, cats: &[Vec<u8>], slow: &SlowIntern, first_day: i32) {
    let mut parts = parts;
    let ne = parts.iter().map(|p| p.ev.len()).sum::<usize>();
    if ne == 0 { return; }
    let mut map = LocalMap::new((ne / 20).max(16));
    let mut people = Vec::<Person>::with_capacity(ne / 24 + 1);
    for part in &mut parts {
        for e in &mut part.ev {
            let i = map.intern(e.key, &mut people);
            e.key = i as u64;
            people[i].ev += 1;
        }
    }
    for part in &mut parts {
        for c in &mut part.cl {
            if let Some(i) = map.lookup(c.key) {
                c.key = i as u64;
                people[i].cl += 1;
            } else {
                c.key = u64::MAX;
            }
        }
    }
    let (mut es, mut cs) = (0u32, 0u32);
    for p in &mut people {
        let (e, c) = (p.ev, p.cl);
        p.ev = es; p.cl = cs;
        es += e; cs += c;
    }
    let mut ev = vec![0u64; es as usize];
    let mut cl = vec![Hit { ts: 0, id: 0, camp: 0 }; cs as usize];
    for part in parts {
        for r in part.ev {
            let cursor = &mut people[r.key as usize].ev;
            ev[*cursor as usize] = r.val;
            *cursor += 1;
        }
        for r in part.cl {
            if r.key != u64::MAX {
                let cursor = &mut people[r.key as usize].cl;
                cl[*cursor as usize] = r.hit;
                *cursor += 1;
            }
        }
    }
    let (mut e0, mut c0) = (0, 0);
    for p in people {
        let (e1, c1) = (p.ev as usize, p.cl as usize);
        acc.person(p.key, &mut ev[e0..e1], &mut cl[c0..c1], cats, slow, first_day);
        e0 = e1; c0 = c1;
    }
}

struct LocalMap {
    table: Vec<(u64,u32)>,
}
impl LocalMap {
    fn new(n:usize)->Self { Self {table: vec![(0,0); (2*n).next_power_of_two()]} }
    #[inline(always)]
    fn slot(&self,key:u64)->usize {
        let mask=self.table.len()-1;
        let mut i=hash(key) as usize&mask;
        while self.table[i].0 != 0 && self.table[i].0 != key {
            i=(i+1)&mask;
        }
        i
    }
    #[cold]
    fn grow(&mut self) {
        let n = self.table.len()*2;
        let old=std::mem::replace(&mut self.table,vec![(0,0);n]);
        for (key,val) in old {
            if key!=0 { let i=self.slot(key); self.table[i]=(key,val); }
        }
    }
    #[inline(always)]
    fn lookup(&self,key:u64)->Option<usize> {
        let (k,i)=self.table[self.slot(key)];
        (k!=0).then_some(i as usize)
    }
    #[inline(always)]
    fn intern(&mut self,key:u64,people:&mut Vec<Person>)->usize {
        let mut s=self.slot(key);
        if self.table[s].0 == key {return self.table[s].1 as usize;}
        if people.len()*4 >= self.table.len()*3 {
            self.grow(); s=self.slot(key);
        }
        let i=people.len();
        self.table[s]=(key,i as u32);
        people.push(Person {key,ev:0,cl:0});
        i
    }
}

// One owner for each job. The lock is acquired once per shard, never per row.
pub(super) fn consume<J:Send,S:Send>(jobs:Vec<J>,workers:usize, init:impl Fn()->S+Sync,
                            run:impl Fn(&mut S,J)+Sync)->Vec<S> {
    let queue=Mutex::new(jobs.into_iter());
    thread::scope(|scope| {
        let handles: Vec<_>=(0..workers).map(|_|scope.spawn(||{
            let mut s=init();
            loop {
                let job=queue.lock().unwrap().next();
                match job { Some(j)=>run(&mut s,j),None=>break }
            }
            s
        })).collect();
        handles.into_iter().map(|h|h.join().unwrap()).collect()
    })
}

enum Route { Ev(Vec<Vec<Record>>), Cl(Vec<Click>) }

pub(super) fn compute(data:&Path, threads:usize)->Result<Tables,Box<dyn std::error::Error>> {
    let total=Instant::now();
    let w=load_window(&data.join("manifest.json"))?;
    let origin=w.start_day as i64*DAY_MS-WEEK_MS;
    if w.end<=w.start || w.end-origin>u32::MAX as i64 {return Err("window does not fit compact millisecond offsets".into());}
    let days=((w.end-1).div_euclid(DAY_MS)-w.start_day as i64+1) as usize;
    let clock=Clock::new(w.start,w.end);
    let (prod_cat,unknown,prod_other,cat_names)=load_products(&data.join("products.csv"))?;
    let (camp_by_num,mut camps,camp_other)=load_campaigns(&data.join("campaigns.csv"))?;
    let organic=u16::try_from(camps.len())?;
    camps.push(Camp{name:b"organic".to_vec(),channel:b"organic".to_vec()});
    let cx=Ctx{prod_cat:&prod_cat,unknown,prod_other:&prod_other,camp_by_num:&camp_by_num,camp_other:&camp_other,window:&w};
    let mut maps=Vec::<Mmap>::new();
    let mut chunks=Vec::new();
    let mut event_bytes=0;
    for (kind,dir) in [(0u8,"events"),(1,"clicks"),(2,"links")] {
        for p in list_csv(&data.join(dir))? {
            let f=File::open(p)?;
            if f.metadata()?.len()==0 {continue;}
            let m=unsafe{Mmap::map(&f)?};
            let _=m.advise(Advice::Sequential);
            if kind==0 {event_bytes+=m.len();}
            chunks.extend(split_chunks(maps.len(),kind,&m));
            maps.push(m);
        }
    }
    let threads=threads.max(1).min(chunks.len().max(1));
    let eb=(event_bytes / (77 * 16384)).next_power_of_two().clamp(8,2048);
    let pb=(event_bytes / (77 * 65536)).next_power_of_two().clamp(16,512);
    chunks.sort_unstable_by_key(|c|std::cmp::Reverse(c.end-c.start));
    let next=AtomicUsize::new(0);
    let shared=Shared{slow:Mutex::new(SlowIntern::new())};
    let prep=total.elapsed();
    let scans=thread::scope(|scope|{
        let handles:Vec<_>=(0..threads).map(|_|scope.spawn(||{
            let mut out=Scan{ev:bins(eb),cl:Vec::new(),links:Vec::new()};
            loop {
                let i=next.fetch_add(1,AtomicOrdering::Relaxed);
                let Some(c)=chunks.get(i) else{break};
                let buf=&maps[c.file as usize][c.start..c.end];
                match c.kind {
                    0=>scan_events(buf,&cx,&clock,&shared,&mut out,origin),
                    1=>scan_clicks(buf,&cx,&clock,&shared,&mut out,origin),
                    _=>wire::rows::<2>(buf,|[a,b]| {
                        if !a.is_empty() && !b.is_empty() && a!=b {
                            out.links.push((user_key(&shared,a),user_key(&shared,b)));
                        }
                    }),
                }
            }
            out
        })).collect();
        handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>()
    });
    let parsed=total.elapsed();
    let slow=shared.slow.into_inner().unwrap();
    let mut parts:Vec<Vec<Vec<Record>>>=bins(eb);
    let mut jobs=Vec::with_capacity(threads+eb);
    let mut lks=Vec::with_capacity(threads);
    for s in scans {
        for (b,v) in parts.iter_mut().zip(s.ev) {b.push(v);}
        if !s.cl.is_empty(){jobs.push(Route::Cl(s.cl));}
        lks.push(s.links);
    }
    let aliases=links::build(lks,&slow,threads);
    let indexed=total.elapsed();
    jobs.extend(parts.into_iter().map(Route::Ev));
    let routed=consume(jobs,threads,||((0..pb).map(|_| Histories::default()).collect::<Vec<_>>(),NameCache::new()),
        |(out,cache),j|match j{
            Route::Ev(rows)=>reduce_event_ids(rows,&aliases,cache,out),
            Route::Cl(rows)=>route_clicks(rows,&aliases,cache,out),
        });
    let reduced=total.elapsed();
    let mut by_name:Vec<Vec<Histories>>=bins(pb);
    for (r,_) in routed {
        for (dst,src) in by_name.iter_mut().zip(r) {dst.push(src);}
    }
    by_name.sort_unstable_by_key(|s|std::cmp::Reverse(s.iter().map(|p|p.ev.len()+p.cl.len()).sum::<usize>()));
    let partials=consume(by_name,threads,||Acc::new(camps.len(),days,cat_names.len()),
                         |acc,job|histories(job,acc,&cat_names,&slow,w.start_day));
    let grouped=total.elapsed();
    let mut users=Vec::with_capacity(partials.iter().map(|p|p.rows.len()).sum());
    let mut stats=vec![[0i64;4];camps.len()*days];
    for mut p in partials {
        users.append(&mut p.rows);
        for (t,v) in stats.iter_mut().zip(p.stats) {
            for i in 0..4{t[i]+=v[i];}
        }
    }
    let mut camps_out=Vec::new();
    for (i,[sessions,purchasing,revenue,assisted]) in stats.into_iter().enumerate(){
        if sessions!=0 || assisted!=0{
            camps_out.push(CampRow{camp:(i/days) as u16,day:w.start_day+(i%days) as i32,sessions,purchasing,revenue,assisted});
        }
    }
    if std::env::var_os("STAGES").is_some() {
        eprintln!("ms prep={} parse={} links={} dedup+route={} histories={} merge={}",
            prep.as_millis(),(parsed-prep).as_millis(),(indexed-parsed).as_millis(),(reduced-indexed).as_millis(),
            (grouped-reduced).as_millis(),(total.elapsed()-grouped).as_millis());
    }
    Ok(Tables{users,camps_out,camps,cat_names,slow,organic})
}
