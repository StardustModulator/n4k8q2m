//! Bounded-memory Parquet output. Each worker owns a file; each column page is
//! gathered directly into reusable scratch and written with its header via writev.

use super::{with_key_bytes, CampRow, Tables, UserRow};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, IoSlice, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;

const GROUP: usize = 16_384; // also bounds dictionary indices to two bytes
const MIN_PART: usize = 32_768;
const CREATOR: &[u8] = b"shopping-sessions-solve version 2";
const MAGIC: &[u8] = b"PAR1";
const PLAIN: i32 = 0;
const RLE: i32 = 3;
const DICT: i32 = 8; // RLE_DICTIONARY

fn invalid(s: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, s)
}

fn var(mut x: u64, b: &mut [u8]) -> usize {
    let mut n = 0;
    while x >= 128 {
        b[n] = x as u8 | 128;
        x >>= 7;
        n += 1;
    }
    b[n] = x as u8;
    n + 1
}

#[derive(Default)]
struct Thrift(Vec<u8>);

impl Thrift {
    fn uvar(&mut self, v: u64) {
        let mut b = [0; 10];
        let n = var(v, &mut b);
        self.0.extend_from_slice(&b[..n]);
    }
    fn tag(&mut self, delta: u8, ty: u8) {
        self.0.push(delta << 4 | ty);
    }
    fn stop(&mut self) {
        self.0.push(0);
    }
    fn zz(&mut self, v: i64) {
        self.uvar(((v << 1) ^ (v >> 63)) as u64);
    }
    fn i32(&mut self, d: u8, v: i32) {
        self.tag(d, 5);
        self.zz(i64::from(v));
    }
    fn i64(&mut self, d: u8, v: i64) {
        self.tag(d, 6);
        self.zz(v);
    }
    fn bytes(&mut self, s: &[u8]) {
        self.uvar(s.len() as u64);
        self.0.extend_from_slice(s);
    }
    fn binary(&mut self, d: u8, s: &[u8]) {
        self.tag(d, 8);
        self.bytes(s);
    }
    fn list(&mut self, d: u8, n: usize, ty: u8) {
        self.tag(d, 9);
        if n < 15 {
            self.0.push((n as u8) << 4 | ty);
        } else {
            self.0.push(0xf0 | ty);
            self.uvar(n as u64);
        }
    }
}

#[derive(Clone, Copy)]
enum Kind { I32, I64, Date, Str }

#[derive(Clone, Copy)]
struct Col(&'static str, Kind, bool);

impl Col {
    fn integer(name: &'static str, bits: u64) -> Self {
        // Aggregates are nonnegative. Negative synthetic inputs simply stay i64.
        Self(name, if bits <= i32::MAX as u64 { Kind::I32 } else { Kind::I64 }, false)
    }
    fn physical(self) -> i32 {
        match self.1 { Kind::I32 | Kind::Date => 1, Kind::I64 => 2, Kind::Str => 6 }
    }
    fn schema(self, t: &mut Thrift) {
        t.i32(1, self.physical());
        t.i32(2, i32::from(self.2)); // required = 0, optional = 1
        t.binary(1, self.0.as_bytes());
        let annotation = match self.1 {
            Kind::Date => Some((6, 6)), // converted DATE, logical DATE
            Kind::Str => Some((0, 1)),  // converted UTF8, logical STRING
            _ => None,
        };
        if let Some((converted, logical)) = annotation {
            t.i32(2, converted);
            t.tag(4, 12); // SchemaElement.logicalType
            t.tag(logical, 12);
            t.stop();
            t.stop();
        }
        t.stop();
    }
}

fn page_header(t: &mut Thrift, dict: bool, encoding: i32, n: usize, bytes: usize) -> io::Result<()> {
    let bytes = i32::try_from(bytes).map_err(|_| invalid("Parquet page exceeds INT32 size"))?;
    let n = i32::try_from(n).map_err(|_| invalid("Parquet page exceeds INT32 value count"))?;
    t.0.clear();
    t.i32(1, if dict { 2 } else { 0 });
    t.i32(1, bytes);
    t.i32(1, bytes); // UNCOMPRESSED
    t.tag(if dict { 4 } else { 2 }, 12); // dictionary_page_header=7, data_page_header=5
    t.i32(1, n);
    t.i32(1, encoding);
    if !dict {
        t.i32(1, RLE);
        t.i32(1, RLE);
    }
    t.stop();
    t.stop();
    Ok(())
}

fn writev<const N: usize>(file: &mut File, bufs: [&[u8]; N]) -> io::Result<()> {
    let mut vecs = bufs.map(IoSlice::new);
    let mut pending = &mut vecs[..];
    loop {
        while pending.first().is_some_and(|b| b.is_empty()) {
            pending = &mut pending[1..];
        }
        if pending.is_empty() { return Ok(()); }
        match file.write_vectored(pending) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => IoSlice::advance_slices(&mut pending, n),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

struct Chunk { start: i64, data: i64, size: i64 }

struct Writer<'a> {
    f: File,
    cols: &'a [Col],
    pos: i64,
    groups: usize,
    rows: i64,
    meta: Thrift,
    dh: Thrift,
    ph: Thrift,
}

impl<'a> Writer<'a> {
    fn new(mut f: File, cols: &'a [Col]) -> io::Result<Self> {
        f.write_all(MAGIC)?;
        Ok(Self { f, cols, pos: 4, groups: 0, rows: 0, meta: Thrift::default(),
            dh: Thrift::default(), ph: Thrift::default() })
    }

    // body = definition-level prefix, bitmap, value prefix, values. Required
    // columns simply supply empty slices for the level sections.
    fn page(&mut self, n: usize, dict: Option<(usize, &[u8])>, body: [&[u8]; 4]) -> io::Result<Chunk> {
        self.dh.0.clear();
        let dict_body = if let Some((entries, bytes)) = dict {
            page_header(&mut self.dh, true, PLAIN, entries, bytes.len())?;
            bytes
        } else { &[] };
        let size: usize = body.iter().map(|p| p.len()).sum();
        page_header(&mut self.ph, false, if dict.is_some() { DICT } else { PLAIN }, n, size)?;
        let start = self.pos;
        let data = start + (self.dh.0.len() + dict_body.len()) as i64;
        writev(&mut self.f, [&self.dh.0, dict_body, &self.ph.0, body[0], body[1], body[2], body[3]])?;
        self.pos = data + (self.ph.0.len() + size) as i64;
        Ok(Chunk { start, data, size: self.pos - start })
    }

    fn group(&mut self, n: usize, chunks: &[Chunk]) {
        debug_assert_eq!(self.cols.len(), chunks.len());
        let t = &mut self.meta;
        t.list(1, chunks.len(), 12);
        let mut total = 0;
        for (&col, c) in self.cols.iter().zip(chunks) {
            t.i64(2, 0); // no duplicated ColumnMetaData outside the footer
            t.tag(1, 12);
            t.i32(1, col.physical());
            let enc: &[i32] = if c.data != c.start { &[PLAIN, RLE, DICT] }
                else if col.2 { &[PLAIN, RLE] } else { &[PLAIN] };
            t.list(1, enc.len(), 5);
            for &e in enc { t.zz(i64::from(e)); }
            t.list(1, 1, 8);
            t.bytes(col.0.as_bytes()); // exactly the leaf name from the schema
            t.i32(1, 0);
            t.i64(1, n as i64);
            t.i64(1, c.size);
            t.i64(1, c.size);
            t.i64(2, c.data); // ColumnMetaData.data_page_offset = 9
            if c.data != c.start { t.i64(2, c.start); } // dictionary_page_offset = 11
            t.stop();
            t.stop();
            total += c.size;
        }
        t.i64(1, total);
        t.i64(1, n as i64);
        t.i64(3, total); // RowGroup.total_compressed_size = 6 (5 is file_offset)
        t.stop();
        self.rows += n as i64;
        self.groups += 1;
    }

    fn finish(mut self) -> io::Result<()> {
        let mut head = Thrift::default();
        head.i32(1, 1);
        head.list(1, 1 + self.cols.len(), 12);
        head.binary(4, b"schema"); // no type or repetition_type on the root
        head.i32(1, self.cols.len() as i32);
        head.stop();
        for &c in self.cols { c.schema(&mut head); }
        head.i64(1, self.rows);
        head.list(1, self.groups, 12); // zero groups, with a real schema, for empty tables
        let mut tail = Thrift::default();
        tail.binary(2, CREATOR);
        tail.stop();
        let len = u32::try_from(head.0.len() + self.meta.0.len() + tail.0.len())
            .map_err(|_| invalid("Parquet footer too large"))?.to_le_bytes();
        writev(&mut self.f, [&head.0, &self.meta.0, &tail.0, &len, MAGIC])
    }
}

#[derive(Default)]
struct Scratch {
    values: Vec<u8>,
    dict: Vec<u8>,
    bitmap: Vec<u8>,
    num: Vec<u8>,
    // Small, reused exact interner; also handles disjoint runs of the same ID.
    slots: Vec<u16>,
    keys: Vec<u64>,
}

fn plain_string(dst: &mut Vec<u8>, s: &[u8]) {
    // If a string doesn't fit a signed i32, its containing page cannot fit
    // either. page_header checks that limit before issuing any write.
    dst.extend_from_slice(&(s.len() as u32).to_le_bytes());
    dst.extend_from_slice(s);
}

fn prefix_for_literals(width: u8, count: usize, buf: &mut [u8; 12]) -> usize {
    buf[0] = width;
    1 + var((count.div_ceil(8) as u64) * 2 + 1, &mut buf[1..])
}

fn user_ids(w: &mut Writer<'_>, s: &mut Scratch, t: &Tables, rows: &[UserRow]) -> io::Result<Chunk> {
    let n = rows.len();
    let runs = 1 + rows.windows(2).filter(|p| p[0].key != p[1].key).count();
    // Per-run dictionary ordinals need 2–3 bytes per person, not per day.
    // Near-unique and small groups are cheaper to write PLAIN instead.
    let dictionary = n >= 32 && runs * 4 <= n * 3;
    let bits = (usize::BITS - (runs - 1).leading_zeros()).max(1) as u8;
    let cap = (runs * 2).next_power_of_two();
    if dictionary {
        s.slots.resize(cap, 0);
        s.slots.fill(0);
        s.keys.clear();
        s.dict.clear();
        s.dict.reserve(runs * 13);
    }
    s.values.clear();
    s.values.reserve(if dictionary { runs * 3 } else { n * 13 });
    let mut i = 0;
    while i < n {
        let key = rows[i].key;
        let mut j = i + 1;
        while j < n && rows[j].key == key { j += 1; }
        if dictionary {
            let h = key.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let mut slot = ((h ^ (h >> 32)) as usize) & (cap - 1);
            let id = loop {
                let entry = s.slots[slot];
                if entry == 0 {
                    let id = s.keys.len() as u16;
                    s.keys.push(key);
                    s.slots[slot] = id + 1;
                    with_key_bytes(key, &t.slow, |bytes| plain_string(&mut s.dict, bytes));
                    break id;
                }
                if s.keys[(entry - 1) as usize] == key { break entry - 1; }
                slot = (slot + 1) & (cap - 1);
            };
            let length = (j - i) << 1;
            if length < 128 { s.values.push(length as u8); }
            else {
                let mut b = [0; 10];
                let len = var(length as u64, &mut b);
                s.values.extend_from_slice(&b[..len]);
            }
            s.values.push(id as u8);
            if bits > 8 { s.values.push((id >> 8) as u8); }
        } else {
            with_key_bytes(key, &t.slow, |bytes| {
                for _ in i..j { plain_string(&mut s.values, bytes); }
            });
        }
        i = j;
    }
    if dictionary {
        w.page(n, Some((s.keys.len(), &s.dict)), [&[], &[], &[bits], &s.values])
    } else {
        w.page(n, None, [&[], &[], &[], &s.values])
    }
}

// A small dense integer range can be written as raw bytes/nibbles into a
// literal dictionary-index run; no general bit-packer, compressor or hashing.
fn integers<R>(w: &mut Writer<'_>, s: &mut Scratch, col: Col, rows: &[R],
    dense: bool, get: impl Fn(&R) -> i64) -> io::Result<Chunk> {
    let n = rows.len();
    let wide = matches!(col.1, Kind::I64);
    if dense && n >= 512 {
        let mut lo = i64::MAX;
        let mut hi = i64::MIN;
        for r in rows {
            let v = get(r);
            lo = lo.min(v);
            hi = hi.max(v);
        }
        let span = hi.abs_diff(lo);
        if span <= 255 && (span + 1) * (if wide { 8 } else { 4 }) <= (n * 2) as u64 {
            let count = span as usize + 1;
            s.dict.clear();
            for i in 0..count {
                let v = lo + i as i64;
                if wide { s.dict.extend_from_slice(&v.to_le_bytes()); }
                else { s.dict.extend_from_slice(&(v as i32).to_le_bytes()); }
            }
            let mut pre = [0; 12];
            if span == 0 {
                pre[0] = 1; // one-bit indices; one RLE run of zero
                let end = 1 + var((n as u64) << 1, &mut pre[1..]);
                return w.page(n, Some((count, &s.dict)), [&[], &[], &pre[..end + 1], &[]]);
            }
            let width = if span < 16 { 4 } else { 8 };
            let len = n.div_ceil(8) * width as usize;
            if s.num.len() < len { s.num.resize(len, 0); }
            if width == 8 {
                for (dst, r) in s.num[..n].iter_mut().zip(rows) { *dst = (get(r) - lo) as u8; }
                s.num[n..len].fill(0);
            } else {
                let mut pairs = rows.chunks_exact(2);
                for (dst, pair) in s.num.iter_mut().zip(&mut pairs) {
                    *dst = (get(&pair[0]) - lo) as u8 | (((get(&pair[1]) - lo) as u8) << 4);
                }
                let at = n / 2;
                s.num[at..len].fill(0);
                if let [r] = pairs.remainder() { s.num[at] = (get(r) - lo) as u8; }
            }
            let p = prefix_for_literals(width, n, &mut pre);
            return w.page(n, Some((count, &s.dict)), [&[], &[], &pre[..p], &s.num[..len]]);
        }
    }
    let len = n * if wide { 8 } else { 4 };
    if s.num.len() < len { s.num.resize(len, 0); }
    if wide {
        for (b, r) in s.num[..len].chunks_exact_mut(8).zip(rows) { b.copy_from_slice(&get(r).to_le_bytes()); }
    } else {
        for (b, r) in s.num[..len].chunks_exact_mut(4).zip(rows) { b.copy_from_slice(&(get(r) as i32).to_le_bytes()); }
    }
    w.page(n, None, [&[], &[], &[], &s.num[..len]])
}

fn top_category(w: &mut Writer<'_>, s: &mut Scratch, t: &Tables, cats: Option<&[u8]>,
    rows: &[UserRow]) -> io::Result<Chunk> {
    s.bitmap.clear();
    s.values.clear();
    let mut defined = 0;
    for group in rows.chunks(8) {
        let mut mask = 0;
        for (b, r) in group.iter().enumerate() {
            if r.top == u16::MAX { continue; }
            let name = t.cat_names.get(r.top as usize).ok_or_else(|| invalid("invalid top_category index"))?;
            mask |= 1 << b;
            defined += 1;
            if cats.is_some() { s.values.push(r.top as u8); }
            else { plain_string(&mut s.values, name); }
        }
        s.bitmap.push(mask);
    }
    let n = rows.len();
    let mut levels = [0; 16];
    let (end, bitmap): (usize, &[u8]) = if defined == 0 || defined == n {
        let end = 4 + var((n as u64) << 1, &mut levels[4..]);
        levels[end] = u8::from(defined != 0);
        (end + 1, &[])
    } else {
        let end = 4 + var(s.bitmap.len() as u64 * 2 + 1, &mut levels[4..]);
        (end, &s.bitmap)
    };
    levels[..4].copy_from_slice(&((end - 4 + bitmap.len()) as u32).to_le_bytes());
    if let Some(dict) = cats.filter(|_| defined != 0) {
        let mut pre = [0; 12];
        let p = prefix_for_literals(8, defined, &mut pre);
        s.values.resize(defined.div_ceil(8) * 8, 0);
        w.page(n, Some((t.cat_names.len(), dict)), [&levels[..end], bitmap, &pre[..p], &s.values])
    } else {
        w.page(n, None, [&levels[..end], bitmap, &[], &s.values])
    }
}

fn user_file(path: &Path, cols: &[Col; 6], t: &Tables, cats: Option<&[u8]>, rows: &[UserRow]) -> io::Result<()> {
    staged(path, |file| {
        let mut w = Writer::new(file, cols)?;
        let mut s = Scratch::default();
        for r in rows.chunks(GROUP) {
            let chunks = [
                user_ids(&mut w, &mut s, t, r)?,
                integers(&mut w, &mut s, cols[1], r, true, |r| i64::from(r.day))?,
                integers(&mut w, &mut s, cols[2], r, true, |r| r.sessions)?,
                integers(&mut w, &mut s, cols[3], r, true, |r| r.events)?,
                integers(&mut w, &mut s, cols[4], r, false, |r| r.revenue)?,
                top_category(&mut w, &mut s, t, cats, r)?,
            ];
            w.group(r.len(), &chunks);
        }
        w.finish()
    })
}

fn campaign_file(path: &Path, t: &Tables) -> io::Result<()> {
    let mut b = [0u64; 4];
    for r in &t.camps_out {
        b[0] |= r.sessions as u64; b[1] |= r.purchasing as u64;
        b[2] |= r.revenue as u64; b[3] |= r.assisted as u64;
    }
    let cols = [Col("campaign_id", Kind::Str, false), Col("channel", Kind::Str, false),
        Col("day", Kind::Date, false), Col::integer("sessions", b[0]),
        Col::integer("purchasing_sessions", b[1]), Col::integer("revenue_cents", b[2]),
        Col::integer("assisted_sessions", b[3])];
    staged(path, |f| {
        let mut w = Writer::new(f, &cols)?;
        let mut s = Scratch::default();
        for r in t.camps_out.chunks(GROUP) {
            let mut text = |channel| -> io::Result<Chunk> {
                s.values.clear();
                for row in r {
                    let camp = t.camps.get(row.camp as usize).ok_or_else(|| invalid("invalid campaign index"))?;
                    plain_string(&mut s.values, if channel { &camp.channel } else { &camp.name });
                }
                w.page(r.len(), None, [&[], &[], &[], &s.values])
            };
            let chunks = [text(false)?, text(true)?,
                integers(&mut w, &mut s, cols[2], r, false, |r: &CampRow| i64::from(r.day))?,
                integers(&mut w, &mut s, cols[3], r, false, |r| r.sessions)?,
                integers(&mut w, &mut s, cols[4], r, false, |r| r.purchasing)?,
                integers(&mut w, &mut s, cols[5], r, false, |r| r.revenue)?,
                integers(&mut w, &mut s, cols[6], r, false, |r| r.assisted)?];
            w.group(r.len(), &chunks);
        }
        w.finish()
    })
}

// Never publish an incomplete part. A killed writer can only leave a hidden
// temp file, which the challenge ignores and a subsequent run won't overwrite.
fn staged(path: &Path, encode: impl FnOnce(File) -> io::Result<()>) -> io::Result<()> {
    let name = path.file_name().unwrap().to_string_lossy();
    let mut generation = 0u64;
    let (tmp, file) = loop {
        let tmp = path.with_file_name(format!(".{name}.{}.{generation}.tmp", std::process::id()));
        match File::create_new(&tmp) {
            Ok(f) => break (tmp, f),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => generation += 1,
            Err(e) => return Err(e),
        }
    };
    let result = encode(file).and_then(|_| fs::rename(&tmp, path));
    if result.is_err() { let _ = fs::remove_file(tmp); }
    result
}

// Both our footer and the previous single-file writer end with created_by
// followed by STOP. Do not claim a foreign file just because its name matches.
fn ours(path: &Path, legacy: bool) -> io::Result<bool> {
    let mut f = File::open(path)?;
    let size = f.metadata()?.len();
    let n = size.min(64) as usize;
    if n < 12 { return Ok(false); }
    let mut b = [0; 64];
    f.seek(SeekFrom::End(-(n as i64)))?;
    f.read_exact(&mut b[..n])?;
    if &b[n - 4..n] != MAGIC { return Ok(false); }
    let footer = u32::from_le_bytes(b[n-8..n-4].try_into().unwrap());
    if u64::from(footer) + 12 > size { return Ok(false); }
    let tail = &b[..n - 8];
    let tagged = |name: &[u8]| {
        let l = name.len();
        l + 3 <= tail.len() && l + 3 <= footer as usize && tail[tail.len()-1] == 0
            && tail[tail.len()-l-3..tail.len()-l-1] == [0x28, l as u8]
            && &tail[tail.len()-l-1..tail.len()-1] == name
    };
    Ok(tagged(CREATOR) || (legacy && tagged(b"solve")))
}

fn parts(dir: &Path, count: usize) -> io::Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    fs::create_dir_all(dir)?;
    let mut foreign = HashSet::new();
    let mut prior = Vec::new();
    for item in fs::read_dir(dir)? {
        let item = item?;
        let name = item.file_name();
        let Some(digits) = name.to_str().and_then(|s| s.strip_prefix("part-"))
            .and_then(|s| s.strip_suffix(".parquet")) else { continue };
        let Ok(idx) = digits.parse::<usize>() else { continue };
        if digits != format!("{idx:05}") { continue; }
        if item.file_type()?.is_file() && ours(&item.path(), idx == 0)? {
            prior.push((idx, item.path()));
        } else { foreign.insert(idx); }
    }
    let mut idx = 0;
    let mut paths = Vec::with_capacity(count);
    while paths.len() < count {
        if !foreign.contains(&idx) { paths.push(dir.join(format!("part-{idx:05}.parquet"))); }
        idx += 1;
    }
    Ok((paths, prior.into_iter().filter_map(|(i, p)| (i >= idx).then_some(p)).collect()))
}

fn widths(rows: &[UserRow]) -> [u64; 3] {
    let mut b = [0; 3];
    for r in rows {
        b[0] |= r.sessions as u64;
        b[1] |= r.events as u64;
        b[2] |= r.revenue as u64;
    }
    b
}

pub(super) fn write(dir: &Path, t: &Tables, threads: usize) -> io::Result<()> {
    let n = t.users.len();
    let p = threads.max(1).min(n.div_ceil(MIN_PART).max(1));
    let (paths, mut stale) = parts(&dir.join("user_daily"), p)?;
    let (camp, stale_camp) = parts(&dir.join("campaign_daily"), 1)?;
    stale.extend(stale_camp);
    let at = |i: usize| (n / p) * i + i.min(n % p);
    let groups: Vec<_> = (0..p).map(|i| &t.users[at(i)..at(i+1)]).collect();
    // One common physical schema, even if just one whale overflows INT32.
    let b = thread::scope(|scope| {
        let jobs: Vec<_> = groups[1..].iter().map(|&r| scope.spawn(move || widths(r))).collect();
        let mut all = widths(groups[0]);
        for job in jobs {
            let b = job.join().unwrap();
            for i in 0..3 { all[i] |= b[i]; }
        }
        all
    });
    let cols = [Col("user_id", Kind::Str, false), Col("day", Kind::Date, false),
        Col::integer("sessions", b[0]), Col::integer("events", b[1]),
        Col::integer("revenue_cents", b[2]), Col("top_category", Kind::Str, true)];
    let cat_dict = if !t.cat_names.is_empty() && t.cat_names.len() <= 256
        && t.cat_names.iter().map(|n| n.len() + 4).sum::<usize>() <= 64 * 1024 {
        let mut d = Vec::new();
        for n in &t.cat_names { plain_string(&mut d, n); }
        Some(d)
    } else { None };
    let cats = cat_dict.as_deref();
    let cols = &cols;
    thread::scope(|scope| -> io::Result<()> {
        let jobs: Vec<_> = paths[1..].iter().zip(&groups[1..])
            .map(|(path, &r)| scope.spawn(move || user_file(path, cols, t, cats, r))).collect();
        user_file(&paths[0], cols, t, cats, groups[0])?;
        campaign_file(&camp[0], t)?;
        for job in jobs { job.join().unwrap()?; }
        Ok(())
    })?;
    for file in stale { fs::remove_file(file)?; }
    Ok(())
}
