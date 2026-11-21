//! One-pass scanner for the deliberately small CSV dialect used by the input
//! files.
//!
//! A comma always separates fields and only LF separates records. Quotes are
//! valid only when exactly two of them wrap an entire field; they do not turn
//! commas or LF into data. CR and every non-structural byte are ordinary field
//! bytes.
//!
//! Each 64-byte block becomes three bitmasks (comma, LF, quote). Commas are
//! recorded per row; quotes are never visited one by one. A row is well quoted
//! exactly when its total quote count equals twice the number of fields that
//! both begin and end with a quote: each such field holds at least two quotes,
//! so equality forces two per wrapped field and none anywhere else.

/// Call `f` for every well-formed record containing exactly `N` fields.
///
/// The input must begin at a record boundary. It may end either after LF or at
/// the end of a final unterminated record. Empty fields are returned as empty
/// slices, and syntactic quotes are removed from returned quoted fields.
#[inline]
pub(super) fn rows<'b, const N: usize>(buf: &'b [u8], f: impl FnMut([&'b [u8]; N])) {
    rows_window::<N>(buf, WINDOW, f);
}

/// Bytes per two-stage window. A multiple of 64 so only the input's final
/// block is ever partial.
const WINDOW: usize = 1 << 15;
const POISON: u32 = 1 << 31;

#[derive(Clone, Copy)]
struct Line {
    pos: u32, // LF offset from the window start (or EOF for a final record)
    cc: u32,  // commas before this LF in the window, | POISON for a stray quote
}

/// Two stages per window. Stage 1 turns 64-byte blocks into a dense list of
/// comma offsets and one entry per LF, without data-dependent loops in the
/// common case. Stage 2 walks rows: a field count is a difference of two comma
/// counts, so the row loop has one well-predicted branch.
///
/// Quotes: a quote whose neighbours are both non-separators lies strictly
/// inside a field and always invalidates its row ("stray"). Every other quote is
/// a field's first or last byte, so a field is well quoted exactly when
/// "first or last byte is a quote" equals "both are, and it has 2+ bytes".
#[inline]
pub(super) fn rows_window<'b, const N: usize>(buf: &'b [u8], window: usize, mut f: impl FnMut([&'b [u8]; N])) {
    debug_assert!(window % 64 == 0 && window > 0 && window < (1 << 31));
    let len = buf.len();
    let mut commas: Vec<u32> = Vec::with_capacity(window + 72);
    let mut lines: Vec<Line> = Vec::with_capacity(window + 72);
    let mut start = 0;
    while start < len {
        let end = (start + window).min(len);
        let next = window_rows::<N>(buf, start, end, &mut commas, &mut lines, &mut f);
        if next > start {
            start = next;
        } else {
            // A full window without LF: one long row. Hand it to the simple
            // scanner, which has no length limit.
            let stop = memchr::memchr(b'\n', &buf[end..]).map_or(len, |i| end + i + 1);
            rows_simple::<N>(&buf[start..stop], &mut f);
            start = stop;
        }
    }
}

#[inline(always)]
fn is_sep(b: u8) -> u64 {
    u64::from(b == b',' || b == b'\n')
}

/// Scan `buf[start..end]` and emit every complete row in it. Returns the
/// offset just past the last complete row, or `start` if there was none.
#[inline(always)]
fn window_rows<'b, const N: usize>(
    buf: &'b [u8],
    start: usize,
    end: usize,
    commas: &mut Vec<u32>,
    lines: &mut Vec<Line>,
    f: &mut impl FnMut([&'b [u8]; N]),
) -> usize {
    let len = buf.len();
    let at_eof = end == len;
    let cp = commas.as_mut_ptr();
    let lp = lines.as_mut_ptr();
    let (mut nc, mut nl) = (0usize, 0usize);
    let mut prev_s = 1u64; // a window starts at a record boundary
    let mut pending = 0u32; // poison for the next LF (stray quote in its row)
    let mut off = start;
    while off < end {
        let rem = end - off;
        let rel = (off - start) as u32;
        // SAFETY: a full block lies inside `buf`; a partial one (only at EOF)
        // is copied into zero padding, which holds no structural byte.
        let (c, n, q) = unsafe {
            if rem >= 64 {
                masks64(buf.as_ptr().add(off))
            } else {
                let mut tmp = [0u8; 64];
                tmp[..rem].copy_from_slice(&buf[off..end]);
                masks64(tmp.as_ptr())
            }
        };
        let s = c | n;
        // The byte after this block, for quotes in its last lane. EOF counts as
        // a separator; in a partial block it sits at bit `rem`.
        let (next_s, tail) = if rem > 64 {
            (is_sep(buf[off + 64]), 0)
        } else if rem == 64 {
            (if off + 64 < len { is_sep(buf[off + 64]) } else { 1 }, 0)
        } else {
            (0, 1u64 << rem)
        };
        let near = (s << 1) | prev_s | ((s | tail) >> 1) | (next_s << 63);
        let stray = q & !near;
        prev_s = s >> 63;

        // Commas: eight unconditional writes cover almost every block.
        let count = c.count_ones() as usize;
        let mut m = c;
        // SAFETY: capacity is window + 72 and a window has at most `window`
        // commas, so writes at nc..nc+8 (and the overflow loop) stay in bounds.
        unsafe {
            for j in 0..8 {
                *cp.add(nc + j) = rel + m.trailing_zeros();
                m &= m.wrapping_sub(1);
            }
            let mut j = 8;
            while m != 0 {
                *cp.add(nc + j) = rel + m.trailing_zeros();
                m &= m - 1;
                j += 1;
            }
        }

        // LFs: two unconditional entries, then any extras.
        let lfs = n.count_ones() as usize;
        let b0 = n.trailing_zeros();
        let n1 = n & n.wrapping_sub(1);
        let b1 = n1.trailing_zeros();
        let below = |b: u32| !u64::MAX.wrapping_shl(b) | if b >= 64 { u64::MAX } else { 0 };
        let first_poison = pending & if n != 0 { POISON } else { 0 };
        pending &= if n != 0 { 0 } else { POISON };
        // SAFETY: as for commas; a window has at most `window` LFs.
        unsafe {
            *lp.add(nl) = Line { pos: rel + b0, cc: (nc + (c & below(b0)).count_ones() as usize) as u32 | first_poison };
            *lp.add(nl + 1) = Line { pos: rel + b1, cc: (nc + (c & below(b1)).count_ones() as usize) as u32 };
            if lfs > 2 {
                let mut m = n1 & n1.wrapping_sub(1);
                let mut j = 2;
                while m != 0 {
                    let b = m.trailing_zeros();
                    *lp.add(nl + j) = Line { pos: rel + b, cc: (nc + (c & below(b)).count_ones() as usize) as u32 };
                    m &= m - 1;
                    j += 1;
                }
            }
        }
        if stray != 0 {
            pending |= poison_rows(lp, nl, n, stray);
        }
        nc += count;
        nl += lfs;
        off += 64;
    }
    if at_eof && buf[len - 1] != b'\n' {
        // EOF terminates an unterminated final record.
        // SAFETY: at most `window` LFs were written, so this slot exists.
        unsafe { *lp.add(nl) = Line { pos: (len - start) as u32, cc: nc as u32 | pending }; }
        nl += 1;
    }
    // SAFETY: entries 0..nc and 0..nl were initialized above.
    let commas = unsafe { std::slice::from_raw_parts(cp, nc) };
    let lines = unsafe { std::slice::from_raw_parts(lp, nl) };

    let last = len - 1;
    let mut row_start = start;
    let mut cc0 = 0usize;
    for line in lines {
        let cc1 = (line.cc & !POISON) as usize;
        let lf = start + line.pos as usize;
        if cc1 - cc0 == N - 1 && line.cc & POISON == 0 {
            let mut fields = [&buf[..0]; N];
            let mut bad = false;
            let mut s = row_start;
            for (i, field) in fields.iter_mut().enumerate() {
                let e = if i == N - 1 { lf } else { start + commas[cc0 + i] as usize };
                debug_assert!(s <= e && e <= len);
                // SAFETY: both indexes are clamped into a non-empty `buf`. An
                // empty field reads separators here, which are never quotes.
                let (a, z) = unsafe {
                    (*buf.get_unchecked(s.min(last)) == b'"', *buf.get_unchecked(e.saturating_sub(1)) == b'"')
                };
                let w = (e - s >= 2) & a & z;
                bad |= (a | z) != w;
                let w = w as usize;
                // SAFETY: separators are increasing, so s <= e, and w = 1
                // only when the field has at least two bytes.
                *field = unsafe { buf.get_unchecked(s + w..e - w) };
                s = e + 1;
            }
            if !bad {
                f(fields);
            }
        }
        row_start = lf + 1;
        cc0 = cc1;
    }
    row_start.min(len)
}

/// Poison the row of every stray quote in a block: the row's LF is the first
/// LF after the quote. Returns POISON if that LF lies in a later block.
#[cold]
#[inline(never)]
fn poison_rows(lp: *mut Line, nl: usize, n: u64, mut stray: u64) -> u32 {
    let mut pending = 0;
    while stray != 0 {
        let b = stray.trailing_zeros();
        if n >> b == 0 {
            pending = POISON;
        } else {
            let k = (n & ((1u64 << b) - 1)).count_ones() as usize;
            // SAFETY: entry nl + k was written for an LF of this block.
            unsafe { (*lp.add(nl + k)).cc |= POISON };
        }
        stray &= stray - 1;
    }
    pending
}

/// The single-stage scanner: no row-length limit.
#[inline]
fn rows_simple<'b, const N: usize>(buf: &'b [u8], mut f: impl FnMut([&'b [u8]; N])) {
    debug_assert!((2..=8).contains(&N));
    let len = buf.len();
    if len == 0 {
        return;
    }
    let mut st = Rows::<N> { ends: [0; 8], cnt: 0, row_start: 0, qrow: 0, qbase: 0 };
    let mut at = 0usize;
    while len - at >= 64 {
        // SAFETY: the loop condition keeps all 64 bytes inside `buf`.
        let (c, n, q) = unsafe { masks64(buf.as_ptr().add(at)) };
        st.block(buf, at, c, n, q, &mut f);
        at += 64;
    }
    let tail_nl = buf[len - 1] != b'\n';
    if at < len {
        // Zero bytes are not structural, so padding adds no bits.
        let mut tmp = [0u8; 64];
        tmp[..len - at].copy_from_slice(&buf[at..]);
        // SAFETY: `tmp` holds exactly 64 bytes.
        let (c, mut n, q) = unsafe { masks64(tmp.as_ptr()) };
        if tail_nl {
            // EOF terminates an unterminated final record.
            n |= 1u64 << (len - at);
        }
        st.block(buf, at, c, n, q, &mut f);
    } else if tail_nl {
        let rowq = st.qbase - st.qrow;
        st.finish(buf, len, rowq, &mut f);
    }
}

struct Rows<const N: usize> {
    ends: [usize; 8],
    cnt: usize,
    row_start: usize,
    qrow: u32,
    qbase: u32,
}

impl<const N: usize> Rows<N> {
    #[inline(always)]
    fn push_commas(&mut self, base: usize, mut m: u64) {
        while m != 0 {
            self.ends[self.cnt & 7] = base + m.trailing_zeros() as usize;
            self.cnt += 1;
            m &= m - 1;
        }
    }

    #[inline(always)]
    fn block<'b>(&mut self, buf: &'b [u8], base: usize, mut c: u64, mut n: u64, q: u64,
                 f: &mut impl FnMut([&'b [u8]; N])) {
        while n != 0 {
            let bit = n.trailing_zeros();
            let below = (1u64 << bit) - 1;
            self.push_commas(base, c & below);
            c &= !below;
            let qnow = self.qbase + (q & below).count_ones();
            self.finish(buf, base + bit as usize, qnow - self.qrow, f);
            self.qrow = qnow;
            n &= n - 1;
        }
        self.push_commas(base, c);
        self.qbase += q.count_ones();
    }

    /// Close the record whose terminator (LF or EOF) is at `end`.
    #[inline(always)]
    fn finish<'b>(&mut self, buf: &'b [u8], end: usize, rowq: u32, f: &mut impl FnMut([&'b [u8]; N])) {
        if self.cnt == N - 1 {
            let last = buf.len() - 1;
            let mut fields = [&buf[..0]; N];
            let mut wrapped = 0u32;
            let mut s = self.row_start;
            for (i, field) in fields.iter_mut().enumerate() {
                let e = if i == N - 1 { end } else { self.ends[i] };
                debug_assert!(s <= e && e <= buf.len());
                // SAFETY: both indexes are clamped into a non-empty `buf`.
                let (first, final_) = unsafe {
                    (*buf.get_unchecked(s.min(last)), *buf.get_unchecked(e.saturating_sub(1)))
                };
                let w = ((e - s >= 2) & (first == b'"') & (final_ == b'"')) as usize;
                // SAFETY: separators are visited in increasing order, so
                // `s <= e`, and `w` is 1 only when the field has 2+ bytes.
                *field = unsafe { buf.get_unchecked(s + w..e - w) };
                wrapped += 2 * w as u32;
                s = e + 1;
            }
            if wrapped == rowq {
                f(fields);
            }
        }
        self.cnt = 0;
        self.row_start = end + 1;
    }
}

/// Comma, LF and quote masks of 64 bytes at `p`; bit i describes `p[i]`.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p.cast());
        let b = _mm256_loadu_si256(p.add(32).cast());
        let m = |c: u8| {
            let k = _mm256_set1_epi8(c as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, k)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, k)) as u32 as u64;
            lo | hi << 32
        };
        (m(b','), m(b'\n'), m(b'"'))
    }
    #[cfg(not(target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let v = [0, 16, 32, 48].map(|o| _mm_loadu_si128(p.add(o).cast()));
        let m = |c: u8| {
            let k = _mm_set1_epi8(c as i8);
            v.iter().enumerate().fold(0u64, |acc, (i, &x)| {
                acc | (_mm_movemask_epi8(_mm_cmpeq_epi8(x, k)) as u32 as u64) << (16 * i)
            })
        };
        (m(b','), m(b'\n'), m(b'"'))
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::aarch64::*;
    const WEIGHTS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
    unsafe {
        let w = vld1q_u8(WEIGHTS.as_ptr());
        let v = [vld1q_u8(p), vld1q_u8(p.add(16)), vld1q_u8(p.add(32)), vld1q_u8(p.add(48))];
        let m = |c: u8| {
            let k = vdupq_n_u8(c);
            let s0 = vpaddq_u8(vandq_u8(vceqq_u8(v[0], k), w), vandq_u8(vceqq_u8(v[1], k), w));
            let s1 = vpaddq_u8(vandq_u8(vceqq_u8(v[2], k), w), vandq_u8(vceqq_u8(v[3], k), w));
            let s = vpaddq_u8(s0, s1);
            vgetq_lane_u64(vreinterpretq_u64_u8(vpaddq_u8(s, s)), 0)
        };
        (m(b','), m(b'\n'), m(b'"'))
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        // SAFETY: callers pass 64 readable bytes.
        let b = unsafe { *p.add(i) };
        c |= u64::from(b == b',') << i;
        n |= u64::from(b == b'\n') << i;
        q |= u64::from(b == b'"') << i;
    }
    (c, n, q)
}

#[cfg(test)]
mod tests {
    use super::{rows, rows_simple, rows_window};

    fn reference<'b, const N: usize>(buf: &'b [u8], mut emit: impl FnMut([&'b [u8]; N])) {
        fn line<'b, const N: usize>(line: &'b [u8]) -> Option<[&'b [u8]; N]> {
            let mut fields = [&line[..0]; N];
            let mut count = 0usize;
            for raw in line.split(|&byte| byte == b',') {
                if count == N {
                    return None;
                }
                let quotes = raw.iter().filter(|&&byte| byte == b'"').count();
                fields[count] = match quotes {
                    0 => raw,
                    2 if raw.len() >= 2 && raw[0] == b'"' && raw[raw.len() - 1] == b'"' => {
                        &raw[1..raw.len() - 1]
                    }
                    _ => return None,
                };
                count += 1;
            }
            (count == N).then_some(fields)
        }

        let mut start = 0usize;
        for (at, &byte) in buf.iter().enumerate() {
            if byte == b'\n' {
                if let Some(fields) = line::<N>(&buf[start..at]) {
                    emit(fields);
                }
                start = at + 1;
            }
        }
        if start < buf.len() {
            if let Some(fields) = line::<N>(&buf[start..]) {
                emit(fields);
            }
        }
    }

    fn owned<const N: usize>(buf: &[u8], simd: bool) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut push = |fields: [&[u8]; N]| {
            out.push(fields.iter().map(|field| field.to_vec()).collect());
        };
        if simd {
            rows::<N>(buf, &mut push);
        } else {
            reference::<N>(buf, &mut push);
        }
        out
    }

    fn collect<const N: usize>(buf: &[u8], run: impl FnOnce(&[u8], &mut dyn FnMut([&[u8]; N]))) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        run(buf, &mut |fields: [&[u8]; N]| out.push(fields.iter().map(|field| field.to_vec()).collect()));
        out
    }

    fn check<const N: usize>(buf: &[u8]) {
        let want = owned::<N>(buf, false);
        assert_eq!(owned::<N>(buf, true), want, "input={buf:?}");
        assert_eq!(collect::<N>(buf, |b, f| rows_simple::<N>(b, f)), want, "simple input={buf:?}");
        for window in [64, 128, 192, 256] {
            assert_eq!(collect::<N>(buf, |b, f| rows_window::<N>(b, window, f)), want, "window={window} input={buf:?}");
        }
    }

    #[test]
    fn stray_and_edge_quotes() {
        for input in [
            b"a\"b,c\n".as_slice(), b"\"a\"b,c\n", b"\",c\n", b"\"\",c\n", b"\"\"\",c\n", b"\"\"\"\",c\n",
            b"a,\"\n", b"a,\"b\"\r\n", b"a,\"b\r\"\n", b"\"a\",\"b\"\n", b"\"a,b\"\n", b"a\",\"b\n",
            b"x\nab\"c,d\ne,f\n", b"\"\",\"\"", b"a,\"", b"a,\"\"", b"\"", b",\"\"\n,",
        ] {
            check::<2>(input);
        }
        // A stray quote poisons only its own row, also across 64-byte blocks.
        let mut long = Vec::new();
        for i in 0..40usize {
            long.extend(std::iter::repeat_n(b'x', i));
            long.extend_from_slice(if i % 3 == 0 { b"a\"b,c\n" } else { b"\"q\",zz\n" });
        }
        check::<2>(&long);
    }

    #[test]
    fn exact_dialect_examples() {
        check::<2>(b"a,b\n\"c\",\"\"\nlast,record");
        check::<4>(b"a,b,c,d\n\"a\",b,\"c\",d\n");
        check::<7>(b"e1,u1,2026-01-01T00:00:00Z,2026-01-01T00:00:01Z,view,p1,\n");

        assert_eq!(
            owned::<2>(b"\"hello\",\"world\"\n", true),
            vec![vec![b"hello".to_vec(), b"world".to_vec()]]
        );
        // A quoted slice points into the source and excludes both wrappers.
        let source = b"\"ab\",cd\n";
        rows::<2>(source, |fields| {
            assert_eq!(fields[0].as_ptr(), unsafe { source.as_ptr().add(1) });
            assert_eq!(fields[0], b"ab");
        });
    }

    #[test]
    fn malformed_quotes_and_field_counts_are_dropped() {
        for input in [
            b"\"a,b\n".as_slice(),
            b"a\",b\n",
            b"\"a\"x,b\n",
            b"x\"a\",b\n",
            b"\"a\"\",b\n",
            b"a\n",
            b"a,b,c\n",
            b"\n",
        ] {
            check::<2>(input);
            assert!(owned::<2>(input, true).is_empty(), "input={input:?}");
        }
        // Commas are delimiters even between quotes in this dialect.
        assert!(owned::<2>(b"\"a,b\"\n", true).is_empty());
    }

    #[test]
    fn eof_empty_cr_and_utf8_are_bytes_exact() {
        check::<2>(b"a,b");
        check::<2>(b"a,");
        check::<2>(b",b");
        check::<2>(b",");
        check::<2>(b"a,b\n");
        check::<2>(b"a,b\n\n");
        check::<2>(b"a,b\nlast,");
        check::<2>("é,雪\nλ,🙂".as_bytes());

        assert_eq!(owned::<2>(b"a,b\r\n", true)[0][1], b"b\r");
        // CR after a closing quote means the quote no longer wraps the whole
        // field, because CR is ordinary data rather than part of the newline.
        assert!(owned::<2>(b"a,\"b\"\r\n", true).is_empty());
    }

    #[test]
    fn every_simd_boundary_and_chunk_tail() {
        for len in 0..=96usize {
            let a = vec![b'a'; len];
            let b = vec![b'b'; 97 - len];
            let mut input = Vec::new();
            input.extend_from_slice(&a);
            input.push(b',');
            input.extend_from_slice(&b);
            if len & 1 == 0 {
                input.push(b'\n');
            }
            check::<2>(&input);

            let mut quoted = Vec::new();
            quoted.push(b'"');
            quoted.extend_from_slice(&a);
            quoted.extend_from_slice(b"\",tail\n");
            check::<2>(&quoted);
        }

        // Shift later records through every position modulo 32.
        for shift in 0..64usize {
            let mut input = Vec::new();
            input.extend(std::iter::repeat_n(b'x', shift));
            input.extend_from_slice(b",z\n\"left\",right\n");
            check::<2>(&input);
        }
    }

    #[test]
    fn differential_random_bytes() {
        let alphabet = [
            b',', b'\n', b'"', b'\r', b'a', b'Z', b'0', b' ', 0, 0x7f, 0x80, 0xa9, 0xc3, 0xe9, 0xff,
        ];
        let mut state = 0xD1B5_4A32_D192_ED03u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        for _ in 0..12_000 {
            let len = (next() as usize) & 511;
            let mut input = Vec::with_capacity(len);
            for _ in 0..len {
                input.push(alphabet[next() as usize % alphabet.len()]);
            }
            check::<2>(&input);
            check::<4>(&input);
            check::<7>(&input);
        }
    }
}

#[cfg(test)]
pub(super) unsafe fn masks64_for_bench(p: *const u8) -> (u64, u64, u64) {
    unsafe { masks64(p) }
}
