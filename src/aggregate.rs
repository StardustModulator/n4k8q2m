//! One person's validated, linked history at a time. A session's start fixes
//! both its attribution window and its output day, even if it lasts past midnight.

use super::{cmp_keys, SlowIntern, UserRow};
use crate::time::{DAY_MS, GAP_MS, WEEK_MS};

#[derive(Clone, Copy)]
pub(super) struct Hit {
    pub(super) id: u64,
    pub(super) ts: u32,
    pub(super) camp: u16,
}

struct ClickCursor<'a> {
    pending: &'a [Hit],
    best: Option<Hit>,
}

pub(super) struct Acc {
    pub(super) rows: Vec<UserRow>,
    pub(super) stats: Vec<[i64; 4]>,
    ndays: usize,
    organic: usize,
    // Dense list of (campaign, latest click time). A slot is valid only if
    // it points back to the same campaign, so clearing the list is O(1).
    active: Vec<(u16, u32)>,
    slot: Vec<usize>,
    cat_cents: Vec<i64>,
    touched_cats: Vec<u8>,
}

impl Acc {
    pub(super) fn new(n_camps: usize, ndays: usize, n_cats: usize) -> Self {
        let organic = n_camps - 1;
        Self {
            rows: Vec::new(),
            stats: vec![[0; 4]; n_camps * ndays],
            ndays,
            organic,
            active: Vec::new(),
            slot: vec![0; organic],
            cat_cents: vec![0; n_cats],
            touched_cats: Vec::new(),
        }
    }

    #[inline]
    fn observe(&mut self, hit: Hit) {
        let slot = &mut self.slot[usize::from(hit.camp)];
        if let Some((camp, latest)) = self.active.get_mut(*slot) {
            if *camp == hit.camp {
                *latest = hit.ts;
                return;
            }
        }
        *slot = self.active.len();
        self.active.push((hit.camp, hit.ts));
    }

    /// Count this session's distinct assists and return its credited stats slot.
    fn attribute(
        &mut self,
        start: u32,
        day: usize,
        cursor: &mut ClickCursor<'_>,
        slow: &SlowIntern,
    ) -> usize {
        let lower = start - WEEK_MS as u32;
        while let Some((&hit, rest)) = cursor.pending.split_first() {
            if hit.ts > start {
                break;
            }
            cursor.pending = rest;
            if hit.ts < lower {
                continue;
            }
            self.observe(hit);
            // Times are nondecreasing; only equal times need an ID comparison.
            if cursor.best.is_none_or(|old| {
                hit.ts != old.ts || cmp_keys(hit.id, old.id, slow).is_lt()
            }) {
                cursor.best = Some(hit);
            }
        }

        let Some(best) = cursor.best.filter(|h| h.ts >= lower) else {
            // If the latest click expired, every campaign expired with it.
            self.active.clear();
            cursor.best = None;
            return self.organic * self.ndays + day;
        };

        let mut i = 0;
        while i < self.active.len() {
            let (camp, latest) = self.active[i];
            if latest < lower {
                self.active.swap_remove(i);
                if let Some(&(moved, _)) = self.active.get(i) {
                    self.slot[usize::from(moved)] = i;
                }
            } else {
                self.stats[usize::from(camp) * self.ndays + day][3] += 1;
                i += 1;
            }
        }
        usize::from(best.camp) * self.ndays + day
    }

    fn take_top_category(&mut self, cats: &[Vec<u8>]) -> u16 {
        let mut best = None::<(u8, i64)>;
        for c in self.touched_cats.drain(..) {
            let i = usize::from(c);
            let cents = std::mem::take(&mut self.cat_cents[i]);
            if best.is_none_or(|(b, max)| {
                cents > max || (cents == max && cats[i] < cats[usize::from(b)])
            }) {
                best = Some((c, cents));
            }
        }
        best.map_or(u16::MAX, |(c, _)| u16::from(c))
    }

    pub(super) fn person(
        &mut self,
        key: u64,
        events: &mut [u64],
        clicks: &mut [Hit],
        cats: &[Vec<u8>],
        slow: &SlowIntern,
        first_day: i32,
    ) {
        if events.is_empty() {
            return;
        }
        events.sort_unstable(); // Timestamp is the high word; tie order is immaterial.
        clicks.sort_unstable_by_key(|h| h.ts);
        self.active.clear();
        let mut cursor = ClickCursor { pending: clicks, best: None };
        let mut ev: &[u64] = events;
        while let Some(&first) = ev.first() {
            let mut start = timestamp(first);
            let day = day_index(start);
            debug_assert!(day < self.ndays);
            let before = ev.len();
            let mut sessions = 0;
            let mut revenue = 0;
            loop {
                let credited = self.attribute(start, day, &mut cursor, slow);
                let mut prev = start;
                let mut session_cents = 0;
                while let Some((&e, rest)) = ev.split_first() {
                    let ts = timestamp(e);
                    if ts - prev > GAP_MS as u32 {
                        break;
                    }
                    ev = rest;
                    prev = ts;
                    let price = i64::from(e as u32 & 0x00ff_ffff);
                    if price != 0 {
                        let cat = (e >> 24) as u8;
                        let sum = &mut self.cat_cents[usize::from(cat)];
                        if *sum == 0 {
                            self.touched_cats.push(cat);
                        }
                        *sum += price;
                        session_cents += price;
                    }
                }
                sessions += 1;
                revenue += session_cents;
                let s = &mut self.stats[credited];
                s[0] += 1;
                s[1] += i64::from(session_cents != 0);
                s[2] += session_cents;
                let Some(&next) = ev.first() else { break };
                start = timestamp(next);
                if day_index(start) != day {
                    break;
                }
            }
            let top = self.take_top_category(cats);
            self.rows.push(UserRow {
                key,
                day: first_day + day as i32,
                sessions,
                events: (before - ev.len()) as i64,
                revenue,
                top,
            });
        }
    }
}

#[inline]
fn timestamp(event: u64) -> u32 {
    (event >> 32) as u32
}

#[inline]
fn day_index(start: u32) -> usize {
    ((start - WEEK_MS as u32) / DAY_MS as u32) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    const WEEK: u32 = WEEK_MS as u32;
    const DAY: u32 = DAY_MS as u32;
    const GAP: u32 = GAP_MS as u32;

    fn ev(ts: u32, cat: u8, cents: u32) -> u64 {
        (u64::from(ts) << 32) | (u64::from(cat) << 24) | u64::from(cents)
    }

    fn row(r: &UserRow) -> (u64, i32, i64, i64, i64, u16) {
        (r.key, r.day, r.sessions, r.events, r.revenue, r.top)
    }

    #[test]
    fn session_gaps_midnight_and_category_totals() {
        let slow = SlowIntern::new();
        let cats = vec![b"zulu".to_vec(), b"unknown".to_vec()];
        let mut acc = Acc::new(1, 16, cats.len());
        let start = WEEK + DAY - 1;
        let after_midnight = start + GAP;
        let next = after_midnight + GAP + 1;
        let mut events = [
            ev(next + GAP + 1, 1, 0),
            ev(WEEK + 15 * DAY, 0, 0),
            ev(after_midnight, 1, 35),
            ev(next, 0, 110),
            ev(after_midnight, 0, 0),
            ev(start, 0, 75),
            ev(after_midnight, 1, 40),
        ];
        acc.person(41, &mut events, &mut [], &cats, &slow, -2);
        assert_eq!(
            acc.rows.iter().map(row).collect::<Vec<_>>(),
            [
                (41, -2, 1, 4, 150, 1), // "unknown" wins a 75:75 tie.
                (41, -1, 2, 2, 110, 0),
                (41, 13, 1, 1, 0, u16::MAX),
            ]
        );
        let mut expected = vec![[0; 4]; 16];
        expected[0] = [1, 1, 150, 0];
        expected[1] = [2, 1, 110, 0];
        expected[15] = [1, 0, 0, 0];
        assert_eq!(acc.stats, expected);
    }

    #[test]
    fn inclusive_click_window_ties_and_duplicate_assists() {
        let mut slow = SlowIntern::new();
        let mut hit = |ts, id: &[u8], camp| Hit {
            id: crate::try_pack(id).unwrap_or_else(|| slow.intern(id)),
            ts,
            camp,
        };
        let a = WEEK + 1;
        let b = a + GAP + 1;
        let c = a + 1 + WEEK;
        let d = c + GAP + 1;
        let dup = hit(a, b"k2", 2);
        let mut clicks = [
            hit(d + 1, b"kfuture", 0),
            dup,
            hit(1, b"katweek", 1),
            hit(a, b"!slow-earliest-id", 3),
            hit(0, b"kexpired", 0),
            hit(a + 1, b"kafter", 4),
            dup,
            hit(a - 1, b"kother", 2),
        ];
        let mut events = [ev(d, 0, 42), ev(b, 0, 20), ev(c, 0, 0), ev(a, 0, 10)];
        let cats = vec![b"book".to_vec()];
        let mut acc = Acc::new(6, 8, cats.len());
        acc.person(1, &mut events, &mut clicks, &cats, &slow, 0);

        let mut expected = vec![[0; 4]; 6 * 8];
        expected[8] = [0, 0, 0, 1]; // Exactly seven days before A, too old for B.
        expected[2 * 8] = [0, 0, 0, 2];
        expected[3 * 8] = [1, 1, 10, 2]; // Slow ID sorts before the packed ID.
        expected[4 * 8] = [1, 1, 20, 1]; // One millisecond after A.
        expected[4 * 8 + 7] = [1, 0, 0, 1]; // Same click credits C, exactly a week later.
        expected[5 * 8 + 7] = [1, 1, 42, 0];
        assert_eq!(acc.stats, expected);
    }

    #[test]
    fn reuse_scratch_across_people_and_sessions() {
        let mut slow = SlowIntern::new();
        let cats = vec![b"z".to_vec(), b"a".to_vec()];
        let mut acc = Acc::new(3, 1, cats.len());
        let first = Hit { ts: 0, id: slow.intern(b"first"), camp: 0 };
        let later = Hit { ts: WEEK, id: slow.intern(b"later"), camp: 1 };
        let refresh = Hit { ts: WEEK + GAP + 2, id: slow.intern(b"refresh"), camp: 1 };
        let mut events = [
            ev(WEEK + 2 * (GAP + 1), 0, 16),
            ev(WEEK, 1, 20),
            ev(WEEK + GAP + 1, 0, 4),
        ];
        acc.person(1, &mut events, &mut [first, later, refresh], &cats, &slow, 7);
        // Camp 0 expires, camp 1 moves to its slot, then gets another click.
        // The two later sessions together tie category 1 on daily revenue.
        assert_eq!(row(&acc.rows[0]), (1, 7, 3, 3, 40, 1));
        acc.person(2, &mut [ev(WEEK, 0, 2)], &mut [first], &cats, &slow, 7);
        acc.person(3, &mut [ev(WEEK, 0, 0)], &mut [], &cats, &slow, 7);
        acc.person(4, &mut [], &mut [later], &cats, &slow, 7);
        assert_eq!(acc.rows.len(), 3);
        assert_eq!(row(&acc.rows[1]), (2, 7, 1, 1, 2, 0));
        assert_eq!(row(&acc.rows[2]), (3, 7, 1, 1, 0, u16::MAX));
        assert_eq!(acc.stats, [[1, 1, 2, 2], [3, 3, 40, 3], [1, 0, 0, 0]]);
    }
}
