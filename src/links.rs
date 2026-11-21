//! Exact linked-id resolution. Endpoints are interned in hash-owned shards,
//! connected in parallel with an atomic disjoint set, and the shard tables are
//! then reused as the immutable key -> component-minimum lookup.

use std::cmp::Ordering;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::thread;

use super::stream::hash;
use super::{SlowIntern, cmp_keys};

#[derive(Clone, Copy, Default)]
struct Entry {
    key: u64,
    // During construction this is a shard-local dense index. In the finished
    // index it is the bytewise-smallest key in `key`'s component.
    value: u64,
}

struct Shard {
    table: Box<[Entry]>,
    mask: usize,
    base: usize,
}

impl Shard {
    #[inline(always)]
    fn slot(&self, key: u64, mixed: u64) -> usize {
        let mut at = mixed as usize & self.mask;
        loop {
            let stored = self.table[at].key;
            if stored == 0 || stored == key {
                return at;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn local_index(&self, key: u64, mixed: u64) -> u32 {
        let entry = self.table[self.slot(key, mixed)];
        debug_assert_eq!(entry.key, key);
        entry.value as u32
    }
}

struct BuiltShard {
    shard: Shard,
    keys: Vec<u64>,
}

/// Immutable exact lookup over every ID cited by a valid link.
pub(super) struct Aliases {
    shards: Box<[Shard]>,
    shard_mask: usize,
    shard_shift: u32,
}

impl Aliases {
    /// Return the bytewise-smallest ID in `k`'s linked component. IDs absent
    /// from all link endpoints are singleton components and resolve to self.
    #[inline(always)]
    pub(super) fn resolve(&self, k: u64) -> u64 {
        debug_assert_ne!(k, 0);
        let mixed = hash(k);
        let shard = &self.shards[shard_of(mixed, self.shard_mask, self.shard_shift)];
        if shard.table.is_empty() {
            return k;
        }
        let mut at = mixed as usize & shard.mask;
        loop {
            let entry = shard.table[at];
            if entry.key == k {
                return entry.value;
            }
            if entry.key == 0 {
                return k;
            }
            at = (at + 1) & shard.mask;
        }
    }
}

#[inline(always)]
fn shard_of(mixed: u64, mask: usize, shift: u32) -> usize {
    (mixed >> shift) as usize & mask
}

fn bins<T>(n: usize, capacity: usize) -> Vec<Vec<T>> {
    (0..n).map(|_| Vec::with_capacity(capacity)).collect()
}

fn make_shard(inputs: Vec<Vec<u64>>) -> BuiltShard {
    let occurrences = inputs.iter().map(Vec::len).sum::<usize>();
    if occurrences == 0 {
        return BuiltShard {
            shard: Shard { table: Box::new([]), mask: 0, base: 0 },
            keys: Vec::new(),
        };
    }

    // Unique endpoints cannot outnumber occurrences. Keep at least 25% of
    // the table empty even when every endpoint is unique, so failed queries
    // always terminate and successful probes remain short.
    let needed = occurrences
        .checked_mul(4)
        .and_then(|n| n.checked_add(2))
        .map(|n| n / 3)
        .expect("too many link endpoints");
    let capacity = needed
        .max(2)
        .checked_next_power_of_two()
        .expect("link endpoint table is too large");
    let mut table = vec![Entry::default(); capacity].into_boxed_slice();
    let mask = capacity - 1;
    let mut keys = Vec::with_capacity(occurrences.min(capacity * 3 / 4));

    for input in inputs {
        for key in input {
            debug_assert_ne!(key, 0);
            let mut at = hash(key) as usize & mask;
            loop {
                let entry = table[at];
                if entry.key == key {
                    break;
                }
                if entry.key == 0 {
                    let local = u32::try_from(keys.len()).expect("too many aliases in one shard");
                    table[at] = Entry { key, value: u64::from(local) };
                    keys.push(key);
                    break;
                }
                at = (at + 1) & mask;
            }
        }
    }

    BuiltShard {
        shard: Shard { table, mask, base: 0 },
        keys,
    }
}

#[inline(always)]
fn index_of(shards: &[Shard], shard_mask: usize, shard_shift: u32, key: u64) -> u32 {
    let mixed = hash(key);
    let shard = &shards[shard_of(mixed, shard_mask, shard_shift)];
    let local = shard.local_index(key, mixed) as usize;
    u32::try_from(shard.base + local).expect("too many linked aliases")
}

/// Follow parents with opportunistic path splitting. Parent priorities only
/// decrease, so a relaxed load can be stale but can never point back upward.
#[inline(always)]
fn atomic_root(parent: &[AtomicU32], mut node: u32) -> u32 {
    loop {
        let p = parent[node as usize].load(AtomicOrdering::Relaxed);
        if p == node {
            return node;
        }
        let grand = parent[p as usize].load(AtomicOrdering::Relaxed);
        if grand != p {
            let _ = parent[node as usize].compare_exchange_weak(
                p,
                grand,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            );
        }
        node = p;
    }
}

#[inline(always)]
fn priority_before(priority: &[u64], a: u32, b: u32) -> bool {
    priority[a as usize] < priority[b as usize]
        || (priority[a as usize] == priority[b as usize] && a < b)
}

/// Lock-free union using one immutable total priority per vertex. Every
/// successful parent CAS points to a strictly lower priority. Consequently
/// concurrent unions cannot form a cycle, even if the chosen winner ceases to
/// be a root before the CAS. Uniform mixed priorities give logarithmic trees
/// without a separately contended rank/size word.
#[inline(always)]
fn atomic_union(parent: &[AtomicU32], priority: &[u64], mut a: u32, mut b: u32) {
    loop {
        a = atomic_root(parent, a);
        b = atomic_root(parent, b);
        if a == b {
            return;
        }
        let (winner, loser) = if priority_before(priority, a, b) { (a, b) } else { (b, a) };
        if parent[loser as usize]
            .compare_exchange_weak(
                loser,
                winner,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            )
            .is_ok()
        {
            return;
        }
    }
}

#[inline(always)]
fn update_minimum(best: &[AtomicU32], root: u32, candidate: u32, keys: &[u64], slow: &SlowIntern) {
    let cell = &best[root as usize];
    let mut old = cell.load(AtomicOrdering::Relaxed);
    loop {
        if old != u32::MAX
            && cmp_keys(keys[candidate as usize], keys[old as usize], slow) != Ordering::Less
        {
            return;
        }
        match cell.compare_exchange_weak(
            old,
            candidate,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => return,
            Err(now) => old = now,
        }
    }
}

/// Build an exact resolver from link endpoints only. Work is sharded by the
/// high bits of the uniformly mixed key: endpoint interning and final table
/// construction have one owner per shard, while connectivity is resolved in
/// parallel over the original edge parts.
pub(super) fn build(
    parts: Vec<Vec<(u64, u64)>>,
    slow: &SlowIntern,
    threads: usize,
) -> Aliases {
    let edge_count = parts.iter().map(Vec::len).sum::<usize>();
    if edge_count == 0 {
        return Aliases {
            shards: vec![Shard { table: Box::new([]), mask: 0, base: 0 }].into_boxed_slice(),
            shard_mask: 0,
            shard_shift: 0,
        };
    }

    let workers = threads.max(1).min(parts.len().max(1));
    let endpoint_count = edge_count.checked_mul(2).expect("too many link endpoints");
    let useful_by_size = endpoint_count
        .div_ceil(1 << 16)
        .max(1)
        .checked_next_power_of_two()
        .unwrap_or(256)
        .min(256);
    let desired = workers
        .checked_mul(4)
        .and_then(usize::checked_next_power_of_two)
        .unwrap_or(256)
        .min(256);
    let shard_count = desired.min(useful_by_size).max(1);
    let shard_mask = shard_count - 1;
    let shard_bits = shard_count.trailing_zeros();
    let shard_shift = if shard_bits == 0 { 0 } else { 64 - shard_bits };

    // First partition endpoint occurrences. Every worker writes only its own
    // vectors; no endpoint insertion takes a lock.
    let average = endpoint_count.div_ceil(workers * shard_count);
    let distributed = thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let parts = &parts;
                scope.spawn(move || {
                    let mut out = bins(shard_count, average);
                    for part in parts.iter().skip(worker).step_by(workers) {
                        for &(a, b) in part {
                            debug_assert!(a != 0 && b != 0);
                            let ah = hash(a);
                            let bh = hash(b);
                            out[shard_of(ah, shard_mask, shard_shift)].push(a);
                            out[shard_of(bh, shard_mask, shard_shift)].push(b);
                        }
                    }
                    out
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect::<Vec<_>>()
    });

    let mut shard_inputs: Vec<Vec<Vec<u64>>> = (0..shard_count).map(|_| Vec::new()).collect();
    for local in distributed {
        for (shard, input) in local.into_iter().enumerate() {
            if !input.is_empty() {
                shard_inputs[shard].push(input);
            }
        }
    }

    // Shards vary slightly in size. A tiny job queue keeps their zero-fill and
    // interning work balanced; the mutex is acquired once per whole shard.
    let build_workers = workers.min(shard_count);
    let queue = Mutex::new(shard_inputs.into_iter().enumerate());
    let built_parts = thread::scope(|scope| {
        let handles: Vec<_> = (0..build_workers)
            .map(|_| {
                let queue = &queue;
                scope.spawn(move || {
                    let mut mine = Vec::new();
                    loop {
                        let job = queue.lock().unwrap().next();
                        let Some((index, input)) = job else { break };
                        mine.push((index, make_shard(input)));
                    }
                    mine
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect::<Vec<_>>()
    });

    let mut ordered: Vec<Option<BuiltShard>> = (0..shard_count).map(|_| None).collect();
    for worker in built_parts {
        for (index, shard) in worker {
            ordered[index] = Some(shard);
        }
    }

    let unique_count = ordered
        .iter()
        .map(|s| s.as_ref().unwrap().keys.len())
        .sum::<usize>();
    assert!(unique_count < u32::MAX as usize, "too many linked aliases");
    let mut keys = Vec::with_capacity(unique_count);
    let mut shards = Vec::with_capacity(shard_count);
    for item in ordered {
        let mut built = item.unwrap();
        built.shard.base = keys.len();
        keys.append(&mut built.keys);
        shards.push(built.shard);
    }

    let parent: Vec<AtomicU32> = (0..unique_count as u32).map(AtomicU32::new).collect();
    let priority: Vec<u64> = keys.iter().map(|&key| hash(key)).collect();

    // Atomic parent changes publish no associated mutable data, and all other
    // arrays are immutable in this phase, so relaxed atomic ordering suffices.
    thread::scope(|scope| {
        for worker in 0..workers {
            let parts = &parts;
            let shards = &shards;
            let parent = &parent;
            let priority = &priority;
            scope.spawn(move || {
                for part in parts.iter().skip(worker).step_by(workers) {
                    for &(a, b) in part {
                        let ia = index_of(shards, shard_mask, shard_shift, a);
                        let ib = index_of(shards, shard_mask, shard_shift, b);
                        atomic_union(parent, priority, ia, ib);
                    }
                }
            });
        }
    });
    drop(parts);
    drop(priority);

    // Resolve every vertex and choose each component's exact bytewise minimum
    // in parallel. CAS candidates monotonically improve under cmp_keys.
    let best: Vec<AtomicU32> = (0..unique_count)
        .map(|_| AtomicU32::new(u32::MAX))
        .collect();
    let mut roots = vec![0u32; unique_count];
    let chunk = unique_count.div_ceil(workers);
    thread::scope(|scope| {
        for (chunk_index, output) in roots.chunks_mut(chunk).enumerate() {
            let start = chunk_index * chunk;
            let parent = &parent;
            let best = &best;
            let keys = &keys;
            scope.spawn(move || {
                for (offset, root_out) in output.iter_mut().enumerate() {
                    let node = (start + offset) as u32;
                    let root = atomic_root(parent, node);
                    *root_out = root;
                    update_minimum(best, root, node, keys, slow);
                }
            });
        }
    });
    drop(parent);

    // Convert local dense IDs in-place to final component minima. This reuses
    // the expensive endpoint hash tables instead of building a second index.
    let finish_workers = workers.min(shards.len());
    let width = shards.len().div_ceil(finish_workers);
    thread::scope(|scope| {
        for slice in shards.chunks_mut(width) {
            let roots = &roots;
            let best = &best;
            let keys = &keys;
            scope.spawn(move || {
                for shard in slice {
                    for entry in &mut shard.table {
                        if entry.key == 0 {
                            continue;
                        }
                        let node = shard.base + entry.value as usize;
                        let root = roots[node] as usize;
                        let minimum = best[root].load(AtomicOrdering::Relaxed);
                        debug_assert_ne!(minimum, u32::MAX);
                        entry.value = keys[minimum as usize];
                    }
                    shard.base = 0;
                }
            });
        }
    });

    Aliases {
        shards: shards.into_boxed_slice(),
        shard_mask,
        shard_shift,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atom::try_pack;

    fn key(text: &[u8]) -> u64 {
        try_pack(text).unwrap()
    }

    #[test]
    fn transitive_components_include_link_only_aliases() {
        let slow = SlowIntern::new();
        let (a, b, c, d) = (key(b"u9"), key(b"u5"), key(b"u1"), key(b"u0"));
        let aliases = build(vec![vec![(a, b)], vec![(c, d), (b, c)]], &slow, 4);
        for linked in [a, b, c, d] {
            assert_eq!(aliases.resolve(linked), d);
        }
    }

    #[test]
    fn case_is_exact_and_bytewise_minimum_wins() {
        let slow = SlowIntern::new();
        let upper = key(b"Uzz");
        let lower = key(b"u0");
        let other = key(b"u1");
        let aliases = build(vec![vec![(lower, other), (other, upper)]], &slow, 3);
        assert_eq!(aliases.resolve(lower), upper);
        assert_eq!(aliases.resolve(other), upper);
        assert_eq!(aliases.resolve(upper), upper);
    }

    #[test]
    fn absent_and_self_linked_ids_are_isolated() {
        let slow = SlowIntern::new();
        let cited = key(b"u2");
        let absent = key(b"u3");
        let aliases = build(vec![vec![(cited, cited)]], &slow, 8);
        assert_eq!(aliases.resolve(cited), cited);
        assert_eq!(aliases.resolve(absent), absent);

        let empty = build(Vec::new(), &slow, 0);
        assert_eq!(empty.resolve(absent), absent);
    }
}
