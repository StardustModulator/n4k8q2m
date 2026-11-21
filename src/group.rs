//! Per-person grouping of a person bin and its hand-off to sessionization.
//!
//! CONTRACT (frozen between modules): `stream` delivers, for every person
//! bin, worker parts of `Histories`. `Event.key`/`Click.key` is the person's
//! name key (bytewise-smallest linked id, packed or interned); every event and
//! click of a person lands in the same bin. `Event.val` is
//! `ts_offset << 32 | category << 24 | price` (price 0 for view/cart).
use super::*;
use crate::aggregate::{Acc, Hit};
use crate::stream::hash;

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

