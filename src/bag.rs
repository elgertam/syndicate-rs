use std::collections::BTreeMap;
use std::collections::btree_map::{Iter, Keys, Entry};
use std::iter::{FromIterator, IntoIterator};

pub type Count = i32;

#[derive(Debug, PartialEq, Eq)]
pub enum Net {
    PresentToAbsent,
    AbsentToAbsent,
    AbsentToPresent,
    PresentToPresent,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
// Allows negative counts - a "delta"
pub struct BTreeBag<V: std::cmp::Ord> {
    counts: BTreeMap<V, Count>,
}

impl<V: std::cmp::Ord> std::default::Default for BTreeBag<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: std::cmp::Ord> BTreeBag<V> {
    pub fn new() -> BTreeBag<V> {
        BTreeBag { counts: BTreeMap::new() }
    }

    pub fn change(&mut self, key: V, delta: Count) -> Net { self._change(key, delta, false) }
    pub fn change_clamped(&mut self, key: V, delta: Count) -> Net { self._change(key, delta, true) }

    pub fn _change(&mut self, key: V, delta: Count, clamp: bool) -> Net {
        let old_count = self[&key];
        let mut new_count = old_count + delta;
        if clamp { new_count = new_count.max(0) }
        if new_count == 0 {
            self.counts.remove(&key);
            if old_count == 0 { Net::AbsentToAbsent } else { Net::PresentToAbsent }
        } else {
            self.counts.insert(key, new_count);
            if old_count == 0 { Net::AbsentToPresent } else { Net::PresentToPresent }
        }
    }

    pub fn clear(&mut self) {
        self.counts.clear();
    }

    pub fn contains_key(&self, key: &V) -> bool {
        self.counts.contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    pub fn len(&self) -> usize {
        self.counts.len()
    }

    pub fn keys(&self) -> Keys<V, Count> {
        self.counts.keys()
    }

    pub fn entry(&mut self, key: V) -> Entry<V, Count> {
        self.counts.entry(key)
    }
}

impl<'a, V: std::cmp::Ord> IntoIterator for &'a BTreeBag<V> {
    type Item = (&'a V, &'a Count);
    type IntoIter = Iter<'a, V, Count>;

    fn into_iter(self) -> Self::IntoIter {
        self.counts.iter()
    }
}

impl<V: std::cmp::Ord> FromIterator<V> for BTreeBag<V> {
    fn from_iter<I: IntoIterator<Item=V>>(iter: I) -> Self {
        let mut bag = Self::new();
        for k in iter {
            bag.change(k, 1);
        }
        bag
    }
}

impl<V: std::cmp::Ord> std::ops::Index<&V> for BTreeBag<V> {
    type Output = Count;
    fn index(&self, i: &V) -> &Count {
        self.counts.get(i).unwrap_or(&0)
    }
}
