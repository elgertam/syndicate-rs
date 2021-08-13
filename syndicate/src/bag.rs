use std::collections::BTreeMap;
use std::collections::btree_map::{Iter, Keys, Entry};
use std::iter::{FromIterator, IntoIterator};

/// Element counts in [`BTreeBag`]s are 32-bit signed integers.
pub type Count = i32;

/// Represents the "net change" to the count of a given `key` after a
/// change-in-count has been applied via [`BTreeBag::change`] and
/// friends.
#[derive(Debug, PartialEq, Eq)]
pub enum Net {
    /// The key previously had non-zero count, now has zero count.
    PresentToAbsent,
    /// The key's count stayed the same, at zero.
    AbsentToAbsent,
    /// The key previously had zero count, now has non-zero count.
    AbsentToPresent,
    /// The key's count was previously, and is now also, non-zero.
    PresentToPresent,
}

/// A bag datastructure (mapping from values `V` to [`Count`]s
/// internally).
///
/// A bag may have a *negative* count against a particular element.
/// Such a bag is called a "delta", and can be used to represent a
/// *change* against a bag containing only positive counts.
///
/// Every value of type `V` notionally has a zero count associated
/// with it at the start of the bag's lifetime; so a bag is a *total*
/// mapping from elements of `V` to counts.
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BTreeBag<V: std::cmp::Ord> {
    counts: BTreeMap<V, Count>,
    total: isize,
}

impl<V: std::cmp::Ord> std::default::Default for BTreeBag<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: std::cmp::Ord> BTreeBag<V> {
    /// Construct a new, empty bag.
    pub fn new() -> BTreeBag<V> {
        BTreeBag { counts: BTreeMap::new(), total: 0 }
    }

    /// Apply a change-in-count (`delta`) against `key`, allowing the
    /// resulting count to be negative if needed.
    pub fn change(&mut self, key: V, delta: Count) -> Net {
        self._change(key, delta, false)
    }

    /// Apply a change-in-count (`delta`) against `key`, preventing
    /// the resulting count from dropping below zero (by clamping it
    /// at zero in that case).
    pub fn change_clamped(&mut self, key: V, delta: Count) -> Net {
        self._change(key, delta, true)
    }

    /// Apply a change-in-count (`delta`) against `key`, clamping the
    /// resulting count at zero iff `clamp` is `true`.
    pub fn _change(&mut self, key: V, delta: Count, clamp: bool) -> Net {
        let old_count = self[&key];
        let mut new_count = old_count + delta;
        if clamp { new_count = new_count.max(0) }
        self.total = self.total + (new_count - old_count) as isize;
        if new_count == 0 {
            self.counts.remove(&key);
            if old_count == 0 { Net::AbsentToAbsent } else { Net::PresentToAbsent }
        } else {
            self.counts.insert(key, new_count);
            if old_count == 0 { Net::AbsentToPresent } else { Net::PresentToPresent }
        }
    }

    /// Removes all elements from the bag, leaving it empty with a zero total count.
    pub fn clear(&mut self) {
        self.counts.clear();
        self.total = 0;
    }

    /// `true` iff `key` has a non-zero count associated with it.
    ///
    /// Note that `true` will be returned even when the count is *negative*.
    ///
    pub fn contains_key(&self, key: &V) -> bool {
        self.counts.contains_key(key)
    }

    /// `true` iff no element of the bag has a non-zero count.
    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    /// Yields the number of elements having a non-zero count.
    pub fn len(&self) -> usize {
        self.counts.len()
    }

    /// Answers the sum of all counts in the bag.
    ///
    /// For bags with no negative counts, this is the same as the
    /// number of "distinct copies" of elements in the bag.
    pub fn total(&self) -> isize {
        self.total
    }

    /// Iterates over elements in the bag with non-zero counts.
    pub fn keys(&self) -> Keys<V, Count> {
        self.counts.keys()
    }

    /// Retrieves an [`Entry`] for `key`.
    ///
    /// Note that the `Entry` will be "absent" when `key` has a zero
    /// associated count.
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
