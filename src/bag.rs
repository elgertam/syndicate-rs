use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Iter;
use std::collections::hash_map::Keys;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::hash::Hash;

type Count = i32;

pub enum Net {
    PresentToAbsent,
    AbsentToAbsent,
    AbsentToPresent,
    PresentToPresent,
}

// Allows negative counts - a "delta"
pub struct HashBag<V, S = RandomState> {
    counts: HashMap<V, Count, S>,
}

impl<V,S> HashBag<V,S>
where V: Eq + Hash, S: BuildHasher + Default
{
    pub fn new() -> HashBag<V,S> {
        HashBag {
            counts: HashMap::with_hasher(Default::default()),
        }
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

    pub fn iter(&self) -> Iter<V, Count> {
        self.counts.iter()
    }
}

impl<V,S> std::convert::From<HashSet<V, S>> for HashBag<V,S>
where V: Eq + Hash + Clone, S: BuildHasher + Default
{
    fn from(xs: HashSet<V,S>) -> Self {
        let mut cs = HashMap::with_hasher(Default::default());
        for k in xs.iter() {
            cs.insert(k.clone(), 1);
        }
        HashBag {
            counts: cs
        }
    }
}

impl<V,S> std::ops::Index<&V> for HashBag<V,S>
where V: Eq + Hash, S: BuildHasher
{
    type Output = Count;
    fn index(&self, i: &V) -> &Count {
        self.counts.get(i).unwrap_or(&0)
    }
}
