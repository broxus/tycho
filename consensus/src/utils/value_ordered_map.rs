use std::cmp;
use std::hash::Hash;

use indexmap::IndexMap;

/// A map ordered by `(value, key)` pair in DESC order.
///
/// The map retains only the greatest `max_len` entries.
/// Discarded entries are forgotten, so inserting the same key later is treated as a new insertion.
/// [`Self::from_iter`] fixes `max_len` to the number of collected unique keys.
pub struct ValueOrderedMap<K, V> {
    /// Internally values have ASC order to support natural order of operations
    inner: IndexMap<K, V, ahash::RandomState>,
    max_len: usize,
}

#[derive(Eq, PartialEq, Debug)]
pub enum UpsertResult<K, V> {
    /// KV inserted without eviction
    OkInserted,
    /// key is found and its old value is replaced
    OkUpdated(V),
    /// map is at `max_len` and insert pushed some tuple out of map
    OkEvicted(K, V),
    /// key is found, but value didn't satisfy `update_if` clause
    ErrRejected,
    /// map is at `max_len` and all retained `(value, key)` are greater than passed one
    ErrTooLow,
}

impl<K, V> ValueOrderedMap<K, V>
where
    K: Hash + Eq + Ord,
    V: Ord,
{
    pub fn with_max_len(max_len: usize) -> Self {
        Self {
            inner: IndexMap::with_capacity_and_hasher(max_len, ahash::RandomState::new()),
            max_len,
        }
    }

    /// Updates contained value if a function of `(new, old)` evaluates to `true`.
    pub fn upsert<F>(&mut self, k: K, v: V, update_if: F) -> UpsertResult<K, V>
    where
        F: FnOnce(&V, &V) -> bool,
    {
        let was_full = self.inner.len() >= self.max_len;
        let pos = (self.inner)
            .binary_search_by(|sk, sv| Self::compare(sk, sv, &k, &v))
            .unwrap_or_else(|to_be| to_be);

        match self.inner.entry(k) {
            indexmap::map::Entry::Occupied(mut occupied) => {
                if !update_if(&v, occupied.get()) {
                    return UpsertResult::ErrRejected;
                }

                let prev_v = occupied.insert(v);
                let target = if occupied.index() < pos { pos - 1 } else { pos };
                occupied.move_index(target);

                UpsertResult::OkUpdated(prev_v)
            }
            indexmap::map::Entry::Vacant(vacant) => {
                if was_full && pos == 0 {
                    return UpsertResult::ErrTooLow;
                }

                if was_full {
                    // Replace the minimum, then rotate only the prefix preceding the candidate.
                    let (old_k, mut occupied) = vacant.replace_index(0);
                    let old_v = occupied.insert(v);
                    occupied.move_index(pos - 1);
                    UpsertResult::OkEvicted(old_k, old_v)
                } else {
                    vacant.shift_insert(pos, v);
                    UpsertResult::OkInserted
                }
            }
        }
    }

    pub fn get(&self, k: &K) -> Option<&V> {
        self.inner.get(k)
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.inner.shift_remove(k)
    }

    pub fn max(&self) -> Option<(&K, &V)> {
        self.inner.last()
    }

    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter().rev()
    }

    fn compare(k1: &K, v1: &V, k2: &K, v2: &V) -> cmp::Ordering {
        (v1, k1).cmp(&(v2, k2))
    }
}

impl<K, V> FromIterator<(K, V)> for ValueOrderedMap<K, V>
where
    K: Hash + Eq + Ord,
    V: Ord,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iterable: I) -> Self {
        let mut inner = IndexMap::from_iter(iterable);
        inner.sort_unstable_by(Self::compare);
        let max_len = inner.len();
        Self { inner, max_len }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::utils::value_ordered_map::UpsertResult::*;

    impl<K, V> ValueOrderedMap<K, V>
    where
        K: Hash + Eq + Ord + Copy,
        V: Ord + Copy,
    {
        /// Hides internal ASC representation, to test DESC external behavior
        fn vec(&self) -> Vec<(K, V)> {
            self.iter().map(|(k, v)| (*k, *v)).collect()
        }
    }

    #[test]
    fn priority_map() {
        // init

        let mut vom = ValueOrderedMap::with_max_len(4);

        assert_eq!(vom.upsert('b', 0, PartialOrd::ge), OkInserted);

        assert_eq!(vom.upsert('a', 0, PartialOrd::ge), OkInserted);

        assert_eq!(vom.upsert('c', 2, PartialOrd::ge), OkInserted);

        assert_eq!(vom.upsert('d', 1, PartialOrd::ge), OkInserted);

        assert_eq!(vom.vec(), vec![('c', 2), ('d', 1), ('b', 0), ('a', 0)]);

        assert_eq!(vom.max(), Some((&'c', &2)));

        // ordered inserts

        assert_eq!(vom.upsert('a', 2, PartialOrd::ge), OkUpdated(0));

        assert_eq!(vom.vec(), vec![('c', 2), ('a', 2), ('d', 1), ('b', 0)]);

        assert_eq!(vom.upsert('a', 3, PartialOrd::ge), OkUpdated(2));

        assert_eq!(vom.vec(), vec![('a', 3), ('c', 2), ('d', 1), ('b', 0)]);

        assert_eq!(vom.upsert('a', 1, PartialOrd::ge), ErrRejected);

        assert_eq!(vom.vec(), vec![('a', 3), ('c', 2), ('d', 1), ('b', 0)]);

        assert_eq!(vom.upsert('a', 1, PartialOrd::le), OkUpdated(3));

        assert_eq!(vom.vec(), vec![('c', 2), ('d', 1), ('a', 1), ('b', 0)]);

        assert_eq!(vom.upsert('a', 0, PartialOrd::le), OkUpdated(1));

        assert_eq!(vom.vec(), vec![('c', 2), ('d', 1), ('b', 0), ('a', 0)]);

        assert_eq!(vom.remove(&'c'), Some(2));
        assert_eq!(vom.max(), Some((&'d', &1)));
        assert_eq!(vom.remove(&'c'), None);
    }

    #[test]
    fn from_iter_uses_last_value_and_bounds_to_unique_keys() {
        let mut map = ValueOrderedMap::from_iter([(3, 1), (1, 2), (2, 0), (1, 0), (0, 0)]);

        assert_eq!(map.vec(), [(3, 1), (2, 0), (1, 0), (0, 0)]);

        assert_eq!(map.upsert(4, -1, PartialEq::ne), ErrTooLow);
    }

    #[test]
    fn bounded_map_retains_greatest_value_key_pairs() {
        let mut map = ValueOrderedMap::with_max_len(3);

        assert_eq!(map.upsert('a', 1, PartialEq::ne), OkInserted);
        assert_eq!(map.upsert('b', 2, PartialEq::ne), OkInserted);
        assert_eq!(map.upsert('c', 3, PartialEq::ne), OkInserted);
        assert_eq!(map.upsert('d', 0, PartialEq::ne), ErrTooLow);
        assert_eq!(map.vec(), [('c', 3), ('b', 2), ('a', 1)]);

        assert_eq!(map.upsert('e', 1, PartialEq::ne), OkEvicted('a', 1));
        assert_eq!(map.vec(), [('c', 3), ('b', 2), ('e', 1)]);

        assert_eq!(map.upsert('d', 2, PartialEq::ne), OkEvicted('e', 1));
        assert_eq!(map.vec(), [('c', 3), ('d', 2), ('b', 2)]);
        assert_eq!(map.get(&'a'), None);

        assert_eq!(map.upsert('a', 2, PartialEq::ne), ErrTooLow);
        assert_eq!(map.vec(), [('c', 3), ('d', 2), ('b', 2)]);

        assert_eq!(map.upsert('e', 4, PartialEq::ne), OkEvicted('b', 2));
        assert_eq!(map.vec(), [('e', 4), ('c', 3), ('d', 2)]);
    }

    #[test]
    fn zero_bound_rejects_every_entry() {
        let mut map = ValueOrderedMap::with_max_len(0);

        assert_eq!(map.upsert('a', 1, PartialEq::ne), ErrTooLow);
        assert_eq!(map.max(), None);
        assert_eq!(map.iter().next(), None);
    }
}
