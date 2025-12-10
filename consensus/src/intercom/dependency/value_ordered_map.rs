use std::cmp;
use std::hash::Hash;

use indexmap::IndexMap;

/// A map ordered by (value, key) pair in ASC order.
/// Upsert takes O(n + log n) time.
/// Greatest values may be popped from the end in O(1) time.
#[derive(Default)]
pub struct ValueOrderedMap<K, V>(IndexMap<K, V, ahash::RandomState>);

impl<K, V> ValueOrderedMap<K, V>
where
    K: Hash + Eq + Ord,
    V: Ord,
{
    /// Updates contained value if a function of `(new, old)` evaluates to `true`.
    /// Then method:
    /// * returns passed value back as an `Err` if replace failed because of maintained priority
    /// * returns `Ok` if passed value was applied, with maybe old replaced value if it exists
    pub fn upsert<F>(&mut self, k: K, v: V, update_if: F) -> Result<Option<V>, V>
    where
        F: FnOnce(&V, &V) -> bool,
    {
        let pos = (self.0)
            .binary_search_by(|sk, sv| Self::compare(sk, sv, &k, &v))
            .unwrap_or_else(|to_be| to_be);
        match self.0.entry(k) {
            indexmap::map::Entry::Occupied(mut occupied) => {
                if update_if(&v, occupied.get()) {
                    let prev = occupied.insert(v);
                    match occupied.index().cmp(&pos) {
                        cmp::Ordering::Equal => {}
                        cmp::Ordering::Greater => occupied.move_index(pos),
                        // `move_index` accepts target as if the entry has already left its position
                        // `strict_sub` cannot panic because current entry is at least at 0 index
                        cmp::Ordering::Less => {
                            let pos = pos.checked_sub(1).expect("current pos is > 0");
                            occupied.move_index(pos);
                        }
                    }
                    Ok(Some(prev))
                } else {
                    Err(v)
                }
            }
            indexmap::map::Entry::Vacant(vacant) => {
                vacant.shift_insert(pos, v);
                Ok(None)
            }
        }
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.0.shift_remove(k)
    }

    pub fn max(&self) -> Option<(&K, &V)> {
        self.0.last()
    }

    pub fn inner(&self) -> &IndexMap<K, V, ahash::RandomState> {
        &self.0
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
        Self(inner)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    impl<K, V> ValueOrderedMap<K, V>
    where
        K: Hash + Eq + Ord + Copy,
        V: Ord + Copy,
    {
        fn vec(&self) -> Vec<(K, V)> {
            self.0.iter().map(|(k, v)| (*k, *v)).collect()
        }
    }

    #[test]
    fn priority_map() {
        // init

        let mut vom = ValueOrderedMap::default();

        assert!(vom.upsert('b', 0, PartialOrd::ge).is_ok());

        assert!(vom.upsert('a', 0, PartialOrd::ge).is_ok());

        assert!(vom.upsert('c', 2, PartialOrd::ge).is_ok());

        assert!(vom.upsert('d', 1, PartialOrd::ge).is_ok());

        assert_eq!(vom.vec(), vec![('a', 0), ('b', 0), ('d', 1), ('c', 2)]);

        assert_eq!(vom.max(), Some((&'c', &2)));

        // ordered inserts

        assert!(vom.upsert('a', 2, PartialOrd::ge).is_ok());

        assert_eq!(vom.vec(), vec![('b', 0), ('d', 1), ('a', 2), ('c', 2)]);

        assert!(vom.upsert('a', 3, PartialOrd::ge).is_ok());

        assert_eq!(vom.vec(), vec![('b', 0), ('d', 1), ('c', 2), ('a', 3)]);

        assert!(vom.upsert('a', 1, PartialOrd::ge).is_err());

        assert_eq!(vom.vec(), vec![('b', 0), ('d', 1), ('c', 2), ('a', 3)]);

        assert!(vom.upsert('a', 1, PartialOrd::le).is_ok());

        assert_eq!(vom.vec(), vec![('b', 0), ('a', 1), ('d', 1), ('c', 2)]);

        assert!(vom.upsert('a', 0, PartialOrd::le).is_ok());

        assert_eq!(vom.vec(), vec![('a', 0), ('b', 0), ('d', 1), ('c', 2)]);
    }
}
