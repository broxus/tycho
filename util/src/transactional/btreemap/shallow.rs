use std::collections::{BTreeMap, BTreeSet};

use crate::transactional::Transactional;

pub struct TransactionalBTreeMap<K, V> {
    inner: BTreeMap<K, V>,
    tx: Option<MapTx<K, V>>,
}

struct MapTx<K, V> {
    added: BTreeSet<K>,
    removed: BTreeMap<K, V>,
}

impl<K: Ord + Clone, V> TransactionalBTreeMap<K, V> {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            tx: None,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> bool {
        let old = self.inner.insert(key.clone(), value);
        if let Some(tx) = &mut self.tx {
            match old {
                Some(old_value) => {
                    if !tx.added.contains(&key) && !tx.removed.contains_key(&key) {
                        tx.removed.insert(key, old_value);
                    }
                    true
                }
                None => {
                    if !tx.removed.contains_key(&key) {
                        tx.added.insert(key);
                    }
                    false
                }
            }
        } else {
            old.is_some()
        }
    }

    pub fn remove(&mut self, key: &K) -> bool {
        let Some(value) = self.inner.remove(key) else {
            return false;
        };
        if let Some(tx) = &mut self.tx {
            if tx.added.remove(key) {
                return true;
            }
            if !tx.removed.contains_key(key) {
                tx.removed.insert(key.clone(), value);
            }
        }
        true
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter()
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.values()
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.inner.keys()
    }
}

impl<K: Ord + Clone, V: Clone> TransactionalBTreeMap<K, V> {
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if let Some(tx) = &mut self.tx
            && let Some(existing) = self.inner.get(key)
            && !tx.added.contains(key)
            && !tx.removed.contains_key(key)
        {
            tx.removed.insert(key.clone(), existing.clone());
        }
        self.inner.get_mut(key)
    }
}

impl<K: Ord, V> Default for TransactionalBTreeMap<K, V> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::default(),
            tx: None,
        }
    }
}

impl<K: Ord, V> From<BTreeMap<K, V>> for TransactionalBTreeMap<K, V> {
    fn from(inner: BTreeMap<K, V>) -> Self {
        Self { inner, tx: None }
    }
}

impl<K: Ord + Clone, V> Transactional for TransactionalBTreeMap<K, V> {
    fn begin(&mut self) {
        debug_assert!(self.tx.is_none());
        self.tx = Some(MapTx {
            added: BTreeSet::new(),
            removed: BTreeMap::new(),
        });
    }

    fn commit(&mut self) {
        self.tx = None;
    }

    fn rollback(&mut self) {
        if let Some(tx) = self.tx.take() {
            for key in tx.added {
                self.inner.remove(&key);
            }
            for (key, value) in tx.removed {
                self.inner.insert(key, value);
            }
        }
    }

    fn in_tx(&self) -> bool {
        self.tx.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_map_is_empty() {
        let map: TransactionalBTreeMap<String, i32> = TransactionalBTreeMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn insert_and_get() {
        let mut map: TransactionalBTreeMap<&str, i32> = TransactionalBTreeMap::new();
        assert!(!map.insert("a", 1));
        assert_eq!(map.get(&"a"), Some(&1));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn rollback_undoes_insert() {
        let mut map: TransactionalBTreeMap<&str, i32> = TransactionalBTreeMap::new();
        map.begin();
        map.insert("a", 1);
        map.rollback();
        assert!(!map.contains_key(&"a"));
    }

    #[test]
    fn rollback_undoes_remove() {
        let mut map: TransactionalBTreeMap<&str, i32> = TransactionalBTreeMap::new();
        map.insert("a", 1);
        map.begin();
        map.remove(&"a");
        map.rollback();
        assert_eq!(map.get(&"a"), Some(&1));
    }

    #[test]
    fn get_mut_rolled_back() {
        let mut map: TransactionalBTreeMap<&str, i32> = TransactionalBTreeMap::new();
        map.insert("a", 1);
        map.begin();
        *map.get_mut(&"a").unwrap() = 100;
        map.rollback();
        assert_eq!(map.get(&"a"), Some(&1));
    }

    #[test]
    fn overwrite_rolled_back() {
        let mut map: TransactionalBTreeMap<&str, i32> = TransactionalBTreeMap::new();
        map.insert("a", 1);
        map.begin();
        map.insert("a", 2);
        map.rollback();
        assert_eq!(map.get(&"a"), Some(&1));
    }
}
