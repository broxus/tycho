use std::collections::BTreeMap;
use std::hash::Hash;

use crate::transactional::Transactional;
use crate::{FastHashMap, FastHashSet};

pub struct TransactionalBTreeMap<K, V> {
    inner: BTreeMap<K, V>,
    tx: Option<MapTx<K, V>>,
}

struct MapTx<K, V> {
    added: FastHashSet<K>,
    removed: FastHashMap<K, V>,
}

impl<K: Ord + Hash + Eq + Clone, V: Transactional> TransactionalBTreeMap<K, V> {
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

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        let to_remove: Vec<K> = self
            .inner
            .iter_mut()
            .filter_map(|(k, v)| (!f(k, v)).then(|| k.clone()))
            .collect();

        for k in to_remove {
            let _ = self.remove(&k);
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.inner.get_mut(key)
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

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + Clone {
        self.inner.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.inner.iter_mut()
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.values()
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.inner.values_mut()
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.inner.keys()
    }

    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        self.inner.last_key_value()
    }

    pub fn inner_mut(&mut self) -> &mut BTreeMap<K, V> {
        &mut self.inner
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

impl<K: Ord + Hash + Eq + Clone, V: Transactional> Transactional for TransactionalBTreeMap<K, V> {
    fn begin(&mut self) {
        debug_assert!(self.tx.is_none());
        for v in self.inner.values_mut() {
            v.begin();
        }
        self.tx = Some(MapTx {
            added: FastHashSet::default(),
            removed: FastHashMap::default(),
        });
    }

    fn commit(&mut self) {
        self.tx = None;
        for v in self.inner.values_mut() {
            if v.in_tx() {
                v.commit();
            }
        }
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
        for v in self.inner.values_mut() {
            if v.in_tx() {
                v.rollback();
            }
        }
    }

    fn in_tx(&self) -> bool {
        self.tx.is_some()
    }
}
