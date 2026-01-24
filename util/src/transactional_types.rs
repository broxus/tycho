use std::collections::BTreeMap;
use std::hash::Hash;

use crate::{FastHashMap, FastHashSet};
pub trait Transactional {
    fn begin(&mut self);
    fn commit(&mut self);
    fn rollback(&mut self);
    fn is_in_transaction(&self) -> bool;
}

macro_rules! impl_transactional_noop {
    ($($t:ty),*) => {
        $(impl Transactional for $t {
            fn begin(&mut self) {}
            fn commit(&mut self) {}
            fn rollback(&mut self) {}
            fn is_in_transaction(&self) -> bool { false }
        })*
    };
}

impl_transactional_noop!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64, bool, char, String
);

impl<T: Transactional> Transactional for Option<T> {
    fn begin(&mut self) {
        if let Some(inner) = self {
            inner.begin();
        }
    }

    fn commit(&mut self) {
        if let Some(inner) = self {
            inner.commit();
        }
    }

    fn rollback(&mut self) {
        if let Some(inner) = self {
            inner.rollback();
        }
    }

    fn is_in_transaction(&self) -> bool {
        self.as_ref()
            .map(|t| t.is_in_transaction())
            .unwrap_or(false)
    }
}

pub trait TransactionalCollection<K, V, R> {
    fn begin_all(&mut self);
    fn commit_all(&mut self);
    fn rollback_all(&mut self, snapshot_keys: &FastHashSet<K>, removed_items: R);
}

impl<K, V> TransactionalCollection<K, V, BTreeMap<K, V>> for BTreeMap<K, V>
where
    K: Clone + Hash + Ord + Eq,
    V: Transactional,
{
    fn begin_all(&mut self) {
        for item in self.values_mut() {
            item.begin();
        }
    }

    fn commit_all(&mut self) {
        for item in self.values_mut() {
            if item.is_in_transaction() {
                item.commit();
            }
        }
    }

    fn rollback_all(&mut self, snapshot_keys: &FastHashSet<K>, removed_items: BTreeMap<K, V>) {
        self.retain(|k, _| snapshot_keys.contains(k));
        self.extend(removed_items);
        for item in self.values_mut() {
            if item.is_in_transaction() {
                item.rollback();
            }
        }
    }
}

impl<K, V> TransactionalCollection<K, V, FastHashMap<K, V>> for FastHashMap<K, V>
where
    K: Clone + Hash + Eq,
    V: Transactional,
{
    fn begin_all(&mut self) {
        for item in self.values_mut() {
            item.begin();
        }
    }

    fn commit_all(&mut self) {
        for item in self.values_mut() {
            if item.is_in_transaction() {
                item.commit();
            }
        }
    }

    fn rollback_all(&mut self, snapshot_keys: &FastHashSet<K>, removed_items: FastHashMap<K, V>) {
        self.retain(|k, _| snapshot_keys.contains(k));
        self.extend(removed_items);
        for item in self.values_mut() {
            if item.is_in_transaction() {
                item.rollback();
            }
        }
    }
}
