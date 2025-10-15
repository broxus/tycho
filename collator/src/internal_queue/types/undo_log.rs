use std::collections::hash_map;
use std::hash::Hash;

use tycho_util::FastHashMap;

pub trait Transactional {
    fn begin_transaction(&mut self);
    fn commit_transaction(&mut self);
    fn rollback_transaction(&mut self);
}

#[derive(Debug, Default)]
struct UndoLog<K, V> {
    original_state: FastHashMap<K, Option<V>>,
    active: bool,
}

impl<K, V> UndoLog<K, V> {
    fn new() -> Self {
        Self {
            original_state: FastHashMap::default(),
            active: false,
        }
    }
    
    fn is_active(&self) -> bool {
        self.active
    }
}

impl<K, V> UndoLog<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{

    fn begin(&mut self) {
        assert!(!self.active, "Transaction already active");
        self.active = true;
        self.original_state.clear();
    }

    fn commit(&mut self) {
        assert!(self.active, "No active transaction");
        self.active = false;
        self.original_state.clear();
    }

    fn rollback(&mut self, map: &mut FastHashMap<K, V>) {
        assert!(self.active, "No active transaction");

        for (key, original) in self.original_state.drain() {
            match original {
                Some(value) => {
                    map.insert(key, value);
                }
                None => {
                    map.remove(&key);
                }
            }
        }

        self.active = false;
    }

    /// Record original state ONLY on first modification of a key
    fn record_original(&mut self, key: K, current_value: Option<V>) {
        if self.active {
            self.original_state.entry(key).or_insert(current_value);
        }
    }
}

/// Transactional wrapper around `FastHashMap`
#[derive(Debug)]
pub struct TransactionalMap<K, V> {
    map: FastHashMap<K, V>,
    undo_log: UndoLog<K, V>,
}

// ============================================
// Basic methods (available for all types)
// ============================================
impl<K, V> TransactionalMap<K, V> {
    pub fn new() -> Self {
        Self {
            map: FastHashMap::default(),
            undo_log: UndoLog::new(),
        }
    }

    pub fn from_map(map: FastHashMap<K, V>) -> Self {
        Self {
            map,
            undo_log: UndoLog::new(),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V>
    where
        K: Eq + Hash,
    {
        self.map.get(key)
    }

    pub fn inner(&self) -> &FastHashMap<K, V> {
        &self.map
    }

    pub fn into_inner(self) -> FastHashMap<K, V> {
        self.map
    }

    pub fn is_transaction_active(&self) -> bool {
        self.undo_log.is_active()
    }
}


impl<K, V> TransactionalMap<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let current = self.map.get(&key).cloned();
        self.undo_log.record_original(key.clone(), current);
        self.map.insert(key, value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let current = self.map.get(key).cloned();
        self.undo_log.record_original(key.clone(), current);
        self.map.remove(key)
    }

    pub fn modify<F>(&mut self, key: &K, f: F) -> bool
    where
        F: FnOnce(&mut V),
    {
        if let Some(value) = self.map.get_mut(key) {
            if self.undo_log.is_active() && !self.undo_log.original_state.contains_key(key) {
                let original = value.clone();
                self.undo_log
                    .record_original(key.clone(), Some(original));
            }
            f(value);
            true
        } else {
            false
        }
    }

    pub fn entry(&mut self, key: K) -> TransactionalEntry<'_, K, V> {
        TransactionalEntry {
            map: &mut self.map,
            undo_log: &mut self.undo_log,
            key,
        }
    }

    pub fn begin_transaction(&mut self) {
        self.undo_log.begin();
    }

    pub fn commit_transaction(&mut self) {
        self.undo_log.commit();
    }

    pub fn rollback_transaction(&mut self) {
        self.undo_log.rollback(&mut self.map);
    }
}


impl<K, V> TransactionalMap<K, V>
where
    K: Clone + Eq + Hash,
    V: Transactional,
{
    pub fn insert_transactional(&mut self, key: K, value: V) -> Option<V> {
        self.map.insert(key, value)
    }

    pub fn modify_transactional<F>(&mut self, key: &K, f: F) -> bool
    where
        F: FnOnce(&mut V),
    {
        if let Some(value) = self.map.get_mut(key) {
            f(value);
            true
        } else {
            false
        }
    }

    pub fn begin_transaction_deep(&mut self) {
        for value in self.map.values_mut() {
            value.begin_transaction();
        }
    }

    pub fn commit_transaction_deep(&mut self) {
        for value in self.map.values_mut() {
            value.commit_transaction();
        }
    }

    pub fn rollback_transaction_deep(&mut self) {
        for value in self.map.values_mut() {
            value.rollback_transaction();
        }
    }
}

impl<K, V> Default for TransactionalMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Transactional for TransactionalMap<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    fn begin_transaction(&mut self) {
        self.undo_log.begin();
    }

    fn commit_transaction(&mut self) {
        self.undo_log.commit();
    }

    fn rollback_transaction(&mut self) {
        self.undo_log.rollback(&mut self.map);
    }
}

/// Entry API for transactional map
pub struct TransactionalEntry<'a, K, V> {
    map: &'a mut FastHashMap<K, V>,
    undo_log: &'a mut UndoLog<K, V>,
    key: K,
}

impl<'a, K, V> TransactionalEntry<'a, K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    pub fn or_insert(self, default: V) -> &'a mut V {
        let current = self.map.get(&self.key).cloned();
        match self.map.entry(self.key.clone()) {
            hash_map::Entry::Occupied(entry) => entry.into_mut(),
            hash_map::Entry::Vacant(entry) => {
                self.undo_log.record_original(self.key, current);
                entry.insert(default)
            }
        }
    }

    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        let current = self.map.get(&self.key).cloned();
        match self.map.entry(self.key.clone()) {
            hash_map::Entry::Occupied(entry) => entry.into_mut(),
            hash_map::Entry::Vacant(entry) => {
                self.undo_log.record_original(self.key, current);
                entry.insert(default())
            }
        }
    }

    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        if let hash_map::Entry::Occupied(mut entry) = self.map.entry(self.key.clone()) {
            let old_value = entry.get().clone();
            self.undo_log
                .record_original(self.key.clone(), Some(old_value));
            f(entry.get_mut());
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================
    // Test structures
    // ============================================

    /// Heavy structure that does NOT implement Clone
    #[derive(Debug)]
    struct HeavyStruct {
        id: u32,
        // Simulating heavy nested transactional data
        nested_data: TransactionalMap<String, i32>,
            }

            impl HeavyStruct {
                fn new(id: u32) -> Self {
                    Self {
                id,
                nested_data: TransactionalMap::new(),
            }
        }

        fn add_data(&mut self, key: String, value: i32) {
            self.nested_data.insert(key, value);
        }

        fn get_data(&self, key: &str) -> Option<&i32> {
            self.nested_data.get(&key.to_string())
        }
    }

    impl Transactional for HeavyStruct {
        fn begin_transaction(&mut self) {
            self.nested_data.begin_transaction();
        }

        fn commit_transaction(&mut self) {
            self.nested_data.commit_transaction();
        }

        fn rollback_transaction(&mut self) {
            self.nested_data.rollback_transaction();
        }
    }

    // ============================================
    // Tests for Clone types (primitives)
    // ============================================

    #[test]
    fn test_basic_transaction() {
        let mut map = TransactionalMap::new();

        map.begin_transaction();
        map.insert(1, 100);
        map.insert(2, 200);
        assert_eq!(map.get(&1), Some(&100));
        assert_eq!(map.get(&2), Some(&200));

        map.commit_transaction();
        assert_eq!(map.get(&1), Some(&100));
        assert_eq!(map.get(&2), Some(&200));
    }

    #[test]
    fn test_rollback() {
        let mut map = TransactionalMap::new();
        map.insert(1, 100);

        map.begin_transaction();
        map.insert(1, 200);
        map.insert(2, 300);
        assert_eq!(map.get(&1), Some(&200));
        assert_eq!(map.get(&2), Some(&300));

        map.rollback_transaction();
        assert_eq!(map.get(&1), Some(&100));
        assert_eq!(map.get(&2), None);
    }

    #[test]
    fn test_modify() {
        let mut map = TransactionalMap::new();
        map.insert(1, 100);

        map.begin_transaction();
        map.modify(&1, |v| *v += 50);
        assert_eq!(map.get(&1), Some(&150));

        map.rollback_transaction();
        assert_eq!(map.get(&1), Some(&100));
    }

    #[test]
    fn test_multiple_modifications_same_key() {
        let mut map = TransactionalMap::new();
        map.insert(1, 100);

        map.begin_transaction();
        
        // Multiple modifications of the same key
        map.modify(&1, |v| *v += 10); // 110
        map.modify(&1, |v| *v += 20); // 130
        map.modify(&1, |v| *v *= 2);  // 260
        map.insert(1, 500);           // 500
        
        assert_eq!(map.get(&1), Some(&500));

        // Should rollback to original 100, not intermediate values
        map.rollback_transaction();
        assert_eq!(map.get(&1), Some(&100));
    }

    #[test]
    fn test_insert_modify_remove_same_key() {
        let mut map = TransactionalMap::new();

        map.begin_transaction();
        
        // Insert new key
        map.insert(1, 100);
        assert_eq!(map.get(&1), Some(&100));
        
        // Modify it
        map.modify(&1, |v| *v = 200);
        assert_eq!(map.get(&1), Some(&200));
        
        // Remove it
        map.remove(&1);
        assert_eq!(map.get(&1), None);
        
        // Rollback should restore to "didn't exist"
        map.rollback_transaction();
        assert_eq!(map.get(&1), None);
    }

    #[test]
    fn test_entry_api() {
        let mut map = TransactionalMap::new();

        map.begin_transaction();
        *map.entry(1).or_insert(0) += 10;
        *map.entry(1).or_insert(0) += 20;
        assert_eq!(map.get(&1), Some(&30));

        map.rollback_transaction();
        assert_eq!(map.get(&1), None);
    }

    #[test]
    fn test_entry_and_modify() {
        let mut map = TransactionalMap::new();
        map.insert(1, 100);

        map.begin_transaction();
        
        *map.entry(1).and_modify(|v| *v += 50).or_insert(0) += 10;
        assert_eq!(map.get(&1), Some(&160));

        map.entry(2).and_modify(|v| *v += 100).or_insert(20);
        assert_eq!(map.get(&2), Some(&20));

        map.rollback_transaction();
        assert_eq!(map.get(&1), Some(&100));
        assert_eq!(map.get(&2), None);
    }

    #[test]
    fn test_nested_transactions_not_allowed() {
        let mut map: TransactionalMap<i32, i32> = TransactionalMap::new();
        
        map.begin_transaction();
        assert!(map.is_transaction_active());
        
        // Should panic on nested transaction
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            map.begin_transaction();
        }));
        
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_without_transaction() {
        let mut map: TransactionalMap<i32, i32> = TransactionalMap::new();
        
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            map.commit_transaction();
        }));
        
        assert!(result.is_err());
    }

    // ============================================
    // Tests for Transactional types (heavy structures)
    // ============================================

    #[test]
    fn test_transactional_struct_rollback() {
        let mut map: TransactionalMap<i32, HeavyStruct> = TransactionalMap::new();
        
        let mut heavy = HeavyStruct::new(1);
        heavy.add_data("initial".to_string(), 100);
        map.insert_transactional(1, heavy);
        
        // Begin deep transaction
        map.begin_transaction_deep();
        
        // Modify nested data
        map.modify_transactional(&1, |h| {
            h.add_data("new".to_string(), 200);
            h.add_data("initial".to_string(), 999);
        });
        
        assert_eq!(map.get(&1).unwrap().get_data("new"), Some(&200));
        assert_eq!(map.get(&1).unwrap().get_data("initial"), Some(&999));
        
        // Rollback deep
        map.rollback_transaction_deep();
        
        // Nested data should be rolled back
        assert_eq!(map.get(&1).unwrap().get_data("new"), None);
        assert_eq!(map.get(&1).unwrap().get_data("initial"), Some(&100));
    }

    #[test]
    fn test_transactional_struct_commit() {
        let mut map: TransactionalMap<i32, HeavyStruct> = TransactionalMap::new();
        
        let mut heavy = HeavyStruct::new(1);
        heavy.add_data("key1".to_string(), 10);
        map.insert_transactional(1, heavy);
        
        map.begin_transaction_deep();
        
        map.modify_transactional(&1, |h| {
            h.add_data("key2".to_string(), 20);
        });
        
        map.commit_transaction_deep();
        
        // Changes should persist
        assert_eq!(map.get(&1).unwrap().get_data("key1"), Some(&10));
        assert_eq!(map.get(&1).unwrap().get_data("key2"), Some(&20));
    }

    #[test]
    fn test_deep_nested_transactional() {
        let mut outer: TransactionalMap<i32, TransactionalMap<String, u64>> =
            TransactionalMap::new();
        
        // Setup initial state
        let mut inner1 = TransactionalMap::new();
        inner1.insert("a".to_string(), 100);
        outer.insert_transactional(1, inner1);
        
        let mut inner2 = TransactionalMap::new();
        inner2.insert("b".to_string(), 200);
        outer.insert_transactional(2, inner2);
        
        // Begin deep transaction
        outer.begin_transaction_deep();
        
        // Modify nested maps
        outer.modify_transactional(&1, |inner| {
            inner.insert("a".to_string(), 999);
            inner.insert("new".to_string(), 111);
        });
        
        outer.modify_transactional(&2, |inner| {
            inner.remove(&"b".to_string());
        });
        
        assert_eq!(
            outer.get(&1).unwrap().get(&"a".to_string()),
            Some(&999)
        );
        assert_eq!(
            outer.get(&1).unwrap().get(&"new".to_string()),
            Some(&111)
        );
        assert_eq!(outer.get(&2).unwrap().get(&"b".to_string()), None);
        
        // Rollback entire hierarchy
        outer.rollback_transaction_deep();
        
        // All nested changes rolled back
        assert_eq!(
            outer.get(&1).unwrap().get(&"a".to_string()),
            Some(&100)
        );
        assert_eq!(outer.get(&1).unwrap().get(&"new".to_string()), None);
        assert_eq!(
            outer.get(&2).unwrap().get(&"b".to_string()),
            Some(&200)
        );
    }

    #[test]
    fn test_optimization_no_redundant_clones() {
        // This test verifies that we only clone the original value once,
        // not on every modification

        #[derive(Debug, PartialEq)]
        struct CloneCounter {
            value: i32,
            clone_count: std::rc::Rc<std::cell::RefCell<usize>>,
        }

        impl CloneCounter {
            fn new(value: i32) -> Self {
                Self {
                    value,
                    clone_count: std::rc::Rc::new(std::cell::RefCell::new(0)),
                }
            }

            fn clones(&self) -> usize {
                *self.clone_count.borrow()
            }
        }

        impl Clone for CloneCounter {
            fn clone(&self) -> Self {
                *self.clone_count.borrow_mut() += 1;
                Self {
                    value: self.value,
                    clone_count: self.clone_count.clone(),
                }
            }
        }

        let mut map = TransactionalMap::new();
        let counter = CloneCounter::new(100);
        map.insert(1, counter);

        let initial_clones = map.get(&1).unwrap().clones();

        map.begin_transaction();

        // Multiple modifications
        map.modify(&1, |v| v.value += 1);
        map.modify(&1, |v| v.value += 1);
        map.modify(&1, |v| v.value += 1);
        map.modify(&1, |v| v.value += 1);
        map.modify(&1, |v| v.value += 1);

        let final_clones = map.get(&1).unwrap().clones();

        // Should have cloned only ONCE (for the first modification)
        assert_eq!(final_clones - initial_clones, 1);

        map.rollback_transaction();
    }

    #[test]
    fn test_string_map() {
        let mut map: TransactionalMap<String, String> = TransactionalMap::new();
        
        map.insert("key1".to_string(), "value1".to_string());
        
        map.begin_transaction();
        map.insert("key1".to_string(), "modified".to_string());
        map.insert("key2".to_string(), "new".to_string());
        
        assert_eq!(map.get(&"key1".to_string()), Some(&"modified".to_string()));
        assert_eq!(map.get(&"key2".to_string()), Some(&"new".to_string()));
        
        map.rollback_transaction();
        
        assert_eq!(map.get(&"key1".to_string()), Some(&"value1".to_string()));
        assert_eq!(map.get(&"key2".to_string()), None);
    }

}
