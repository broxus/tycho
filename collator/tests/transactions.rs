use std::collections::BTreeMap;

use tycho_collator::collator::ForceMasterCollation::No;
use tycho_util::transactional_types::{Transactional, TransactionalCollection};
use tycho_util::{FastHashMap, FastHashSet};
use tycho_util_proc::Transactional;

// ------------------------------
// Test types
// ------------------------------

#[derive(Transactional)]
struct TestState {
    #[tx(collection)]
    values: BTreeMap<i32, TestValue>,

    non_tx_values: BTreeMap<i32, TestValueNonTx>,

    #[tx(state)]
    tx: Option<TestStateTx>,
}

#[derive(Transactional)]
struct TestValue {
    value: i32,
    optional_value: Option<i32>,

    #[tx(state)]
    tx: Option<TestValueTx>,
}

#[derive(Clone)]
struct TestValueNonTx {
    value: i32,
    optional_value: Option<i32>,
}

#[derive(Transactional)]
struct TxValue {
    #[tx(state)]
    tx: Option<TxValueTx>,
    value: i32,
}

impl TxValue {
    fn new(value: i32) -> Self {
        Self { tx: None, value }
    }
}

#[derive(Transactional)]
struct TxComplex {
    #[tx(state)]
    tx: Option<TxComplexTx>,

    name: String,
    count: u32,

    #[tx(transactional)]
    nested: TxValue,

    #[tx(transactional)]
    maybe: Option<TxValue>,

    #[tx(skip)]
    cache: i32,
}

impl TxComplex {
    fn new(name: &str, count: u32, nested_val: i32) -> Self {
        Self {
            tx: None,
            name: name.to_string(),
            count,
            nested: TxValue::new(nested_val),
            maybe: None,
            cache: 0,
        }
    }
}

// ------------------------------
// Helpers
// ------------------------------

fn make_state() -> TestState {
    let mut state = TestState {
        values: Default::default(),
        non_tx_values: Default::default(),
        tx: None,
    };

    state.values.insert(1, TestValue {
        value: 10,
        optional_value: None,
        tx: None,
    });
    state.values.insert(2, TestValue {
        value: 15,
        optional_value: Some(3),
        tx: None,
    });

    state.non_tx_values.insert(1, TestValueNonTx {
        value: 100,
        optional_value: None,
    });
    state.non_tx_values.insert(2, TestValueNonTx {
        value: 150,
        optional_value: Some(3),
    });

    state
}

fn assert_state_eq_baseline(state: &TestState) {
    assert_eq!(state.values.len(), 2);
    assert_eq!(state.non_tx_values.len(), 2);

    {
        let v = state.values.get(&1).unwrap();
        assert_eq!(v.value, 10);
        assert_eq!(v.optional_value, None);
    }
    {
        let v = state.values.get(&2).unwrap();
        assert_eq!(v.value, 15);
        assert_eq!(v.optional_value, Some(3));
    }

    {
        let v = state.non_tx_values.get(&1).unwrap();
        assert_eq!(v.value, 100);
        assert_eq!(v.optional_value, None);
    }
    {
        let v = state.non_tx_values.get(&2).unwrap();
        assert_eq!(v.value, 150);
        assert_eq!(v.optional_value, Some(3));
    }
}

// ------------------------------
// TxValue - commit/rollback + invariants
// ------------------------------

#[test]
fn txvalue_commit_and_invariants() {
    let mut val = TxValue::new(10);

    assert!(!val.is_in_transaction());
    val.begin();
    assert!(val.is_in_transaction());

    val.value = 99;
    val.commit();

    assert!(!val.is_in_transaction());
    assert_eq!(val.value, 99);
}

#[test]
fn txvalue_rollback_and_invariants() {
    let mut val = TxValue::new(10);

    val.begin();
    assert!(val.is_in_transaction());

    val.value = 99;
    val.rollback();

    assert!(!val.is_in_transaction());
    assert_eq!(val.value, 10);
}

#[test]
fn txvalue_multiple_transactions() {
    let mut val = TxValue::new(1);

    val.begin();
    val.value = 10;
    val.commit();
    assert!(!val.is_in_transaction());
    assert_eq!(val.value, 10);

    val.begin();
    val.value = 100;
    val.rollback();
    assert!(!val.is_in_transaction());
    assert_eq!(val.value, 10);

    val.begin();
    val.value = 777;
    val.commit();
    assert!(!val.is_in_transaction());
    assert_eq!(val.value, 777);
}

#[test]
#[should_panic(expected = "transaction already in progress")]
fn txvalue_double_begin_panics() {
    let mut val = TxValue::new(1);
    val.begin();
    val.begin();
}

#[test]
#[should_panic(expected = "no active transaction")]
fn txvalue_commit_without_begin_panics() {
    let mut val = TxValue::new(1);
    val.commit();
}

#[test]
#[should_panic(expected = "no active transaction")]
fn txvalue_rollback_without_begin_panics() {
    let mut val = TxValue::new(1);
    val.rollback();
}

// ------------------------------
// Option<TxValue> transitions
// ------------------------------

#[test]
fn option_some_modify_commit() {
    let mut opt = Some(TxValue::new(10));
    assert!(!opt.is_in_transaction());

    opt.begin();
    assert!(opt.is_in_transaction());

    opt.as_mut().unwrap().value = 99;
    opt.commit();

    assert!(!opt.is_in_transaction());
    assert_eq!(opt.unwrap().value, 99);
}

#[test]
fn option_some_modify_rollback() {
    let mut opt = Some(TxValue::new(10));

    opt.begin();
    opt.as_mut().unwrap().value = 99;
    opt.rollback();

    assert!(!opt.is_in_transaction());
    assert_eq!(opt.unwrap().value, 10);
}

// ------------------------------
// TxComplex - nested/option/skip + invariants
// ------------------------------

#[test]
fn complex_commit_all_fields_invariants() {
    let mut obj = TxComplex::new("test", 5, 100);

    assert!(!obj.is_in_transaction());
    assert!(!obj.nested.is_in_transaction());
    assert!(!obj.maybe.is_in_transaction());

    obj.begin();
    assert!(obj.is_in_transaction());
    assert!(obj.nested.is_in_transaction());
    assert_eq!(obj.maybe.is_in_transaction(), obj.maybe.is_some());

    obj.name = "changed".to_string();
    obj.count = 999;
    obj.nested.value = 200;
    obj.tx_set_maybe(Some(TxValue::new(50)));
    obj.cache = 42;

    obj.commit();

    assert!(!obj.is_in_transaction());
    assert!(!obj.nested.is_in_transaction());
    assert!(!obj.maybe.is_in_transaction());

    assert_eq!(obj.name, "changed");
    assert_eq!(obj.count, 999);
    assert_eq!(obj.nested.value, 200);
    assert_eq!(obj.maybe.as_ref().unwrap().value, 50);
    assert_eq!(obj.cache, 42);
}

#[test]
fn complex_rollback_restores_tx_fields_but_not_skip_invariants() {
    let mut obj = TxComplex::new("test", 5, 100);

    obj.begin();
    obj.name = "changed".to_string();
    obj.count = 999;
    obj.nested.value = 200;
    obj.tx_set_maybe(Some(TxValue::new(50)));
    obj.cache = 42;

    obj.rollback();

    assert!(!obj.is_in_transaction());
    assert!(!obj.nested.is_in_transaction());
    assert!(!obj.maybe.is_in_transaction());

    assert_eq!(obj.name, "test");
    assert_eq!(obj.count, 5);
    assert_eq!(obj.nested.value, 100);
    assert!(obj.maybe.is_none());
    assert_eq!(obj.cache, 42); // skip — не откатился
}

#[test]
fn complex_option_none_to_some_rollback() {
    let mut obj = TxComplex::new("test", 1, 10);
    assert!(obj.maybe.is_none());

    obj.begin();
    obj.tx_set_maybe(Some(TxValue::new(50)));
    obj.rollback();

    assert!(!obj.is_in_transaction());
    assert!(obj.maybe.is_none());
}

#[test]
fn complex_option_some_to_none_rollback() {
    let mut obj = TxComplex::new("test", 1, 10);
    obj.maybe = Some(TxValue::new(7));

    obj.begin();
    obj.tx_set_maybe(None);
    obj.rollback();

    assert!(obj.maybe.is_some());
    assert_eq!(obj.maybe.as_ref().unwrap().value, 7);
}

// ------------------------------
// TestState - transactional collection vs plain map
// ------------------------------

#[test]
fn state_commit_modify_existing_values() {
    let mut state = make_state();
    assert_state_eq_baseline(&state);

    state.begin();

    // transactional collection element modification
    {
        let v = state.values.get_mut(&1).unwrap();
        v.value = 20;
        v.optional_value = Some(5);
    }
    {
        let v = state.values.get_mut(&2).unwrap();
        v.value = 25;
        v.optional_value = None;
    }

    // plain map modifications (should follow whole-struct tx behavior)
    {
        let v = state.non_tx_values.get_mut(&1).unwrap();
        v.value = 200;
        v.optional_value = Some(5);
    }
    {
        let v = state.non_tx_values.get_mut(&2).unwrap();
        v.value = 250;
        v.optional_value = None;
    }

    state.commit();

    assert!(!state.is_in_transaction());

    {
        let v = state.values.get(&1).unwrap();
        assert_eq!(v.value, 20);
        assert_eq!(v.optional_value, Some(5));
        assert!(!v.is_in_transaction());
    }
    {
        let v = state.values.get(&2).unwrap();
        assert_eq!(v.value, 25);
        assert_eq!(v.optional_value, None);
        assert!(!v.is_in_transaction());
    }
    {
        let v = state.non_tx_values.get(&1).unwrap();
        assert_eq!(v.value, 200);
        assert_eq!(v.optional_value, Some(5));
    }
    {
        let v = state.non_tx_values.get(&2).unwrap();
        assert_eq!(v.value, 250);
        assert_eq!(v.optional_value, None);
    }
}

#[test]
fn state_rollback_modify_existing_values() {
    let mut state = make_state();
    assert_state_eq_baseline(&state);

    state.begin();

    {
        let v = state.values.get_mut(&1).unwrap();
        v.value = 20;
        v.optional_value = Some(5);
    }
    {
        let v = state.values.get_mut(&2).unwrap();
        v.value = 25;
        v.optional_value = None;
    }

    {
        let v = state.non_tx_values.get_mut(&1).unwrap();
        v.value = 200;
        v.optional_value = Some(5);
    }
    {
        let v = state.non_tx_values.get_mut(&2).unwrap();
        v.value = 250;
        v.optional_value = None;
    }

    state.rollback();

    assert!(!state.is_in_transaction());
    assert_state_eq_baseline(&state);

    for v in state.values.values() {
        assert!(!v.is_in_transaction());
    }
}

#[test]
fn state_commit_add_then_remove_in_new_tx_rollback_restores() {
    let mut state = TestState {
        values: Default::default(),
        non_tx_values: Default::default(),
        tx: None,
    };

    state.values.insert(1, TestValue {
        value: 10,
        optional_value: None,
        tx: None,
    });
    state.non_tx_values.insert(1, TestValueNonTx {
        value: 100,
        optional_value: None,
    });

    // TX #1: add and commit
    state.begin();
    state.tx_insert_values(2, TestValue {
        value: 30,
        optional_value: Some(2),
        tx: None,
    });
    state.non_tx_values.insert(2, TestValueNonTx {
        value: 30,
        optional_value: Some(2),
    });
    state.commit();

    assert!(state.values.contains_key(&2));
    assert!(state.non_tx_values.contains_key(&2));
    assert_eq!(state.values.len(), 2);
    assert_eq!(state.non_tx_values.len(), 2);

    // TX #2: remove, but rollback
    state.begin();
    state.tx_remove_values(&2);
    state.non_tx_values.remove(&2);

    assert!(!state.values.contains_key(&2));
    assert!(!state.non_tx_values.contains_key(&2));

    state.rollback();

    assert!(state.values.contains_key(&2));
    assert!(state.non_tx_values.contains_key(&2));
    assert_eq!(state.values.len(), 2);
    assert_eq!(state.non_tx_values.len(), 2);
}

#[test]
fn state_rollback_add_in_tx_removes_added() {
    let mut state = TestState {
        values: Default::default(),
        non_tx_values: Default::default(),
        tx: None,
    };

    state.values.insert(1, TestValue {
        value: 10,
        optional_value: None,
        tx: None,
    });
    state.non_tx_values.insert(1, TestValueNonTx {
        value: 100,
        optional_value: None,
    });

    state.begin();
    state.tx_insert_values(2, TestValue {
        value: 30,
        optional_value: Some(2),
        tx: None,
    });
    state.non_tx_values.insert(2, TestValueNonTx {
        value: 30,
        optional_value: Some(2),
    });

    assert!(state.values.contains_key(&2));
    assert!(state.non_tx_values.contains_key(&2));

    state.rollback();

    assert!(!state.values.contains_key(&2));
    assert!(!state.non_tx_values.contains_key(&2));
    assert_eq!(state.values.len(), 1);
    assert_eq!(state.non_tx_values.len(), 1);
}

// ------------------------------
// Collection helpers: begin_all / commit_all / rollback_all
// ------------------------------

#[test]
fn btreemap_begin_commit_all_modifies_and_clears_tx() {
    let mut map = BTreeMap::new();
    map.insert("a", TxValue::new(1));
    map.insert("b", TxValue::new(2));

    map.begin_all();
    assert!(map.get("a").unwrap().is_in_transaction());
    assert!(map.get("b").unwrap().is_in_transaction());

    map.get_mut("a").unwrap().value = 100;
    map.get_mut("b").unwrap().value = 200;

    map.commit_all();

    assert!(!map.get("a").unwrap().is_in_transaction());
    assert!(!map.get("b").unwrap().is_in_transaction());
    assert_eq!(map.get("a").unwrap().value, 100);
    assert_eq!(map.get("b").unwrap().value, 200);
}

#[test]
fn btreemap_rollback_all_handles_modify_insert_remove_and_clears_tx() {
    let mut map = BTreeMap::new();
    map.insert(1, TxValue::new(10));
    map.insert(2, TxValue::new(20));
    map.insert(3, TxValue::new(30));

    let snapshot_keys: FastHashSet<i32> = map.keys().cloned().collect();

    map.begin_all();
    map.get_mut(&1).unwrap().value = 999; // modify existing
    map.insert(4, TxValue::new(40)); // new insert

    // remove two keys to ensure removed_items aggregation works
    let removed2 = map.remove(&2).unwrap();
    let removed3 = map.remove(&3).unwrap();

    let mut removed_items = BTreeMap::new();
    removed_items.insert(2, removed2);
    removed_items.insert(3, removed3);

    map.rollback_all(&snapshot_keys, removed_items);

    assert_eq!(map.len(), 3);
    assert!(!map.contains_key(&4));
    assert!(map.contains_key(&2));
    assert!(map.contains_key(&3));
    assert_eq!(map.get(&1).unwrap().value, 10);
    assert_eq!(map.get(&2).unwrap().value, 20);
    assert_eq!(map.get(&3).unwrap().value, 30);

    for v in map.values() {
        assert!(!v.is_in_transaction());
    }
}

#[test]
fn fasthashmap_rollback_all_handles_modify_insert_and_clears_tx() {
    let mut map: FastHashMap<i32, TxValue> = FastHashMap::default();
    map.insert(1, TxValue::new(10));
    map.insert(2, TxValue::new(20));

    let snapshot_keys: FastHashSet<i32> = map.keys().cloned().collect();

    map.begin_all();
    map.get_mut(&1).unwrap().value = 999;
    map.insert(3, TxValue::new(30));

    let removed = map.remove(&2).unwrap();
    let mut removed_items: FastHashMap<i32, TxValue> = FastHashMap::default();
    removed_items.insert(2, removed);

    map.rollback_all(&snapshot_keys, removed_items);

    assert_eq!(map.len(), 2);
    assert!(!map.contains_key(&3));
    assert!(map.contains_key(&2));
    assert_eq!(map.get(&1).unwrap().value, 10);
    assert_eq!(map.get(&2).unwrap().value, 20);

    for v in map.values() {
        assert!(!v.is_in_transaction());
    }
}

#[test]
fn complex_maybe_some_modify_and_rollback() {
    let mut obj = TxComplex::new("test", 1, 10);
    obj.maybe = Some(TxValue::new(7));

    obj.begin();
    assert!(obj.maybe.is_in_transaction());

    obj.maybe.as_mut().unwrap().value = 999;
    obj.rollback();

    assert!(obj.maybe.is_some());
    assert_eq!(obj.maybe.as_ref().unwrap().value, 7);
}

#[test]
fn complex_maybe_some_modify_and_commit() {
    let mut obj = TxComplex::new("test", 1, 10);
    obj.maybe = Some(TxValue::new(7));

    obj.begin();
    assert!(obj.maybe.is_in_transaction());

    obj.maybe.as_mut().unwrap().value = 999;
    obj.commit();

    assert!(obj.maybe.is_some());
    assert_eq!(obj.maybe.as_ref().unwrap().value, 999);
}

#[test]
fn complex_option_none_to_some_commit() {
    let mut obj = TxComplex::new("test", 1, 10);
    assert!(obj.maybe.is_none());

    obj.begin();
    obj.tx_set_maybe(Some(TxValue::new(50)));
    obj.commit();

    assert!(obj.maybe.is_some());
    assert_eq!(obj.maybe.as_ref().unwrap().value, 50);
}

fn direct_set_option_some() {
    let mut test_value = TestValue {
        value: 42,
        optional_value: None,
        tx: None,
    };

    test_value.begin();
    test_value.optional_value = Some(100);

    test_value.commit();
    assert_eq!(test_value.optional_value, Some(100));
}

fn direct_set_option_node() {
    let mut test_value = TestValue {
        value: 42,
        optional_value: Some(100),
        tx: None,
    };

    test_value.begin();
    test_value.optional_value = None;

    test_value.commit();
    assert_eq!(test_value.optional_value, None);
}
