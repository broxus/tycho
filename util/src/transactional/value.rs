use std::ops::{Deref, DerefMut};

use crate::transactional::Transactional;

pub struct TransactionalValue<T> {
    value: T,
    snapshot: Option<T>,
}

impl<T> TransactionalValue<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            snapshot: None,
        }
    }
}

impl<T> Deref for TransactionalValue<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for TransactionalValue<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: Default> Default for TransactionalValue<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for TransactionalValue<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> AsRef<T> for TransactionalValue<T> {
    fn as_ref(&self) -> &T {
        &self.value
    }
}

impl<T> AsMut<T> for TransactionalValue<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: Clone> Transactional for TransactionalValue<T> {
    fn begin(&mut self) {
        debug_assert!(self.snapshot.is_none());
        self.snapshot = Some(self.value.clone());
    }

    fn commit(&mut self) {
        debug_assert!(self.snapshot.is_some());
        self.snapshot = None;
    }

    fn rollback(&mut self) {
        if let Some(snap) = self.snapshot.take() {
            self.value = snap;
        }
    }

    fn in_tx(&self) -> bool {
        self.snapshot.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_and_deref() {
        let v = TransactionalValue::new(42);
        assert_eq!(*v, 42);
    }

    #[test]
    fn deref_mut() {
        let mut v = TransactionalValue::new(1);
        *v = 10;
        assert_eq!(*v, 10);
    }

    #[test]
    fn default_trait() {
        let v: TransactionalValue<i32> = TransactionalValue::default();
        assert_eq!(*v, 0);
    }

    #[test]
    fn in_tx_tracking() {
        let mut v = TransactionalValue::new(0);
        assert!(!v.in_tx());
        v.begin();
        assert!(v.in_tx());
        v.commit();
        assert!(!v.in_tx());
    }

    #[test]
    fn commit_keeps_new_value() {
        let mut v = TransactionalValue::new(1);
        v.begin();
        *v = 2;
        v.commit();
        assert_eq!(*v, 2);
    }

    #[test]
    fn rollback_restores_original() {
        let mut v = TransactionalValue::new(1);
        v.begin();
        *v = 999;
        v.rollback();
        assert_eq!(*v, 1);
    }

    #[test]
    fn rollback_after_multiple_mutations() {
        let mut v = TransactionalValue::new(1);
        v.begin();
        *v = 2;
        *v = 3;
        *v = 4;
        v.rollback();
        assert_eq!(*v, 1);
    }

    #[test]
    fn rollback_no_change() {
        let mut v = TransactionalValue::new(5);
        v.begin();
        v.rollback();
        assert_eq!(*v, 5);
    }

    #[test]
    fn rollback_outside_tx_is_noop() {
        let mut v = TransactionalValue::new(5);
        v.rollback();
        assert_eq!(*v, 5);
    }

    #[test]
    fn sequential_commit_then_rollback() {
        let mut v = TransactionalValue::new(1);
        v.begin();
        *v = 2;
        v.commit();

        v.begin();
        *v = 3;
        v.rollback();
        assert_eq!(*v, 2);
    }

    #[test]
    fn sequential_rollback_then_commit() {
        let mut v = TransactionalValue::new(1);
        v.begin();
        *v = 2;
        v.rollback();

        v.begin();
        *v = 3;
        v.commit();
        assert_eq!(*v, 3);
    }

    #[test]
    fn works_with_string() {
        let mut v = TransactionalValue::new(String::from("hello"));
        v.begin();
        v.push_str(" world");
        assert_eq!(&*v, "hello world");
        v.rollback();
        assert_eq!(&*v, "hello");
    }

    #[test]
    fn works_with_vec() {
        let mut v = TransactionalValue::new(vec![1, 2, 3]);
        v.begin();
        v.push(4);
        v.push(5);
        assert_eq!(&*v, &[1, 2, 3, 4, 5]);
        v.rollback();
        assert_eq!(&*v, &[1, 2, 3]);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn double_begin_panics_in_debug() {
        let mut v = TransactionalValue::new(1);
        v.begin();
        v.begin();
    }
}
