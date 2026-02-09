use std::ops::{Deref, DerefMut};

pub mod btreemap;
pub mod hashmap;
pub mod option;
pub mod value;

pub trait Transactional {
    fn begin(&mut self);
    fn commit(&mut self);
    fn rollback(&mut self);
    fn in_tx(&self) -> bool;
}

/// RAII guard for `Transactional` types.
/// Calls `begin()` on creation and `rollback()` on drop if not committed.
pub struct TransactionGuard<'a, T: Transactional> {
    inner: &'a mut T,
    committed: bool,
}

impl<'a, T: Transactional> TransactionGuard<'a, T> {
    pub fn new(inner: &'a mut T) -> Self {
        inner.begin();
        Self {
            inner,
            committed: false,
        }
    }

    pub fn commit(mut self) {
        self.inner.commit();
        self.committed = true;
    }
}

impl<T: Transactional> Drop for TransactionGuard<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            self.inner.rollback();
        }
    }
}

impl<T: Transactional> Deref for TransactionGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.inner
    }
}

impl<T: Transactional> DerefMut for TransactionGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transactional::value::TransactionalValue;

    #[test]
    fn guard_commits() {
        let mut v = TransactionalValue::new(1);
        {
            let mut guard = TransactionGuard::new(&mut v);
            **guard = 2;
            guard.commit();
        }
        assert_eq!(*v, 2);
    }

    #[test]
    fn guard_rollbacks_on_drop() {
        let mut v = TransactionalValue::new(1);
        {
            let mut guard = TransactionGuard::new(&mut v);
            **guard = 2;
            // no commit - should rollback
        }
        assert_eq!(*v, 1);
    }

    #[test]
    fn guard_deref() {
        let mut v = TransactionalValue::new(42);
        let guard = TransactionGuard::new(&mut v);
        assert_eq!(**guard, 42);
    }
}
