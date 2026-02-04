use crate::transactional::Transactional;

pub struct TransactionalOption<T> {
    value: Option<T>,
    #[allow(clippy::option_option)]
    snapshot: Option<Option<T>>,
}

impl<T: Transactional> TransactionalOption<T> {
    pub fn new(value: Option<T>) -> Self {
        Self {
            value,
            snapshot: None,
        }
    }

    pub fn inner(&self) -> Option<&T> {
        self.value.as_ref()
    }

    pub fn get(&self) -> Option<&T> {
        self.value.as_ref()
    }

    pub fn inner_mut(&mut self) -> Option<&mut T> {
        self.value.as_mut()
    }

    pub fn set(&mut self, new: Option<T>) {
        if self.snapshot.is_none() && self.tx_active() {
            self.snapshot = Some(self.value.take());
        }
        self.value = new;
    }

    pub fn is_none(&self) -> bool {
        self.value.is_none()
    }

    pub fn is_some(&self) -> bool {
        self.value.is_some()
    }

    fn tx_active(&self) -> bool {
        self.value.as_ref().is_some_and(|v| v.in_tx()) || self.snapshot.is_some()
    }
}

impl<T: Transactional> Transactional for TransactionalOption<T> {
    fn begin(&mut self) {
        if let Some(inner) = &mut self.value {
            inner.begin();
        } else {
            self.snapshot = Some(None);
        }
    }

    fn commit(&mut self) {
        self.snapshot = None;
        if let Some(inner) = &mut self.value
            && inner.in_tx()
        {
            inner.commit();
        }
    }

    fn rollback(&mut self) {
        if let Some(snap) = self.snapshot.take() {
            self.value = snap;
        }
        if let Some(inner) = &mut self.value
            && inner.in_tx()
        {
            inner.rollback();
        }
    }

    fn in_tx(&self) -> bool {
        self.tx_active()
    }
}

impl<T: Transactional> Default for TransactionalOption<T> {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transactional::value::TransactionalValue;

    #[test]
    fn rollback_some_to_none() {
        let mut opt = TransactionalOption::new(Some(TransactionalValue::new(10)));
        opt.begin();
        opt.set(None);
        assert!(opt.inner().is_none());
        opt.rollback();
        assert_eq!(**opt.inner().unwrap(), 10);
    }

    #[test]
    fn commit_some_to_none() {
        let mut opt = TransactionalOption::new(Some(TransactionalValue::new(10)));
        opt.begin();
        opt.set(None);
        opt.commit();
        assert!(opt.inner().is_none());
    }

    #[test]
    fn rollback_none_to_some() {
        let mut opt: TransactionalOption<TransactionalValue<i32>> = TransactionalOption::new(None);
        opt.begin();
        opt.set(Some(TransactionalValue::new(42)));
        assert_eq!(**opt.inner().unwrap(), 42);
        opt.rollback();
        assert!(opt.inner().is_none());
    }

    #[test]
    fn commit_none_to_some() {
        let mut opt: TransactionalOption<TransactionalValue<i32>> = TransactionalOption::new(None);
        opt.begin();
        opt.set(Some(TransactionalValue::new(42)));
        opt.commit();
        assert_eq!(**opt.inner().unwrap(), 42);
    }

    #[test]
    fn rollback_some_to_some() {
        let mut opt = TransactionalOption::new(Some(TransactionalValue::new(1)));
        opt.begin();
        opt.set(Some(TransactionalValue::new(2)));
        assert_eq!(**opt.inner().unwrap(), 2);
        opt.rollback();
        assert_eq!(**opt.inner().unwrap(), 1);
    }

    #[test]
    fn rollback_no_change() {
        let mut opt = TransactionalOption::new(Some(TransactionalValue::new(5)));
        opt.begin();
        opt.rollback();
        assert_eq!(**opt.inner().unwrap(), 5);
    }

    #[test]
    fn rollback_none_no_change() {
        let mut opt: TransactionalOption<TransactionalValue<i32>> = TransactionalOption::new(None);
        opt.begin();
        opt.rollback();
        assert!(opt.inner().is_none());
    }

    #[test]
    fn in_tx_tracking() {
        let mut opt = TransactionalOption::new(Some(TransactionalValue::new(0)));
        assert!(!opt.in_tx());
        opt.begin();
        assert!(opt.in_tx());
        opt.commit();
        assert!(!opt.in_tx());
    }

    #[test]
    fn in_tx_none() {
        let mut opt: TransactionalOption<TransactionalValue<i32>> = TransactionalOption::new(None);
        assert!(!opt.in_tx());
        opt.begin();
        assert!(opt.in_tx());
        opt.rollback();
        assert!(!opt.in_tx());
    }

    #[test]
    fn set_new_value_during_tx_commit_no_panic() {
        let mut opt = TransactionalOption::new(Some(TransactionalValue::new(1)));
        opt.begin();
        opt.set(Some(TransactionalValue::new(99)));
        opt.commit();
        assert_eq!(**opt.inner().unwrap(), 99);
    }
}
