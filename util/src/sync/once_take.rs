use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct OnceTake<T> {
    value: MaybeUninit<T>,
    has_value: AtomicBool,
}

impl<T> OnceTake<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: MaybeUninit::new(value),
            has_value: AtomicBool::new(true),
        }
    }
    pub fn take(&self) -> Option<T> {
        if self.has_value.swap(false, Ordering::Relaxed) {
            // SAFETY: `self.value` is initialized and contains a valid `T`.
            // `self.has_value` is disarmed and prevents the value from being read twice;
            // the value will be dropped at the calling site.
            let value = unsafe { self.value.assume_init_read() };
            Some(value)
        } else {
            None
        }
    }
}

impl<T> Drop for OnceTake<T> {
    fn drop(&mut self) {
        if *self.has_value.get_mut() {
            // SAFETY: we are the only thread executing Drop,
            // and the value is not dropped outside as per `self.has_value`
            unsafe { self.value.assume_init_drop() }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use super::OnceTake;

    #[tokio::test]
    async fn once_take() -> anyhow::Result<()> {
        let counter = DropCounter::default();
        let once = Arc::new(OnceTake::new(counter.clone()));

        let once_1 = once.clone();
        let fut_1 = async move { once_1.take().map(|copy| copy.get()) };

        let once_2 = once.clone();
        let fut_2 = async move { once_2.take().map(|copy| copy.get()) };

        let mut result = [tokio::spawn(fut_1).await?, tokio::spawn(fut_2).await?];
        result.sort();
        assert_eq!([None, Some(0)], result);

        assert_eq!(1, counter.get());

        assert_eq!(None, once.clone().take());
        drop(once);
        assert_eq!(1, counter.get());
        Ok(())
    }

    #[derive(Default, Clone, Debug)]
    struct DropCounter {
        counter: Arc<Mutex<u8>>,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            let mut guard = self.counter.lock().unwrap();
            *guard += 1;
        }
    }
    impl DropCounter {
        pub fn get(&self) -> u8 {
            let guard = self.counter.lock().unwrap();
            *guard
        }
    }
    impl PartialEq for DropCounter {
        fn eq(&self, other: &Self) -> bool {
            self.get() == other.get()
        }
    }
}
