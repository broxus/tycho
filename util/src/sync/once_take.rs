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
            let value = unsafe { self.value.assume_init_read() };
            Some(value)
        } else {
            None
        }
    }
    pub fn has_value(&self) -> bool {
        self.has_value.load(Ordering::Relaxed)
    }
}
impl<T> Drop for OnceTake<T> {
    fn drop(&mut self) {
        if *self.has_value.get_mut() {
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(once_take)),
            file!(),
            50u32,
        );
        let counter = DropCounter::default();
        let once = Arc::new(OnceTake::new(counter.clone()));
        let once_1 = once.clone();
        let fut_1 = async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                55u32,
            );
            once_1.take().map(|copy| copy.get())
        };
        let once_2 = once.clone();
        let fut_2 = async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                58u32,
            );
            once_2.take().map(|copy| copy.get())
        };
        let mut result = [
            {
                __guard.end_section(60u32);
                let __result = tokio::spawn(fut_1).await;
                __guard.start_section(60u32);
                __result
            }?,
            {
                __guard.end_section(60u32);
                let __result = tokio::spawn(fut_2).await;
                __guard.start_section(60u32);
                __result
            }?,
        ];
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
