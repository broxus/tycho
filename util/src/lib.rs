use std::collections::HashMap;
use std::collections::HashSet;

pub mod progress_bar;
pub mod serde_helpers;
pub mod time;

pub mod futures {
    pub use self::box_future_or_noop::BoxFutureOrNoop;
    pub use self::join_task::JoinTask;
    pub use self::shared::{Shared, WeakShared};

    mod box_future_or_noop;
    mod join_task;
    mod shared;
}

pub mod sync {
    pub use self::priority_semaphore::{AcquireError, PrioritySemaphore, TryAcquireError};

    mod priority_semaphore;
}

mod util {
    pub(crate) mod linked_list;
    pub(crate) mod wake_list;
}

pub type FastDashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;
pub type FastDashSet<K> = dashmap::DashSet<K, ahash::RandomState>;
pub type FastHashMap<K, V> = HashMap<K, V, ahash::RandomState>;
pub type FastHashSet<K> = HashSet<K, ahash::RandomState>;
pub type FastHasherState = ahash::RandomState;

/// # Example
///
/// ```rust
/// # use tycho_util::realloc_box_enum;
/// enum Value {
///     One(BigValue1),
///     Two(BigValue2),
/// }
///
/// struct BigValue1([u32; 10]);
///
/// struct BigValue2([u32; 7]);
///
/// fn convert_to_one(value: Box<Value>) -> Option<Box<BigValue1>> {
///     realloc_box_enum!(value, {
///         Value::One(value) => Box::new(value) => Some(value),
///         _ => None,
///     })
/// }
/// ```
#[macro_export]
macro_rules! realloc_box_enum {
    ($value:expr, {
        $target_variant:pat => Box::new($extracted:ident) => $target:expr,
        $other_variant:pat => $other:expr,
    }) => {{
        let value: ::std::boxed::Box<_> = $value;
        match ::core::convert::AsRef::as_ref(&value) {
            #[allow(unused_variables)]
            $target_variant => {
                let $extracted = unsafe {
                    $crate::__internal::realloc_box(value, |value| match value {
                        $target_variant => $extracted,
                        _ => unreachable!(),
                    })
                };
                $target
            }
            $other_variant => $other,
        }
    }};
}

#[doc(hidden)]
pub mod __internal {
    /// # Safety
    /// The following must be true:
    /// - `T` must have the same layout as `R`
    /// - `f` must not panic
    pub unsafe fn realloc_box<T, F, R>(value: Box<T>, f: F) -> Box<R>
    where
        F: FnOnce(T) -> R,
    {
        assert!(std::mem::align_of::<T>() == std::mem::align_of::<R>());

        let ptr = Box::into_raw(value);
        let value = std::ptr::read(ptr);

        let ptr = std::alloc::realloc(
            ptr.cast::<u8>(),
            std::alloc::Layout::new::<T>(),
            std::mem::size_of::<R>(),
        )
        .cast::<R>();

        if ptr.is_null() {
            std::alloc::handle_alloc_error(std::alloc::Layout::new::<R>());
        }

        // NOTE: in case of panic, the memory will be leaked
        std::ptr::write(ptr, f(value));

        Box::from_raw(ptr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(dead_code)]
    fn realloc_enum() {
        enum Value {
            One(BigValue1),
            Two(BigValue2),
        }

        #[derive(Clone)]
        struct BigValue1([u32; 10]);

        #[derive(Clone)]
        struct BigValue2([u32; 7]);

        fn convert_to_one(value: Box<Value>) -> Option<Box<BigValue1>> {
            realloc_box_enum!(value, {
                Value::One(value) => Box::new(value) => Some(value),
                _ => None,
            })
        }

        let value = BigValue1([123; 10]);
        let one = convert_to_one(Box::new(Value::One(value.clone())));
        assert_eq!(one.unwrap().0, value.0);
    }
}

#[cfg(feature = "test-util")]
pub fn init_logger() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("debug"))
        .try_init()
        .ok();
}
