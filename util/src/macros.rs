#[macro_export]
macro_rules! rl_trace {
    ($($arg:tt)*) => {{

        use std::cell::RefCell;

        thread_local! {
            static LAST_TRACE: RefCell<$crate::FastHalfBrownMap<u32, ::std::time::Instant>> = ::std::cell::RefCell::new($crate::FastHalfBrownMap::default());
        }

        LAST_TRACE.with(|last_trace| {
            let mut map = last_trace.borrow_mut();
            let now = ::std::time::Instant::now();
            let last = map.entry(line!()).or_insert(now - ::std::time::Duration::from_secs(1));

            if now.duration_since(*last) >= ::std::time::Duration::from_secs(1) {
                *last = now;
                ::tracing::warn!($($arg)*);
            }
        });
    }};
}

#[macro_export]
macro_rules! rl_debug {
    ($($arg:tt)*) => {{

        use std::cell::RefCell;

        thread_local! {
            static LAST_DEBUG: RefCell<$crate::FastHalfBrownMap<u32, ::std::time::Instant>> = ::std::cell::RefCell::new($crate::FastHalfBrownMap::default());
        }

        LAST_DEBUG.with(|last_debug| {
            let mut map = last_debug.borrow_mut();
            let now = ::std::time::Instant::now();
            let last = map.entry(line!()).or_insert(now - ::std::time::Duration::from_secs(1));

            if now.duration_since(*last) >= ::std::time::Duration::from_secs(1) {
                *last = now;
                ::tracing::warn!($($arg)*);
            }
        });
    }};
}

#[macro_export]
macro_rules! rl_info {
    ($($arg:tt)*) => {{

        use std::cell::RefCell;

        thread_local! {
            static LAST_INFO: RefCell<$crate::FastHalfBrownMap<u32, ::std::time::Instant>> = ::std::cell::RefCell::new($crate::FastHalfBrownMap::default());
        }

        LAST_INFO.with(|last_info| {
            let mut map = last_info.borrow_mut();
            let now = ::std::time::Instant::now();
            let last = map.entry(line!()).or_insert(now - ::std::time::Duration::from_secs(1));

            if now.duration_since(*last) >= ::std::time::Duration::from_secs(1) {
                *last = now;
                ::tracing::warn!($($arg)*);
            }
        });
    }};
}

#[macro_export]
macro_rules! rl_warn {
    ($($arg:tt)*) => {{

        use std::cell::RefCell;

        thread_local! {
            static LAST_WARN: RefCell<$crate::FastHalfBrownMap<u32, ::std::time::Instant>> = ::std::cell::RefCell::new($crate::FastHalfBrownMap::default());
        }

        LAST_WARN.with(|last_warn| {
            let mut map = last_warn.borrow_mut();
            let now = ::std::time::Instant::now();
            let last = map.entry(line!()).or_insert(now - ::std::time::Duration::from_secs(1));

            if now.duration_since(*last) >= ::std::time::Duration::from_secs(1) {
                *last = now;
                ::tracing::warn!($($arg)*);
            }
        });
    }};
}

#[macro_export]
macro_rules! rl_error {
    ($($arg:tt)*) => {{

        use std::cell::RefCell;

        thread_local! {
            static LAST_ERROR: RefCell<$crate::FastHalfBrownMap<u32, ::std::time::Instant>> = ::std::cell::RefCell::new($crate::FastHalfBrownMap::default());
        }

        LAST_ERROR.with(|last_error| {
            let mut map = last_error.borrow_mut();
            let now = ::std::time::Instant::now();
            let last = map.entry(line!()).or_insert(now - ::std::time::Duration::from_secs(1));

            if now.duration_since(*last) >= ::std::time::Duration::from_secs(1) {
                *last = now;
                ::tracing::warn!($($arg)*);
            }
        });
    }};
}
