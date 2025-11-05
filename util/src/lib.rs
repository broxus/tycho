#[doc(hidden)]
#[allow(dead_code)]
mod __async_profile_guard__ {
    use std::time::{Duration, Instant};
    const THRESHOLD_MS: u64 = 10u64;
    pub struct Guard {
        name: &'static str,
        file: &'static str,
        from_line: u32,
        current_start: Option<Instant>,
        consecutive_hits: u32,
    }
    impl Guard {
        pub fn new(name: &'static str, file: &'static str, line: u32) -> Self {
            Guard {
                name,
                file,
                from_line: line,
                current_start: Some(Instant::now()),
                consecutive_hits: 0,
            }
        }
        pub fn checkpoint(&mut self, new_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{from}-{to}", file = self.file, from = self.from_line, to
                        = new_line
                    );
                    let wraparound = new_line < self.from_line;
                    if wraparound {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (iteration tail wraparound)"
                        );
                    } else {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (iteration tail)"
                        );
                    }
                } else {
                    self.consecutive_hits = 0;
                }
            }
            self.from_line = new_line;
            self.current_start = Some(Instant::now());
        }
        pub fn end_section(&mut self, to_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{from}-{to}", file = self.file, from = self.from_line, to
                        = to_line
                    );
                    let wraparound = to_line < self.from_line;
                    if wraparound {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (loop wraparound)"
                        );
                    } else {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll"
                        );
                    }
                } else {
                    self.consecutive_hits = 0;
                }
            }
        }
        pub fn start_section(&mut self, new_line: u32) {
            self.from_line = new_line;
            self.current_start = Some(Instant::now());
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            if let Some(start) = self.current_start {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{line}-{line}", file = self.file, line = self.from_line
                    );
                    tracing::warn!(
                        elapsed_ms = elapsed.as_millis(), name = % self.name, span = %
                        span, hits = self.consecutive_hits, wraparound = false,
                        "long poll"
                    );
                }
            }
        }
    }
}
#[allow(unused_extern_crates)]
extern crate self as tycho_util;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Command;
pub mod compression;
pub mod config;
pub mod io;
pub mod progress_bar;
pub mod serde_helpers;
pub mod time;
pub mod tl;
pub mod futures {
    pub use self::await_blocking::AwaitBlocking;
    pub use self::box_future_or_noop::BoxFutureOrNoop;
    pub use self::join_task::JoinTask;
    pub use self::shared::{Shared, WeakShared, WeakSharedHandle};
    mod await_blocking;
    mod box_future_or_noop;
    mod join_task;
    mod shared;
}
pub mod mem {
    pub use self::reclaimer::{Reclaimer, ReclaimerError};
    pub use self::slicer::{
        AllocatedMemoryConstraints, MemoryConstraint, MemoryConstraints, MemorySlicer,
        MemorySlicerGuard, MemorySlicerRange,
    };
    mod reclaimer;
    mod slicer;
}
pub mod num {
    pub use self::median::{StreamingUnsignedMedian, VecOfStreamingUnsignedMedian};
    pub use self::safe_avg::{SafeSignedAvg, SafeUnsignedAvg, SafeUnsignedVecAvg};
    mod median;
    mod safe_avg;
}
pub mod sync {
    pub use self::once_take::*;
    pub use self::priority_semaphore::{AcquireError, PrioritySemaphore, TryAcquireError};
    pub use self::rayon::{rayon_run, rayon_run_fifo};
    pub use self::task::{CancellationFlag, DebounceCancellationFlag, yield_on_complex};
    mod once_take;
    mod priority_semaphore;
    mod rayon;
    mod task;
}
#[cfg(any(test, feature = "test"))]
pub mod test {
    pub use self::logger::init_logger;
    mod logger;
}
pub mod metrics {
    pub use self::gauge_guard::GaugeGuard;
    pub use self::histogram_guard::{HistogramGuard, HistogramGuardWithLabels};
    pub use self::metrics_loop::spawn_metrics_loop;
    mod gauge_guard;
    mod histogram_guard;
    mod metrics_loop;
}
mod util {
    pub(crate) mod linked_list;
    pub(crate) mod wake_list;
}
#[cfg(feature = "cli")]
pub mod cli;
pub use dashmap::mapref::entry::Entry as DashMapEntry;
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
    (
        $value:expr, { $target_variant:pat => Box::new($extracted:ident) => $target:expr,
        $other_variant:pat => $other:expr, }
    ) => {
        { let value : ::std::boxed::Box < _ > = $value; match
        ::core::convert::AsRef::as_ref(& value) { #[allow(unused_variables)]
        $target_variant => { let $extracted = unsafe {
        $crate::__internal::realloc_box(value, | value | match value { $target_variant =>
        $extracted, _ => unreachable!(), }) }; $target } $other_variant => $other, } }
    };
}
#[doc(hidden)]
pub mod __internal {
    pub use serde;
    /// # Safety
    /// The following must be true:
    /// - `T` must have the same layout as `R`
    /// - `f` must not panic
    pub unsafe fn realloc_box<T, F, R>(value: Box<T>, f: F) -> Box<R>
    where
        F: FnOnce(T) -> R,
    {
        unsafe {
            assert!(std::mem::align_of::< T > () == std::mem::align_of::< R > ());
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
            std::ptr::write(ptr, f(value));
            Box::from_raw(ptr)
        }
    }
}
pub fn project_root() -> Result<PathBuf, std::io::Error> {
    let project_root = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .output()?
        .stdout;
    let project_root = PathBuf::from(
        String::from_utf8(project_root)
            .map_err(|e| std::io::Error::other(format!("invalid project root: {e}")))?
            .trim(),
    );
    Ok(project_root)
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
            realloc_box_enum!(
                value, { Value::One(value) => Box::new(value) => Some(value), _ => None,
                }
            )
        }
        let value = BigValue1([123; 10]);
        let one = convert_to_one(Box::new(Value::One(value.clone())));
        assert_eq!(one.unwrap().0, value.0);
    }
}
