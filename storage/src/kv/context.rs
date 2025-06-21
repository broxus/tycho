use std::sync::{Arc, Mutex};

use bytesize::ByteSize;

// TODO: Add potato mode.
#[derive(Clone)]
pub struct TableContext {
    caches: weedb::Caches,
    usage: Arc<Mutex<BufferUsage>>,
}

impl TableContext {
    pub const DEFAULT_LRU_CAPACITY: ByteSize = ByteSize::gib(1);

    pub fn caches(&self) -> &weedb::Caches {
        &self.caches
    }

    pub fn buffer_usage(&self) -> BufferUsage {
        *self.usage.lock().unwrap()
    }

    pub fn track_buffer_usage(&self, min: ByteSize, max: ByteSize) {
        assert!(min <= max);

        let mut usage = self.usage.lock().unwrap();
        usage.min_bytes = ByteSize(usage.min_bytes.0.saturating_add(min.0));
        usage.max_bytes = ByteSize(usage.max_bytes.0.saturating_add(max.0));
    }
}

impl AsRef<weedb::Caches> for TableContext {
    #[inline]
    fn as_ref(&self) -> &weedb::Caches {
        &self.caches
    }
}

impl Default for TableContext {
    fn default() -> Self {
        Self {
            caches: weedb::Caches::with_capacity(Self::DEFAULT_LRU_CAPACITY.0 as usize),
            usage: Default::default(),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferUsage {
    pub min_bytes: ByteSize,
    pub max_bytes: ByteSize,
}
