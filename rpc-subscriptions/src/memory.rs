use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytesize::ByteSize;

#[derive(Debug, Default, Clone)]
pub(crate) struct MemCounter(Arc<AtomicU64>);

impl MemCounter {
    #[inline]
    pub(crate) fn load(&self) -> ByteSize {
        ByteSize::b(self.0.load(Ordering::Relaxed))
    }

    #[inline]
    pub(crate) fn add_bytes(&self, bytes: usize) {
        self.0.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn sub_bytes(&self, bytes: usize) {
        self.0.fetch_sub(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemorySnapshot {
    pub addr_to_subs: ByteSize,
    pub subs_clients: ByteSize,
    pub int_addr_to_addr: ByteSize,
    pub client_to_int_addrs: ByteSize,
    pub clients: ByteSize,
    pub free_lists: ByteSize,
    pub total: ByteSize,
}

#[derive(Debug, Default)]
pub(crate) struct MemoryTracker {
    pub(crate) subs_clients: MemCounter,
    pub(crate) client_to_int_addrs: MemCounter,
    pub(crate) free_lists: MemCounter,
}
