use std::sync::{Arc, Weak};

use everscale_types::models::*;
use tokio::sync::{Semaphore, SemaphorePermit};

use super::{BlockFlags, BlockHandleCache, BlockMeta};

#[derive(Clone)]
#[repr(transparent)]
pub struct WeakBlockHandle {
    inner: Weak<Inner>,
}

impl WeakBlockHandle {
    pub fn strong_count(&self) -> usize {
        self.inner.strong_count()
    }

    pub fn upgrade(&self) -> Option<BlockHandle> {
        self.inner.upgrade().map(|inner| BlockHandle { inner })
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct BlockHandle {
    inner: Arc<Inner>,
}

impl BlockHandle {
    pub(crate) fn new(id: &BlockId, meta: BlockMeta, cache: Arc<BlockHandleCache>) -> Self {
        Self {
            inner: Arc::new(Inner {
                id: *id,
                meta,
                block_data_lock: Default::default(),
                proof_data_block: Default::default(),
                queue_diff_data_lock: Default::default(),
                cache,
            }),
        }
    }

    pub fn downgrade(&self) -> WeakBlockHandle {
        WeakBlockHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub fn id(&self) -> &BlockId {
        &self.inner.id
    }

    pub fn is_masterchain(&self) -> bool {
        self.inner.id.is_masterchain()
    }

    pub fn meta(&self) -> &BlockMeta {
        &self.inner.meta
    }

    pub fn gen_utime(&self) -> u32 {
        self.inner.meta.gen_utime()
    }

    pub fn is_key_block(&self) -> bool {
        self.inner.meta.flags().contains(BlockFlags::IS_KEY_BLOCK)
            || self.inner.id.is_masterchain() && self.inner.id.seqno == 0
    }

    pub fn is_applied(&self) -> bool {
        self.inner.meta.flags().contains(BlockFlags::IS_APPLIED)
    }

    pub fn is_persistent(&self) -> bool {
        self.inner.meta.flags().contains(BlockFlags::IS_PERSISTENT) || self.inner.id.seqno == 0
    }

    pub fn has_data(&self) -> bool {
        const MASK: u32 = BlockFlags::HAS_DATA.bits() | BlockFlags::IS_REMOVED.bits();
        let flags = self.inner.meta.flags();
        flags.bits() & MASK == BlockFlags::HAS_DATA.bits()
    }

    pub fn has_proof(&self) -> bool {
        const MASK: u32 = BlockFlags::HAS_PROOF.bits() | BlockFlags::IS_REMOVED.bits();
        let flags = self.inner.meta.flags();
        flags.bits() & MASK == BlockFlags::HAS_PROOF.bits()
    }

    pub fn has_queue_diff(&self) -> bool {
        const MASK: u32 = BlockFlags::HAS_QUEUE_DIFF.bits() | BlockFlags::IS_REMOVED.bits();
        let flags = self.inner.meta.flags();
        flags.bits() & MASK == BlockFlags::HAS_QUEUE_DIFF.bits()
    }

    pub fn has_all_block_parts(&self) -> bool {
        const MASK: u32 = BlockFlags::HAS_ALL_BLOCK_PARTS.bits() | BlockFlags::IS_REMOVED.bits();
        let flags = self.inner.meta.flags();
        flags.bits() & MASK == BlockFlags::HAS_ALL_BLOCK_PARTS.bits()
    }

    pub fn has_next1(&self) -> bool {
        self.inner.meta.flags().contains(BlockFlags::HAS_NEXT_1)
    }

    pub fn has_state(&self) -> bool {
        self.inner.meta.flags().contains(BlockFlags::HAS_STATE)
    }

    pub fn has_persistent_shard_state(&self) -> bool {
        self.inner
            .meta
            .flags()
            .contains(BlockFlags::HAS_PERSISTENT_SHARD_STATE)
    }

    pub fn has_persistent_queue_state(&self) -> bool {
        self.inner
            .meta
            .flags()
            .contains(BlockFlags::HAS_PERSISTENT_QUEUE_STATE)
    }

    pub fn mc_ref_seqno(&self) -> u32 {
        if self.inner.id.shard.is_masterchain() {
            self.inner.id.seqno
        } else {
            self.inner.meta.ref_by_mc_seqno()
        }
    }

    pub(crate) fn block_data_lock(&self) -> &BlockDataLock {
        &self.inner.block_data_lock
    }

    pub(crate) fn proof_data_lock(&self) -> &BlockDataLock {
        &self.inner.proof_data_block
    }

    pub(crate) fn queue_diff_data_lock(&self) -> &BlockDataLock {
        &self.inner.queue_diff_data_lock
    }
}

unsafe impl arc_swap::RefCnt for BlockHandle {
    type Base = Inner;

    fn into_ptr(me: Self) -> *mut Self::Base {
        arc_swap::RefCnt::into_ptr(me.inner)
    }

    fn as_ptr(me: &Self) -> *mut Self::Base {
        arc_swap::RefCnt::as_ptr(&me.inner)
    }

    unsafe fn from_ptr(ptr: *const Self::Base) -> Self {
        Self {
            inner: arc_swap::RefCnt::from_ptr(ptr),
        }
    }
}

#[doc(hidden)]
pub struct Inner {
    id: BlockId,
    meta: BlockMeta,
    block_data_lock: BlockDataLock,
    proof_data_block: BlockDataLock,
    queue_diff_data_lock: BlockDataLock,
    cache: Arc<BlockHandleCache>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cache
            .remove_if(&self.id, |_, weak| weak.strong_count() == 0);
    }
}

pub(crate) struct BlockDataLock {
    semaphore: Semaphore,
}

impl BlockDataLock {
    const fn new() -> Self {
        Self {
            semaphore: Semaphore::const_new(MAX_READS as usize),
        }
    }

    pub async fn read(&self) -> BlockDataGuard<'_> {
        BlockDataGuard(self.semaphore.acquire().await.unwrap_or_else(|_| {
            // The semaphore was closed. but, we never explicitly close it.
            unreachable!()
        }))
    }

    pub async fn write(&self) -> BlockDataGuard<'_> {
        BlockDataGuard(
            self.semaphore
                .acquire_many(MAX_READS)
                .await
                .unwrap_or_else(|_| {
                    // The semaphore was closed. but, we never explicitly close it.
                    unreachable!()
                }),
        )
    }
}

impl Default for BlockDataLock {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct BlockDataGuard<'a>(#[allow(unused)] SemaphorePermit<'a>);

const MAX_READS: u32 = u32::MAX >> 3;
