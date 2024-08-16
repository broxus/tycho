use std::sync::{Arc, Weak};

use everscale_types::models::*;
use tokio::sync::RwLock;
use tycho_util::FastDashMap;

use super::BlockMeta;

pub type BlockHandleCache = FastDashMap<BlockId, WeakBlockHandle>;

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
    pub fn new(id: &BlockId, meta: BlockMeta, cache: Arc<BlockHandleCache>) -> Self {
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

    #[inline]
    pub fn id(&self) -> &BlockId {
        &self.inner.id
    }

    #[inline]
    pub fn meta(&self) -> &BlockMeta {
        &self.inner.meta
    }

    #[inline]
    pub fn is_key_block(&self) -> bool {
        self.inner.meta.is_key_block() || self.inner.id.is_masterchain() && self.inner.id.seqno == 0
    }

    #[inline]
    pub fn block_data_lock(&self) -> &RwLock<()> {
        &self.inner.block_data_lock
    }

    #[inline]
    pub fn proof_data_lock(&self) -> &RwLock<()> {
        &self.inner.proof_data_block
    }

    #[inline]
    pub fn queue_diff_data_lock(&self) -> &RwLock<()> {
        &self.inner.queue_diff_data_lock
    }

    pub fn has_all_parts(&self) -> bool {
        // TODO: Load once?
        let meta = self.meta();
        meta.has_data() && meta.has_proof() && meta.has_queue_diff()
    }

    pub fn mc_ref_seqno(&self) -> u32 {
        if self.inner.id.shard.is_masterchain() {
            self.inner.id.seqno
        } else {
            self.inner.meta.masterchain_ref_seqno()
        }
    }

    pub fn set_mc_ref_seqno(&self, mc_seqno: u32) -> bool {
        match self.meta().set_mc_ref_seqno(mc_seqno) {
            0 => true,
            prev_seqno if prev_seqno == mc_seqno => false,
            _ => panic!("mc ref seqno already set"),
        }
    }

    pub fn downgrade(&self) -> WeakBlockHandle {
        WeakBlockHandle {
            inner: Arc::downgrade(&self.inner),
        }
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
    block_data_lock: RwLock<()>,
    proof_data_block: RwLock<()>,
    queue_diff_data_lock: RwLock<()>,
    cache: Arc<BlockHandleCache>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cache
            .remove_if(&self.id, |_, weak| weak.strong_count() == 0);
    }
}
