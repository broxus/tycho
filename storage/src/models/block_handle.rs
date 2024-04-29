use std::sync::{Arc, Weak};

use everscale_types::models::*;
use tokio::sync::RwLock;

use super::BlockMeta;
use tycho_util::FastDashMap;

#[derive(Clone)]
#[repr(transparent)]
pub struct BlockHandle {
    inner: Arc<Inner>,
}

impl BlockHandle {
    pub fn new(
        id: BlockId,
        meta: BlockMeta,
        cache: Arc<FastDashMap<BlockId, Weak<BlockHandle>>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                id,
                meta,
                block_data_lock: Default::default(),
                proof_data_block: Default::default(),
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
        self.inner.meta.is_key_block() || self.inner.id.seqno == 0
    }

    #[inline]
    pub fn block_data_lock(&self) -> &RwLock<()> {
        &self.inner.block_data_lock
    }

    #[inline]
    pub fn proof_data_lock(&self) -> &RwLock<()> {
        &self.inner.proof_data_block
    }

    pub fn has_proof_or_link(&self, is_link: &mut bool) -> bool {
        *is_link = !self.inner.id.shard.is_masterchain();
        if *is_link {
            self.inner.meta.has_proof_link()
        } else {
            self.inner.meta.has_proof()
        }
    }

    pub fn masterchain_ref_seqno(&self) -> u32 {
        if self.inner.id.shard.is_masterchain() {
            self.inner.id.seqno
        } else {
            self.inner.meta.masterchain_ref_seqno()
        }
    }
}

impl Drop for BlockHandle {
    fn drop(&mut self) {
        self.inner
            .cache
            .remove_if(&self.inner.id, |_, weak| weak.strong_count() == 0);
    }
}

struct Inner {
    id: BlockId,
    meta: BlockMeta,
    block_data_lock: RwLock<()>,
    proof_data_block: RwLock<()>,
    cache: Arc<FastDashMap<BlockId, Weak<BlockHandle>>>,
}
