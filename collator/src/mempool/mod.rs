use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_types::models::*;
use everscale_types::prelude::*;
use tycho_network::PeerId;

pub use self::impls::*;

mod impls {
    pub use self::std_impl::MempoolAdapterStdImpl;
    #[cfg(test)]
    pub(crate) use self::stub_impl::make_stub_anchor;
    pub use self::stub_impl::MempoolAdapterStubImpl;

    mod std_impl;
    mod stub_impl;
}

// === Factory ===

pub trait MempoolAdapterFactory {
    type Adapter: MempoolAdapter;

    fn create(&self, listener: Arc<dyn MempoolEventListener>) -> Arc<Self::Adapter>;
}

impl<F, R> MempoolAdapterFactory for F
where
    F: Fn(Arc<dyn MempoolEventListener>) -> Arc<R>,
    R: MempoolAdapter,
{
    type Adapter = R;

    fn create(&self, listener: Arc<dyn MempoolEventListener>) -> Arc<Self::Adapter> {
        self(listener)
    }
}

// === Events Listener ===

#[async_trait]
pub trait MempoolEventListener: Send + Sync {
    /// Process new anchor from mempool
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()>;
}

// === Adapter ===

#[async_trait]
pub trait MempoolAdapter: Send + Sync + 'static {
    /// Schedule task to process new master block state (may perform gc or nodes rotation).
    ///
    /// TODO: Replace `mc_block_id` with some kind of context if more data is needed.
    async fn on_new_mc_state(&self, mc_block_id: &BlockId) -> Result<()>;

    /// Request, await, and return anchor from connected mempool by id.
    /// Return None if the requested anchor does not exist and cannot be synced from other nodes.
    async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>>;

    /// Request, await, and return the next anchor after the specified previous one.
    /// If anchor does not exist then await until it be produced or downloaded during sync.
    /// Return None if anchor cannot be produced or synced from other nodes.
    async fn get_next_anchor(
        &self,
        prev_anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>>;

    /// Process top processed to anchor reported by collation manager.
    /// Will manage mempool sync depth.
    /// Mempool should be ready to return this anchor and all next after it.
    async fn handle_top_processed_to_anchor(&self, anchor_id: u32) -> Result<()>;

    /// Clean cache from all anchors that before specified.
    /// We can do this for anchors that processed in blocks
    /// which included in signed master - we do not need them anymore
    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()>;
}

// === Types ===

pub type MempoolAnchorId = u32;

#[derive(Debug)]
pub struct ExternalMessage {
    pub cell: Cell,
    pub info: ExtInMsgInfo,
}

impl ExternalMessage {
    pub fn hash(&self) -> &HashBytes {
        self.cell.repr_hash()
    }
}

#[derive(Debug)]
pub struct MempoolAnchor {
    pub id: MempoolAnchorId,
    pub author: PeerId,
    pub chain_time: u64,
    pub externals: Vec<Arc<ExternalMessage>>,
}

impl MempoolAnchor {
    pub fn count_externals_for(&self, shard_id: &ShardIdent, offset: usize) -> usize {
        self.externals
            .iter()
            .skip(offset)
            .filter(|ext| shard_id.contains_address(&ext.info.dst))
            .count()
    }

    pub fn has_externals_for(&self, shard_id: &ShardIdent, offset: usize) -> bool {
        self.externals
            .iter()
            .skip(offset)
            .any(|ext| shard_id.contains_address(&ext.info.dst))
    }

    pub fn iter_externals(
        &self,
        from_idx: usize,
    ) -> impl Iterator<Item = Arc<ExternalMessage>> + '_ {
        self.externals.iter().skip(from_idx).cloned()
    }
}
