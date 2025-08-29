mod state_update_context;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use tycho_network::PeerId;
use tycho_types::models::*;
use tycho_types::prelude::*;

pub use self::impls::*;
pub use self::state_update_context::*;
use crate::types::processed_upto::BlockSeqno;

mod impls {
    pub use self::single_node_impl::MempoolAdapterSingleNodeImpl;
    pub use self::std_impl::MempoolAdapterStdImpl;
    pub use self::stub_impl::MempoolAdapterStubImpl;
    #[cfg(test)]
    pub(crate) use self::stub_impl::{make_stub_anchor, make_stub_external};

    mod common;
    mod single_node_impl;
    mod std_impl;
    mod stub_impl;
}

// === Factory ===

pub trait MempoolAdapterFactory {
    fn create(&self, listener: Arc<dyn MempoolEventListener>) -> Arc<dyn MempoolAdapter>;
}

impl<F, R> MempoolAdapterFactory for F
where
    F: Fn(Arc<dyn MempoolEventListener>) -> Arc<R>,
    R: MempoolAdapter,
{
    fn create(&self, listener: Arc<dyn MempoolEventListener>) -> Arc<dyn MempoolAdapter> {
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
    /// Process updates related to master block:
    /// 1. Mempool switch round
    /// 2. Mempool config
    /// 3. Validators sets
    async fn handle_mc_state_update(&self, cx: StateUpdateContext) -> Result<()>;

    /// Process state update reported by collation manager earlier.
    /// Will apply vset and config changes to mempool. Also starts mempool at first call.
    /// Advances mempool pause bound which allows mempool to resume its work.
    /// Mempool should be ready to return mc block `processed_up_to` anchor and all next after it.
    /// This method will not clean anchor cache.
    async fn handle_signed_mc_block(&self, mc_block_seqno: BlockSeqno) -> Result<()>;

    /// Request, await, and return anchor from connected mempool by id.
    /// Return None if the requested anchor does not exist and cannot be synced from other nodes.
    async fn get_anchor_by_id(&self, anchor_id: MempoolAnchorId) -> Result<GetAnchorResult>;

    /// Request, await, and return the next anchor after the specified previous one.
    /// If anchor does not exist then await until it be produced or downloaded during sync.
    /// Return None if anchor cannot be produced or synced from other nodes.
    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<GetAnchorResult>;

    /// Clean cache from all anchors that before specified.
    /// We can do this for anchors that processed in blocks
    /// which included in signed master - we do not need them anymore
    fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()>;

    /// Enqueue external message to be consumed and processed by mempool
    fn accept_external(&self, message: Bytes);

    /// **Warning:** changes from `GlobalConfig` may be rewritten by applied mc state
    /// only if applied mc state has greater time and GEQ round
    async fn update_delayed_config(
        &self,
        consensus_config: Option<&ConsensusConfig>,
        genesis_info: &GenesisInfo,
    ) -> Result<()>;
}

impl MempoolAdapterFactory for Arc<dyn MempoolAdapter> {
    fn create(&self, _listener: Arc<dyn MempoolEventListener>) -> Arc<dyn MempoolAdapter> {
        self.clone()
    }
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
    // None for first after Genesis
    pub prev_id: Option<MempoolAnchorId>,
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

pub enum GetAnchorResult {
    NotExist,
    Exist(Arc<MempoolAnchor>),
}

impl GetAnchorResult {
    pub fn anchor(&self) -> Option<&MempoolAnchor> {
        match self {
            Self::Exist(arc) => Some(arc),
            Self::NotExist => None,
        }
    }
}
