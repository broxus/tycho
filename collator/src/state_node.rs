use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};
use tokio::sync::{broadcast, Mutex};
use tycho_block_util::block::{BlockStuff, BlockStuffAug};
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::{BlockHandle, Storage};

use crate::tracing_targets;
use crate::types::BlockStuffForSync;

// FACTORY

pub trait StateNodeAdapterFactory {
    type Adapter: StateNodeAdapter;

    fn create(&self, listener: Arc<dyn StateNodeEventListener>) -> Self::Adapter;
}

impl<F, R> StateNodeAdapterFactory for F
where
    F: Fn(Arc<dyn StateNodeEventListener>) -> R,
    R: StateNodeAdapter,
{
    type Adapter = R;

    fn create(&self, listener: Arc<dyn StateNodeEventListener>) -> Self::Adapter {
        self(listener)
    }
}

#[async_trait]
pub trait StateNodeEventListener: Send + Sync {
    /// When our collated block was accepted and applied in state node
    async fn on_block_accepted(&self, block_id: &BlockId) -> Result<()>;
    /// When new applied block was received from blockchain
    async fn on_block_accepted_external(&self, state: &ShardStateStuff) -> Result<()>;
}

#[async_trait]
pub trait StateNodeAdapter: Send + Sync + 'static {
    /// Return id of last master block that was applied to node local state
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockId>;
    /// Return master or shard state on specified block from node local state
    async fn load_state(&self, block_id: &BlockId) -> Result<ShardStateStuff>;
    /// Return block by it's id from node local state
    async fn load_block(&self, block_id: &BlockId) -> Result<Option<BlockStuff>>;
    /// Return block handle by it's id from node local state
    async fn load_block_handle(&self, block_id: &BlockId) -> Result<Option<BlockHandle>>;
    /// Accept block:
    /// 1. (TODO) Broadcast block to blockchain network
    /// 2. Provide block to the block strider
    async fn accept_block(&self, block: BlockStuffForSync) -> Result<()>;
    /// Waits for the specified block to be received and returns it
    async fn wait_for_block(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>>;
    /// Waits for the specified block by prev_id to be received and returns it
    async fn wait_for_block_next(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>>;
    /// Handle state after block was applied
    async fn handle_state(&self, state: &ShardStateStuff) -> Result<()>;
}

pub struct StateNodeAdapterStdImpl {
    listener: Arc<dyn StateNodeEventListener>,
    blocks: Arc<Mutex<HashMap<ShardIdent, BTreeMap<u32, BlockStuffForSync>>>>,
    storage: Storage,
    broadcaster: broadcast::Sender<BlockId>,
}

impl StateNodeAdapterStdImpl {
    pub fn new(listener: Arc<dyn StateNodeEventListener>, storage: Storage) -> Self {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "State node adapter created");

        let (broadcaster, _) = broadcast::channel(10000);
        Self {
            listener,
            storage,
            blocks: Default::default(),
            broadcaster,
        }
    }
}

#[async_trait]
impl StateNodeAdapter for StateNodeAdapterStdImpl {
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockId> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Load last applied mc block id");
        self.storage
            .node_state()
            .load_last_mc_block_id()
            .context("no blocks applied yet")
    }

    async fn load_state(&self, block_id: &BlockId) -> Result<ShardStateStuff> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Load state: {:?}", block_id);
        let state = self
            .storage
            .shard_state_storage()
            .load_state(block_id)
            .await?;
        Ok(state)
    }

    async fn load_block(&self, block_id: &BlockId) -> Result<Option<BlockStuff>> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Load block: {:?}", block_id);

        let handle_storage = self.storage.block_handle_storage();
        let block_storage = self.storage.block_storage();

        let Some(handle) = handle_storage.load_handle(block_id) else {
            return Ok(None);
        };
        block_storage.load_block_data(&handle).await.map(Some)
    }

    async fn load_block_handle(&self, block_id: &BlockId) -> Result<Option<BlockHandle>> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Load block handle: {:?}", block_id);
        Ok(self.storage.block_handle_storage().load_handle(block_id))
    }

    async fn accept_block(&self, block: BlockStuffForSync) -> Result<()> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Block accepted: {:?}", block.block_id);
        let mut blocks = self.blocks.lock().await;
        let block_id = block.block_id;
        blocks
            .entry(block.block_id.shard)
            .or_insert_with(BTreeMap::new)
            .insert(block.block_id.seqno, block);

        let broadcast_result = self.broadcaster.send(block_id).ok();
        tracing::trace!(target: tracing_targets::STATE_NODE_ADAPTER, "Block broadcast_result: {:?}", broadcast_result);
        Ok(())
    }

    async fn wait_for_block(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>> {
        let block_id = BlockIdToWait::Full(block_id);
        self.wait_for_block_ext(block_id).await
    }

    async fn wait_for_block_next(&self, prev_block_id: &BlockId) -> Option<Result<BlockStuffAug>> {
        let next_block_id_short =
            BlockIdShort::from((prev_block_id.shard, prev_block_id.seqno + 1));
        let block_id = BlockIdToWait::Short(&next_block_id_short);
        self.wait_for_block_ext(block_id).await
    }

    async fn handle_state(&self, state: &ShardStateStuff) -> Result<()> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Handle block: {:?}", state.block_id());
        let block_id = *state.block_id();

        let mut to_split = Vec::new();

        let shard = block_id.shard;
        let seqno = block_id.seqno;

        {
            let blocks_guard = self.blocks.lock().await;
            if let Some(shard_blocks) = blocks_guard.get(&shard) {
                let block = shard_blocks.get(&seqno);

                if shard.is_masterchain() {
                    let prev_mc_block = shard_blocks
                        .range(..=seqno)
                        .rev()
                        .find_map(|(&key, value)| if key < seqno { Some(value) } else { None });

                    if let Some(prev_mc_block) = prev_mc_block {
                        for id in &prev_mc_block.top_shard_blocks_ids {
                            to_split.push((id.shard, id.seqno + 1));
                        }
                        to_split.push((shard, prev_mc_block.block_id.seqno + 1));
                    }
                }

                match block {
                    None => {
                        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Block handled external: {:?}", block_id);
                        self.listener.on_block_accepted_external(state).await?;
                    }
                    Some(block) => {
                        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Block handled: {:?}", block_id);
                        self.listener.on_block_accepted(&block.block_id).await?;
                    }
                }
            } else {
                tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Block handled external. Shard ID not found in blocks buffer: {:?}", block_id);
                self.listener.on_block_accepted_external(state).await?;
            }
        }

        {
            let mut blocks_guard = self.blocks.lock().await;
            for (shard, seqno) in &to_split {
                if let Some(shard_blocks) = blocks_guard.get_mut(shard) {
                    *shard_blocks = shard_blocks.split_off(seqno);
                }
            }
        }

        Ok(())
    }
}

impl StateNodeAdapterStdImpl {
    async fn wait_for_block_ext(
        &self,
        block_id: BlockIdToWait<'_>,
    ) -> Option<Result<BlockStuffAug>> {
        let mut receiver = self.broadcaster.subscribe();
        loop {
            let blocks = self.blocks.lock().await;
            if let Some(shard_blocks) = blocks.get(&block_id.shard()) {
                if let Some(block) = shard_blocks.get(&block_id.seqno()) {
                    return Some(Ok(block.block_stuff_aug.clone()));
                }
            }
            drop(blocks);

            loop {
                match receiver.recv().await {
                    Ok(received_block_id) if block_id == received_block_id => {
                        break;
                    }
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!(target: tracing_targets::STATE_NODE_ADAPTER, "Broadcast channel lagged: {}", count);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!(target: tracing_targets::STATE_NODE_ADAPTER, "Broadcast channel closed");
                        return None;
                    }
                }
            }
        }
    }
}

enum BlockIdToWait<'a> {
    Short(&'a BlockIdShort),
    Full(&'a BlockId),
}

impl BlockIdToWait<'_> {
    fn shard(&self) -> ShardIdent {
        match self {
            Self::Short(id) => id.shard,
            Self::Full(id) => id.shard,
        }
    }

    fn seqno(&self) -> u32 {
        match self {
            Self::Short(id) => id.seqno,
            Self::Full(id) => id.seqno,
        }
    }
}

impl PartialEq<BlockId> for BlockIdToWait<'_> {
    fn eq(&self, other: &BlockId) -> bool {
        match *self {
            BlockIdToWait::Short(short) => &other.as_short_id() == short,
            BlockIdToWait::Full(full) => full == other,
        }
    }
}
