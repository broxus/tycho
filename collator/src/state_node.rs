use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent};
use tokio::sync::{broadcast, Mutex};

use tycho_block_util::block::BlockStuffAug;
use tycho_block_util::{block::BlockStuff, state::ShardStateStuff};
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
    /// Handle state after block was applied
    async fn handle_state(&self, state: &ShardStateStuff) -> Result<()>;
}

pub struct StateNodeAdapterStdImpl {
    listener: Arc<dyn StateNodeEventListener>,
    blocks: Arc<Mutex<HashMap<ShardIdent, BTreeMap<u32, BlockStuffForSync>>>>,
    blocks_mapping: Arc<Mutex<HashMap<BlockId, BlockId>>>,
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
            blocks_mapping: Arc::new(Default::default()),
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
        let block_id = match block.block_id.shard.is_masterchain() {
            true => {
                let prev_block_id = *block
                    .prev_blocks_ids
                    .last()
                    .ok_or(anyhow!("no prev block"))?;

                self.blocks_mapping
                    .lock()
                    .await
                    .insert(block.block_id, prev_block_id);

                blocks
                    .entry(block.block_id.shard)
                    .or_insert_with(BTreeMap::new)
                    .insert(prev_block_id.seqno, block);

                prev_block_id
            }
            false => {
                let block_id = block.block_id;
                blocks
                    .entry(block.block_id.shard)
                    .or_insert_with(BTreeMap::new)
                    .insert(block.block_id.seqno, block);
                block_id
            }
        };
        let broadcast_result = self.broadcaster.send(block_id).ok();
        tracing::trace!(target: tracing_targets::STATE_NODE_ADAPTER, "Block broadcast_result: {:?}", broadcast_result);
        Ok(())
    }

    async fn wait_for_block(&self, block_id: &BlockId) -> Option<Result<BlockStuffAug>> {
        let mut receiver = self.broadcaster.subscribe();
        loop {
            let blocks = self.blocks.lock().await;
            if let Some(shard_blocks) = blocks.get(&block_id.shard) {
                if let Some(block) = shard_blocks.get(&block_id.seqno) {
                    return Some(Ok(block.block_stuff_aug.clone()));
                }
            }
            drop(blocks);

            loop {
                match receiver.recv().await {
                    Ok(received_block_id) if received_block_id == *block_id => {
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

    async fn handle_state(&self, state: &ShardStateStuff) -> Result<()> {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Handle block: {:?}", state.block_id());
        let block_id = *state.block_id();

        let mut to_split = Vec::new();
        let mut to_remove = Vec::new();

        let mut block_mapping_guard = self.blocks_mapping.lock().await;
        let block_id = match block_mapping_guard.remove(&block_id) {
            None => block_id,
            Some(some) => some.clone(),
        };

        let shard = block_id.shard;
        let seqno = block_id.seqno;

        let mut blocks_guard = self.blocks.lock().await;

        let result_future = if let Some(shard_blocks) = blocks_guard.get(&shard) {
            if let Some(block_data) = shard_blocks.get(&seqno) {
                if shard.is_masterchain() {
                    let prev_seqno = block_data
                        .prev_blocks_ids
                        .last()
                        .ok_or(anyhow!("no prev block"))?
                        .seqno;
                    for id in &block_data.top_shard_blocks_ids {
                        to_split.push((id.shard, id.seqno));
                        to_remove.push((id.shard, id.seqno));
                    }
                    to_split.push((shard, prev_seqno));
                    to_remove.push((shard, prev_seqno));
                } else {
                    to_remove.push((shard, seqno));
                }
                tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Block accepted: {:?}", block_id);
                self.listener.on_block_accepted(&block_id)
            } else {
                tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Block accepted external: {:?}", block_id);
                self.listener.on_block_accepted_external(state)
            }
        } else {
            tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Block accepted external: {:?}", block_id);
            self.listener.on_block_accepted_external(state)
        };

        for (shard, seqno) in &to_split {
            if let Some(shard_blocks) = blocks_guard.get_mut(shard) {
                shard_blocks.split_off(seqno);
            }
        }

        for (shard, seqno) in &to_remove {
            if let Some(shard_blocks) = blocks_guard.get_mut(shard) {
                shard_blocks.remove(seqno);
            }
        }

        drop(blocks_guard);

        result_future.await?;

        Ok(())
    }
}
