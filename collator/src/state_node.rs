use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent};
use futures_util::future::BoxFuture;
use tokio::sync::{broadcast, Mutex};

use tycho_block_util::block::BlockStuffAug;
use tycho_block_util::{block::BlockStuff, state::ShardStateStuff};
use tycho_core::block_strider::provider::{BlockProvider, OptionalBlockStuff};
use tycho_core::block_strider::subscriber::BlockSubscriber;
use tycho_storage::{BlockHandle, Storage};

use crate::tracing_targets;
use crate::types::BlockStuffForSync;

#[allow(private_bounds, private_interfaces)]
pub trait StateNodeAdapterBuilder<T>
where
    T: StateNodeAdapter,
{
    fn new(storage: Arc<Storage>) -> Self;
    fn build(self, listener: Arc<dyn StateNodeEventListener>) -> T;
}

pub struct StateNodeAdapterBuilderStdImpl {
    pub storage: Arc<Storage>,
}

impl StateNodeAdapterBuilder<StateNodeAdapterStdImpl> for StateNodeAdapterBuilderStdImpl {
    fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }
    #[allow(private_interfaces)]
    fn build(self, listener: Arc<dyn StateNodeEventListener>) -> StateNodeAdapterStdImpl {
        StateNodeAdapterStdImpl::create(listener, self.storage)
    }
}

#[async_trait]
pub trait StateNodeEventListener: Send + Sync {
    /// When our collated block was accepted and applied in state node
    async fn on_block_accepted(&self, block_id: &BlockId) -> Result<()>;
    /// When new applied block was received from blockchain
    async fn on_block_accepted_external(
        &self,
        block_id: &BlockId,
        state: Option<Arc<ShardStateStuff>>,
    ) -> Result<()>;
}

#[async_trait]
pub trait StateNodeAdapter: BlockProvider + Send + Sync + 'static {
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockId>;
    async fn load_state(&self, block_id: &BlockId) -> Result<Arc<ShardStateStuff>>;
    async fn load_block(&self, block_id: &BlockId) -> Result<Option<Arc<BlockStuff>>>;
    async fn load_block_handle(&self, block_id: &BlockId) -> Result<Option<Arc<BlockHandle>>>;
    async fn accept_block(&self, block: BlockStuffForSync) -> Result<()>;
}

pub struct StateNodeAdapterStdImpl {
    listener: Arc<dyn StateNodeEventListener>,
    blocks: Arc<Mutex<HashMap<ShardIdent, BTreeMap<u32, BlockStuffForSync>>>>,
    storage: Arc<Storage>,
    broadcaster: broadcast::Sender<BlockId>,
}

impl BlockProvider for StateNodeAdapterStdImpl {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        self.wait_for_block(prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        self.wait_for_block(block_id)
    }
}

impl StateNodeAdapterStdImpl {
    pub fn create(listener: Arc<dyn StateNodeEventListener>, storage: Arc<Storage>) -> Self {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "State node adapter created");
        let (broadcaster, _) = broadcast::channel(10000);
        Self {
            listener,
            storage,
            blocks: Default::default(),
            broadcaster,
        }
    }

    fn wait_for_block<'a>(
        &'a self,
        block_id: &'a BlockId,
    ) -> <StateNodeAdapterStdImpl as BlockProvider>::GetBlockFut<'a> {
        let mut receiver = self.broadcaster.subscribe();
        Box::pin(async move {
            loop {
                let blocks = self.blocks.lock().await;
                if let Some(shard_blocks) = blocks.get(&block_id.shard) {
                    if let Some(block) = shard_blocks.get(&block_id.seqno) {
                        return block
                            .block_stuff_aug
                            .as_ref()
                            .map(|block_stuff_aug| Ok(block_stuff_aug.clone()));
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
        })
    }
}

impl BlockSubscriber for StateNodeAdapterStdImpl {
    type HandleBlockFut = BoxFuture<'static, Result<()>>;

    fn handle_block(
        &self,
        block: &BlockStuffAug,
        state: Option<&Arc<ShardStateStuff>>,
    ) -> Self::HandleBlockFut {
        let block_id = *block.id();
        let shard = block_id.shard;
        let seqno = block_id.seqno;

        let blocks_lock = self.blocks.clone();
        let listener = self.listener.clone();
        let state = state.cloned();

        Box::pin(async move {
            let mut blocks_guard = blocks_lock.lock().await;
            let mut to_split = Vec::new();
            let mut to_remove = Vec::new();

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
                    listener.on_block_accepted(&block_id)
                } else {
                    listener.on_block_accepted_external(&block_id, state)
                }
            } else {
                listener.on_block_accepted_external(&block_id, state)
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
        })
    }
}

#[async_trait]
impl StateNodeAdapter for StateNodeAdapterStdImpl {
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockId> {
        let last_mc_block_id = self.storage.node_state().load_last_mc_block_id()?;
        Ok(last_mc_block_id)
    }

    async fn load_state(&self, block_id: &BlockId) -> Result<Arc<ShardStateStuff>> {
        let state = self
            .storage
            .shard_state_storage()
            .load_state(block_id)
            .await?;
        Ok(state)
    }

    async fn load_block(&self, block_id: &BlockId) -> Result<Option<Arc<BlockStuff>>> {
        let block_handle = self.storage.block_handle_storage().load_handle(block_id)?;
        if let Some(handle) = block_handle {
            let block_stuff = self
                .storage
                .block_storage()
                .load_block_data(handle.as_ref())
                .await
                .map(|block| Some(Arc::new(block)))?;
            return Ok(block_stuff);
        }
        Ok(None)
    }

    async fn load_block_handle(&self, block_id: &BlockId) -> Result<Option<Arc<BlockHandle>>> {
        let block_handle = self.storage.block_handle_storage().load_handle(block_id)?;
        Ok(block_handle)
    }

    async fn accept_block(&self, block: BlockStuffForSync) -> Result<()> {
        let mut blocks = self.blocks.lock().await;
        let block_id = match block.block_id.shard.is_masterchain() {
            true => {
                let prev_block_id = *block
                    .prev_blocks_ids
                    .last()
                    .ok_or(anyhow!("no prev block"))?;

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
}
