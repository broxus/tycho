use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent};
use futures_util::future::BoxFuture;
use tokio::sync::{Mutex, Notify};

use tycho_block_util::{block::BlockStuff, state::ShardStateStuff};
use tycho_core::block_strider::provider::{BlockProvider, OptionalBlockStuff};
use tycho_core::block_strider::subscriber::BlockSubscriber;
use tycho_storage::Storage;

use crate::tracing_targets;
use crate::types::BlockStuffForSync;

// BUILDER

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

// EVENTS LISTENER

#[async_trait]
pub(crate) trait StateNodeEventListener: Send + Sync {
    /// When new masterchain block received from blockchain
    async fn on_mc_block(&self, mc_block_id: BlockId) -> Result<()>;
    async fn on_block_accepted(&self, block_id: &BlockId);
    async fn on_block_accepted_external(&self, block_id: &BlockId);
}

#[async_trait]
pub(crate) trait StateNodeAdapter: BlockProvider + Send + Sync + 'static {
    async fn load_last_applied_mc_block_id(&self) -> Result<BlockId>;
    async fn load_state(&self, block_id: BlockId) -> Result<Arc<ShardStateStuff>>;
    async fn load_block(&self, block_id: BlockId) -> Result<Option<Arc<BlockStuff>>>;
    async fn accept_block(&self, block: BlockStuffForSync) -> Result<()>;
}

pub struct StateNodeAdapterStdImpl {
    listener: Arc<dyn StateNodeEventListener>,
    blocks: Arc<Mutex<HashMap<ShardIdent, BTreeMap<u32, BlockStuffForSync>>>>,
    notifier: Arc<Notify>,
    storage: Arc<Storage>,
}

impl BlockProvider for StateNodeAdapterStdImpl {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(async move {
            loop {
                let blocks = self.blocks.lock().await;
                if let Some(mc_shard) = blocks.get(&prev_block_id.shard) {
                    if let Some(block) = mc_shard.get(&prev_block_id.seqno) {
                        for id in block.top_shard_blocks_ids.iter() {
                            if let Some(shard_blocks) = blocks.get(&id.shard) {
                                if let Some(block) = shard_blocks.get(&id.seqno) {
                                    return Some(
                                        block.block_stuff.clone().ok_or(anyhow!("no block stuff")),
                                    );
                                }
                            }
                        }
                    }
                }
                drop(blocks);
                self.notifier.notified().await;
            }
        })
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(async move {
            loop {
                let blocks = self.blocks.lock().await;
                if let Some(sc_shard) = blocks.get(&block_id.shard) {
                    if let Some(block) = sc_shard.get(&block_id.seqno) {
                        return Some(block.block_stuff.clone().ok_or(anyhow!("no block stuff")));
                    }
                }
                drop(blocks);
                self.notifier.notified().await;
            }
        })
    }
}

impl StateNodeAdapterStdImpl {
    fn create(listener: Arc<dyn StateNodeEventListener>, storage: Arc<Storage>) -> Self {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "State node adapter created");
        Self {
            listener,
            storage,
            blocks: Default::default(),
            notifier: Arc::new(Notify::new()),
        }
    }
}

impl BlockSubscriber for StateNodeAdapterStdImpl {
    type HandleBlockFut = BoxFuture<'static, anyhow::Result<()>>;

    fn handle_block(&self, block: &BlockStuff) -> Self::HandleBlockFut {
        let block_id = *block.id();
        let shard = block_id.shard;
        let seqno = block_id.seqno;

        let blocks_lock = self.blocks.clone();
        let listener = self.listener.clone();

        Box::pin(async move {
            let mut blocks = blocks_lock.lock().await;

            if let Some(shard_blocks) = blocks.get_mut(&shard) {
                if let Some(block_data) = shard_blocks.get(&seqno) {
                    let top_shard_blocks_ids = block_data.top_shard_blocks_ids.clone();

                    if shard.is_masterchain() {
                        let prev_id = block_data
                            .prev_blocks_ids
                            .last()
                            .ok_or(anyhow!("no prev block"))?
                            .seqno;
                        for id in top_shard_blocks_ids.iter() {
                            if let Some(shard_blocks) = blocks.get_mut(&id.shard) {
                                shard_blocks.split_off(&prev_id);
                                shard_blocks.remove(&prev_id);
                            }
                        }
                    } else {
                        shard_blocks.split_off(&seqno);
                        shard_blocks.remove(&seqno);
                    }
                    listener.on_block_accepted(&block_id).await;
                } else {
                    listener.on_block_accepted_external(&block_id).await;
                }
            } else {
                listener.on_block_accepted_external(&block_id).await;
            }
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

    async fn load_state(&self, block_id: BlockId) -> Result<Arc<ShardStateStuff>> {
        let state = self
            .storage
            .shard_state_storage()
            .load_state(&block_id)
            .await?;
        Ok(state)
    }

    async fn load_block(&self, block_id: BlockId) -> Result<Option<Arc<BlockStuff>>> {
        let block_handle = self.storage.block_handle_storage().load_handle(&block_id)?;
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

    async fn accept_block(&self, block: BlockStuffForSync) -> Result<()> {
        let mut blocks = self.blocks.lock().await;
        match block.block_id.shard.is_masterchain() {
            true => {
                let prev_id = block
                    .prev_blocks_ids
                    .last()
                    .ok_or(anyhow!("no prev block"))?
                    .seqno;
                blocks
                    .entry(block.block_id.shard)
                    .or_insert_with(BTreeMap::new)
                    .insert(prev_id, block);
            }
            false => {
                blocks
                    .entry(block.block_id.shard)
                    .or_insert_with(BTreeMap::new)
                    .insert(block.block_id.seqno, block);
            }
        }

        self.notifier.notify_waiters();
        Ok(())
    }
}
