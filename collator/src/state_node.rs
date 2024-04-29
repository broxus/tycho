use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use everscale_types::boc::Boc;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, ShardIdent, ShardStateUnsplit};

use tycho_block_util::state::MinRefMcStateTracker;
use tycho_block_util::{block::BlockStuff, state::ShardStateStuff};
use tycho_storage::BlockHandle;

use crate::tracing_targets;
use crate::types::BlockStuffForSync;
use crate::utils::task_descr::TaskResponseReceiver;

// BUILDER

#[allow(private_bounds, private_interfaces)]
pub trait StateNodeAdapterBuilder<T>
where
    T: StateNodeAdapter,
{
    fn new() -> Self;
    fn build(self, listener: Arc<dyn StateNodeEventListener>) -> T;
}

pub struct StateNodeAdapterBuilderStdImpl;

impl StateNodeAdapterBuilder<StateNodeAdapterStdImpl> for StateNodeAdapterBuilderStdImpl {
    fn new() -> Self {
        Self {}
    }
    #[allow(private_interfaces)]
    fn build(self, listener: Arc<dyn StateNodeEventListener>) -> StateNodeAdapterStdImpl {
        StateNodeAdapterStdImpl::create(listener)
    }
}

// EVENTS LISTENER

#[async_trait]
pub(crate) trait StateNodeEventListener: Send + Sync {
    /// When new masterchain block received from blockchain
    async fn on_mc_block(&self, mc_block_id: BlockId) -> Result<()>;
}

// ADAPTER

#[async_trait]
pub(crate) trait StateNodeAdapter: Send + Sync + 'static {
    async fn get_last_applied_mc_block_id(&self) -> Result<BlockId>;
    async fn request_state(
        &self,
        block_id: BlockId,
    ) -> Result<TaskResponseReceiver<Arc<ShardStateStuff>>>;
    async fn get_block(&self, block_id: BlockId) -> Result<Option<Arc<BlockStuff>>>;
    async fn request_block(
        &self,
        block_id: BlockId,
    ) -> Result<TaskResponseReceiver<Option<Arc<BlockStuff>>>>;
    async fn accept_block(&self, block: BlockStuffForSync) -> Result<Arc<BlockHandle>>;
}

pub struct StateNodeAdapterStdImpl {
    _listener: Arc<dyn StateNodeEventListener>,
}

impl StateNodeAdapterStdImpl {
    fn create(listener: Arc<dyn StateNodeEventListener>) -> Self {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Creating state node adapter...");

        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "State node adapter created");

        Self {
            _listener: listener,
        }
    }
}

#[async_trait]
impl StateNodeAdapter for StateNodeAdapterStdImpl {
    async fn get_last_applied_mc_block_id(&self) -> Result<BlockId> {
        //TODO: make real implementation

        //STUB: return block 1
        let stub_mc_block_id = BlockId {
            shard: ShardIdent::new_full(-1),
            seqno: 1,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "STUB: returns stub last applied mc block ({})", stub_mc_block_id.as_short_id());
        Ok(stub_mc_block_id)
    }

    async fn request_state(
        &self,
        block_id: BlockId,
    ) -> Result<TaskResponseReceiver<Arc<ShardStateStuff>>> {
        //TODO: make real implementation
        let (sender, receiver) = tokio::sync::oneshot::channel::<Result<Arc<ShardStateStuff>>>();

        //STUB: emulating async task
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(45)).await;

            let cell = if block_id.is_masterchain() {
                tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "STUB: returns stub master state on block 2");
                const BOC: &[u8] = include_bytes!("state_node/tests/data/test_state_2_master.boc");
                Boc::decode(BOC)
            } else {
                tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "STUB: returns stub shard state on block 2");
                const BOC: &[u8] = include_bytes!("state_node/tests/data/test_state_2_0:80.boc");
                Boc::decode(BOC)
            };
            let cell = cell?;

            let shard_state = cell.parse::<ShardStateUnsplit>()?;
            tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "state: {:?}", shard_state);

            let fixed_stub_block_id = BlockId {
                shard: shard_state.shard_ident,
                seqno: shard_state.seqno,
                root_hash: block_id.root_hash,
                file_hash: block_id.file_hash,
            };
            let tracker = MinRefMcStateTracker::new();
            let state_stuff =
                ShardStateStuff::new(fixed_stub_block_id, cell, &tracker).map(Arc::new);

            sender
                .send(state_stuff)
                .map_err(|_err| anyhow!("eror sending result out of spawned future"))
        });

        let receiver = TaskResponseReceiver::create(receiver);

        Ok(receiver)
    }

    async fn get_block(&self, _block_id: BlockId) -> Result<Option<Arc<BlockStuff>>> {
        //TODO: make real implementation

        //STUB: just remove empty block
        Ok(None)
    }

    async fn request_block(
        &self,
        _block_id: BlockId,
    ) -> Result<TaskResponseReceiver<Option<Arc<BlockStuff>>>> {
        //TODO: make real implementation
        let (sender, receiver) = tokio::sync::oneshot::channel::<Result<Option<Arc<BlockStuff>>>>();

        //STUB: emulating async task
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(85)).await;
            sender.send(Ok(None))
        });

        Ok(TaskResponseReceiver::create(receiver))
    }

    async fn accept_block(&self, block: BlockStuffForSync) -> Result<Arc<BlockHandle>> {
        //TODO: make real implementation
        //STUB: create dummy blcok handle
        let handle = BlockHandle::new(
            &block.block_id,
            Default::default(),
            Arc::new(Default::default()),
        );
        Ok(Arc::new(handle))
    }
}
