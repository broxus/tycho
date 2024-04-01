use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use everscale_types::boc::Boc;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, ShardIdent, ShardStateUnsplit};

use tycho_block_util::state::MinRefMcStateTracker;
use tycho_block_util::{block::BlockStuff, state::ShardStateStuff};

use crate::tracing_targets;
use crate::types::ext_types::BlockHandle;
use crate::{
    impl_enum_try_into, method_to_async_task_closure,
    types::BlockStuffForSync,
    utils::{
        async_queued_dispatcher::{AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE},
        task_descr::TaskResponseReceiver,
    },
};

// BUILDER

#[allow(private_bounds, private_interfaces)]
pub trait StateNodeAdapterBuilder<T>
where
    T: StateNodeAdapter,
{
    fn new() -> Self;
    fn build(self, listener: Arc<dyn StateNodeEventListener>) -> T;
}

pub struct StateNodeAdapterBuilderStdImpl<T> {
    _marker_adapter: std::marker::PhantomData<T>,
}

#[allow(private_bounds, private_interfaces)]
impl<T> StateNodeAdapterBuilder<T> for StateNodeAdapterBuilderStdImpl<T>
where
    T: StateNodeAdapter,
{
    fn new() -> Self {
        Self {
            _marker_adapter: std::marker::PhantomData,
        }
    }
    fn build(self, listener: Arc<dyn StateNodeEventListener>) -> T {
        T::create(listener)
    }
}

// EVENTS EMITTER AMD LISTENER

#[async_trait]
pub(crate) trait StateNodeEventEmitter {
    /// When new masterchain block received from blockchain
    async fn on_mc_block_event(&self, mc_block_id: BlockId);
}

#[async_trait]
pub(crate) trait StateNodeEventListener: Send + Sync {
    /// Process new received masterchain block from blockchain
    async fn on_mc_block(&self, mc_block_id: BlockId) -> Result<()>;
}

// ADAPTER

#[async_trait]
pub(crate) trait StateNodeAdapter: Send + Sync + 'static {
    fn create(listener: Arc<dyn StateNodeEventListener>) -> Self;
    async fn get_last_applied_mc_block_id(&self) -> Result<BlockId>;
    async fn request_state(
        &self,
        block_id: BlockId,
    ) -> Result<StateNodeTaskResponseReceiver<Arc<ShardStateStuff>>>;
    async fn get_block(&self, block_id: BlockId) -> Result<Option<Arc<BlockStuff>>>;
    async fn request_block(
        &self,
        block_id: BlockId,
    ) -> Result<StateNodeTaskResponseReceiver<Option<Arc<BlockStuff>>>>;
    async fn accept_block(&self, block: BlockStuffForSync) -> Result<Arc<BlockHandle>>;
}

pub struct StateNodeAdapterStdImpl {
    dispatcher: Arc<AsyncQueuedDispatcher<StateNodeProcessor, StateNodeTaskResult>>,
    listener: Arc<dyn StateNodeEventListener>,
}

#[async_trait]
impl StateNodeAdapter for StateNodeAdapterStdImpl {
    fn create(listener: Arc<dyn StateNodeEventListener>) -> Self {
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "Creating state node adapter...");

        let processor = StateNodeProcessor {
            listener: listener.clone(),
        };
        let dispatcher =
            AsyncQueuedDispatcher::create(processor, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        tracing::trace!(target: tracing_targets::STATE_NODE_ADAPTER, "Tasks queue dispatcher started");

        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "State node adapter created");

        Self {
            dispatcher: Arc::new(dispatcher),
            listener,
        }
    }

    async fn get_last_applied_mc_block_id(&self) -> Result<BlockId> {
        self.dispatcher
            .execute_task(method_to_async_task_closure!(get_last_applied_mc_block_id,))
            .await
            .and_then(|res| res.try_into())
    }

    async fn request_state(
        &self,
        block_id: BlockId,
    ) -> Result<StateNodeTaskResponseReceiver<Arc<ShardStateStuff>>> {
        let receiver = self
            .dispatcher
            .enqueue_task_with_responder(method_to_async_task_closure!(get_state, block_id))
            .await?;

        Ok(StateNodeTaskResponseReceiver::create(receiver))
    }

    async fn get_block(&self, block_id: BlockId) -> Result<Option<Arc<BlockStuff>>> {
        self.dispatcher
            .execute_task(method_to_async_task_closure!(get_block, block_id))
            .await
            .and_then(|res| res.try_into())
    }

    async fn request_block(
        &self,
        block_id: BlockId,
    ) -> Result<StateNodeTaskResponseReceiver<Option<Arc<BlockStuff>>>> {
        let receiver = self
            .dispatcher
            .enqueue_task_with_responder(method_to_async_task_closure!(get_block, block_id))
            .await?;

        Ok(StateNodeTaskResponseReceiver::create(receiver))
    }

    async fn accept_block(&self, block: BlockStuffForSync) -> Result<Arc<BlockHandle>> {
        self.dispatcher
            .execute_task(method_to_async_task_closure!(accept_block, block))
            .await
            .and_then(|res| res.try_into())
    }
}

// ADAPTER PROCESSOR

struct StateNodeProcessor {
    listener: Arc<dyn StateNodeEventListener>,
}

pub(crate) enum StateNodeTaskResult {
    Void,
    BlockId(BlockId),
    ShardState(Arc<ShardStateStuff>),
    Block(Option<Arc<BlockStuff>>),
    BlockHandle(Arc<BlockHandle>),
}

impl_enum_try_into!(StateNodeTaskResult, BlockId, BlockId);
impl_enum_try_into!(StateNodeTaskResult, ShardState, Arc<ShardStateStuff>);
impl_enum_try_into!(StateNodeTaskResult, Block, Option<Arc<BlockStuff>>);
impl_enum_try_into!(StateNodeTaskResult, BlockHandle, Arc<BlockHandle>);

type StateNodeTaskResponseReceiver<T> = TaskResponseReceiver<StateNodeTaskResult, T>;

impl StateNodeProcessor {
    async fn get_last_applied_mc_block_id(&self) -> Result<StateNodeTaskResult> {
        //TODO: make real implementation

        //STUB: return block 1
        let stub_mc_block_id = BlockId {
            shard: ShardIdent::new_full(-1),
            seqno: 1,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };
        tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "STUB: returns stub last applied mc block ({})", stub_mc_block_id.as_short_id());
        Ok(StateNodeTaskResult::BlockId(stub_mc_block_id))
    }
    async fn get_state(&self, block_id: BlockId) -> Result<StateNodeTaskResult> {
        //TODO: make real implementation
        let cell = if block_id.is_masterchain() {
            tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "STUB: returns stub master state on block 2");
            const BOC: &[u8] = include_bytes!("state_node/tests/data/test_state_2_master.boc");
            Boc::decode(BOC)?
        } else {
            tracing::info!(target: tracing_targets::STATE_NODE_ADAPTER, "STUB: returns stub shard state on block 2");
            const BOC: &[u8] = include_bytes!("state_node/tests/data/test_state_2_0:80.boc");
            Boc::decode(BOC)?
        };

        let shard_state = cell.parse::<ShardStateUnsplit>()?;
        tracing::debug!(target: tracing_targets::STATE_NODE_ADAPTER, "state: {:?}", shard_state);

        let fixed_stub_block_id = BlockId {
            shard: shard_state.shard_ident,
            seqno: shard_state.seqno,
            root_hash: block_id.root_hash,
            file_hash: block_id.file_hash,
        };
        let tracker = MinRefMcStateTracker::new();
        let state_stuff = ShardStateStuff::new(fixed_stub_block_id, cell, &tracker)?;

        Ok(StateNodeTaskResult::ShardState(Arc::new(state_stuff)))
    }
    async fn get_block(&self, block_id: BlockId) -> Result<StateNodeTaskResult> {
        //TODO: make real implementation
        Ok(StateNodeTaskResult::Block(None))
    }
    async fn accept_block(&mut self, block: BlockStuffForSync) -> Result<StateNodeTaskResult> {
        //TODO: make real implementation
        Ok(StateNodeTaskResult::BlockHandle(Arc::new(BlockHandle {})))
    }
}
