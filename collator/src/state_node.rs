use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use everscale_types::models::BlockId;

use tycho_block_util::{block::BlockStuff, state::ShardStateStuff};

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
    async fn get_last_applied_mc_block_id(&self) -> Result<BlockId> {
        self.dispatcher
            .execute_task(method_to_async_task_closure!(get_last_applied_mc_block_id,))
            .await
            .and_then(|res| res.try_into())
    }
    fn create(listener: Arc<dyn StateNodeEventListener>) -> Self {
        let processor = StateNodeProcessor {
            listener: listener.clone(),
        };
        let dispatcher =
            AsyncQueuedDispatcher::create(processor, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE);
        Self {
            dispatcher: Arc::new(dispatcher),
            listener,
        }
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
        todo!()
    }
    async fn get_state(&self, block_id: BlockId) -> Result<StateNodeTaskResult> {
        todo!()
    }
    async fn get_block(&self, block_id: BlockId) -> Result<StateNodeTaskResult> {
        todo!()
    }
    async fn accept_block(&mut self, block: BlockStuffForSync) -> Result<StateNodeTaskResult> {
        todo!()
    }
}
