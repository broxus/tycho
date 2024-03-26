use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use everscale_types::models::BlockId;
use tycho_block_util::block::BlockStuff;

use tycho_block_util::state::ShardStateStuff;

use crate::{
    impl_enum_try_into, method_to_async_task_closure,
    types::{ext_types::BlockHandle, BlockStuffForSync},
    utils::{
        async_queued_dispatcher::{AsyncQueuedDispatcher, STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE},
        task_descr::TaskResponseReceiver,
    },
};

// BUILDER

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
pub trait StateNodeEventEmitter {
    /// When new masterchain block received from blockchain
    async fn on_mc_block_event(&self, mc_block_id: BlockId);
}

#[async_trait]
pub trait StateNodeEventListener: Send + Sync {
    /// Process new received masterchain block from blockchain
    async fn on_mc_block(&self, mc_block_id: BlockId) -> Result<()>;
}

// ADAPTER

#[async_trait]
pub trait StateNodeAdapter: Send + Sync + 'static {
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

pub(crate) struct StateNodeProcessor {
    // state_provider: BlockProvider,
    pub listener: Arc<dyn StateNodeEventListener>,
}

pub enum StateNodeTaskResult {
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
        // let block = "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";
        // let block = everscale_types::boc::BocRepr::decode_base64( block).unwrap();
        // let block_stuff = BlockStuff::with_block(block_id.clone(), block);
        // Ok(StateNodeTaskResult::Block(Some(Arc::new(block_stuff))))
        Ok(StateNodeTaskResult::Block(None))
    }
    async fn accept_block(&mut self, block: BlockStuffForSync) -> Result<StateNodeTaskResult> {
        todo!()
    }
}
