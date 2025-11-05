use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use anyhow::Result;
use futures_util::future::BoxFuture;
use tokio::task::JoinHandle;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::RefMcStateHandle;
use crate::block_strider::{StateSubscriber, StateSubscriberContext};
use crate::storage::{BlockHandle, CoreStorage};
/// Persistent state subscriber.
#[derive(Clone)]
pub struct PsSubscriber {
    inner: Arc<Inner>,
}
impl PsSubscriber {
    pub fn new(storage: CoreStorage) -> Self {
        let last_key_block_utime = Self::find_last_key_block_utime(&storage);
        Self {
            inner: Arc::new(Inner {
                last_key_block_utime: AtomicU32::new(last_key_block_utime),
                storage,
                completion_subscriber: Default::default(),
                prev_state_task: Default::default(),
            }),
        }
    }
    pub fn with_completion_subscriber<S>(
        storage: CoreStorage,
        completion_subscriber: S,
    ) -> Self
    where
        S: PsCompletionSubscriber,
    {
        let last_key_block_utime = Self::find_last_key_block_utime(&storage);
        Self {
            inner: Arc::new(Inner {
                last_key_block_utime: AtomicU32::new(last_key_block_utime),
                storage,
                completion_subscriber: Some(Arc::new(completion_subscriber)),
                prev_state_task: Default::default(),
            }),
        }
    }
    fn find_last_key_block_utime(storage: &CoreStorage) -> u32 {
        storage
            .block_handle_storage()
            .find_last_key_block()
            .map_or(0, |handle| handle.gen_utime())
    }
}
impl StateSubscriber for PsSubscriber {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;
    fn handle_state<'a>(
        &'a self,
        cx: &'a StateSubscriberContext,
    ) -> Self::HandleStateFut<'a> {
        Box::pin(self.inner.handle_state_impl(cx))
    }
}
struct Inner {
    last_key_block_utime: AtomicU32,
    storage: CoreStorage,
    completion_subscriber: Option<Arc<dyn PsCompletionSubscriber>>,
    prev_state_task: tokio::sync::Mutex<Option<StorePersistentStateTask>>,
}
impl Inner {
    async fn handle_state_impl(
        self: &Arc<Self>,
        cx: &StateSubscriberContext,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_state_impl)),
            file!(),
            71u32,
        );
        let cx = cx;
        let mut prev_task = {
            __guard.end_section(74u32);
            let __result = self.prev_state_task.lock().await;
            __guard.start_section(74u32);
            __result
        };
        if let Some(task) = &mut *prev_task && task.is_finished() {
            {
                __guard.end_section(78u32);
                let __result = task.join().await;
                __guard.start_section(78u32);
                __result
            }?;
        }
        drop(prev_task);
        if cx.is_key_block {
            let block_info = cx.block.load_info()?;
            let prev_utime = self
                .last_key_block_utime
                .swap(block_info.gen_utime, Ordering::Relaxed);
            let is_persistent = BlockStuff::compute_is_persistent(
                block_info.gen_utime,
                prev_utime,
            );
            if is_persistent && cx.block.id().seqno != 0 {
                let mut prev_task = {
                    __guard.end_section(91u32);
                    let __result = self.prev_state_task.lock().await;
                    __guard.start_section(91u32);
                    __result
                };
                if let Some(task) = &mut *prev_task {
                    {
                        __guard.end_section(93u32);
                        let __result = task.join().await;
                        __guard.start_section(93u32);
                        __result
                    }?;
                }
                let block = cx.block.clone();
                let inner = self.clone();
                let state_handle = cx.state.ref_mc_state_handle().clone();
                *prev_task = Some(StorePersistentStateTask {
                    mc_seqno: cx.mc_block_id.seqno,
                    handle: Some(
                        tokio::spawn(async move {
                            let mut __guard = crate::__async_profile_guard__::Guard::new(
                                concat!(module_path!(), "::async_block"),
                                file!(),
                                102u32,
                            );
                            {
                                __guard.end_section(103u32);
                                let __result = inner.save_impl(block, state_handle).await;
                                __guard.start_section(103u32);
                                __result
                            }
                        }),
                    ),
                });
            }
        }
        Ok(())
    }
    async fn save_impl(
        &self,
        mc_block: BlockStuff,
        mc_state_handle: RefMcStateHandle,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(save_impl)),
            file!(),
            116u32,
        );
        let mc_block = mc_block;
        let mc_state_handle = mc_state_handle;
        let block_handles = self.storage.block_handle_storage();
        let mc_seqno = mc_block.id().seqno;
        let Some(mc_block_handle) = block_handles.load_handle(mc_block.id()) else {
            anyhow::bail!("masterchain block handle not found: {}", mc_block.id());
        };
        block_handles.set_block_persistent(&mc_block_handle);
        let (state_result, queue_result) = {
            __guard.end_section(126u32);
            let __result = tokio::join!(
                self.save_persistent_shard_states(mc_block_handle.clone(), mc_block
                .clone(), mc_state_handle), self
                .save_persistent_queue_states(mc_block_handle.clone(), mc_block),
            );
            __guard.start_section(126u32);
            __result
        };
        state_result?;
        queue_result?;
        {
            __guard.end_section(140u32);
            let __result = self
                .storage
                .persistent_state_storage()
                .rotate_persistent_states(&mc_block_handle)
                .await;
            __guard.start_section(140u32);
            __result
        }?;
        if let Some(completion_subscriber) = &self.completion_subscriber {
            completion_subscriber.on_state_persisted(mc_seqno);
        }
        metrics::counter!("tycho_core_ps_subscriber_saved_persistent_states_count")
            .increment(1);
        Ok(())
    }
    async fn save_persistent_shard_states(
        &self,
        mc_block_handle: BlockHandle,
        mc_block: BlockStuff,
        mc_state_handle: RefMcStateHandle,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(save_persistent_shard_states)),
            file!(),
            156u32,
        );
        let mc_block_handle = mc_block_handle;
        let mc_block = mc_block;
        let mc_state_handle = mc_state_handle;
        let block_handles = self.storage.block_handle_storage();
        let persistent_states = self.storage.persistent_state_storage();
        let mc_seqno = mc_block_handle.id().seqno;
        for entry in mc_block.load_custom()?.shards.latest_blocks() {
            __guard.checkpoint(161u32);
            let block_id = entry?;
            let Some(block_handle) = block_handles.load_handle(&block_id) else {
                anyhow::bail!("top shard block handle not found: {block_id}");
            };
            {
                __guard.end_section(172u32);
                let __result = persistent_states
                    .store_shard_state(mc_seqno, &block_handle, mc_state_handle.clone())
                    .await;
                __guard.start_section(172u32);
                __result
            }?;
        }
        {
            __guard.end_section(180u32);
            let __result = persistent_states
                .store_shard_state(mc_seqno, &mc_block_handle, mc_state_handle)
                .await;
            __guard.start_section(180u32);
            __result
        }
    }
    async fn save_persistent_queue_states(
        &self,
        mc_block_handle: BlockHandle,
        mc_block: BlockStuff,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(save_persistent_queue_states)),
            file!(),
            187u32,
        );
        let mc_block_handle = mc_block_handle;
        let mc_block = mc_block;
        if mc_block_handle.id().seqno == 0 {
            {
                __guard.end_section(190u32);
                return Ok(());
            };
        }
        let blocks = self.storage.block_storage();
        let block_handles = self.storage.block_handle_storage();
        let persistent_states = self.storage.persistent_state_storage();
        let mut shard_block_handles = Vec::new();
        for entry in mc_block.load_custom()?.shards.latest_blocks() {
            __guard.checkpoint(199u32);
            let block_id = entry?;
            if block_id.seqno == 0 {
                {
                    __guard.end_section(203u32);
                    __guard.start_section(203u32);
                    continue;
                };
            }
            let Some(block_handle) = block_handles.load_handle(&block_id) else {
                anyhow::bail!("top shard block handle not found: {block_id}");
            };
            block_handles.set_block_persistent(&block_handle);
            shard_block_handles.push(block_handle);
        }
        let mc_seqno = mc_block_handle.id().seqno;
        for block_handle in shard_block_handles {
            __guard.checkpoint(219u32);
            let block = {
                __guard.end_section(220u32);
                let __result = blocks.load_block_data(&block_handle).await;
                __guard.start_section(220u32);
                __result
            }?;
            {
                __guard.end_section(223u32);
                let __result = persistent_states
                    .store_queue_state(mc_seqno, &block_handle, block)
                    .await;
                __guard.start_section(223u32);
                __result
            }?;
        }
        {
            __guard.end_section(228u32);
            let __result = persistent_states
                .store_queue_state(mc_seqno, &mc_block_handle, mc_block)
                .await;
            __guard.start_section(228u32);
            __result
        }
    }
}
struct StorePersistentStateTask {
    mc_seqno: u32,
    handle: Option<JoinHandle<Result<()>>>,
}
impl StorePersistentStateTask {
    async fn join(&mut self) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(join)),
            file!(),
            238u32,
        );
        if let Some(handle) = &mut self.handle {
            let result = {
                __guard.end_section(242u32);
                let __result = handle.await;
                __guard.start_section(242u32);
                __result
            }
                .map_err(|e| {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    }
                    anyhow::Error::from(e)
                })
                .and_then(std::convert::identity);
            self.handle = None;
            if let Err(e) = &result {
                tracing::error!(
                    mc_seqno = self.mc_seqno, "failed to save persistent state: {e:?}"
                );
            }
            {
                __guard.end_section(260u32);
                return result;
            };
        }
        Ok(())
    }
    fn is_finished(&self) -> bool {
        if let Some(handle) = &self.handle {
            return handle.is_finished();
        }
        false
    }
}
pub trait PsCompletionSubscriber: Send + Sync + 'static {
    fn on_state_persisted(&self, mc_seqno: u32);
}
