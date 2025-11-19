use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use anyhow::Result;
use futures_util::future::{BoxFuture, FutureExt};
use tokio::task::JoinHandle;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::RefMcStateHandle;

use crate::block_strider::{
    NoopSubscriber, PsCompletionContext, PsCompletionSubscriber, StateSubscriber,
    StateSubscriberContext,
};
use crate::storage::{BlockHandle, CoreStorage, PersistentStateKind};

/// A builder for [`PsSubscriber`].
pub struct PsSubscriberBuilder {
    storage: CoreStorage,
    completion_subscriber: Option<BoxPsCompletionSubscriber>,
}

impl PsSubscriberBuilder {
    pub fn new(storage: CoreStorage) -> Self {
        Self {
            storage,
            completion_subscriber: None,
        }
    }

    pub fn build(self) -> PsSubscriber {
        let last_key_block_utime = Self::find_last_key_block_utime(&self.storage);
        PsSubscriber {
            inner: Arc::new(Inner {
                last_key_block_utime: AtomicU32::new(last_key_block_utime),
                storage: self.storage,
                completion_subscriber: self
                    .completion_subscriber
                    .unwrap_or_else(|| BoxPsCompletionSubscriber::new(NoopSubscriber)),
                prev_state_task: Default::default(),
            }),
        }
    }

    pub fn with_completion_subscriber<S: PsCompletionSubscriber>(
        mut self,
        completion_subscriber: S,
    ) -> Self {
        self.completion_subscriber = Some(BoxPsCompletionSubscriber::new(completion_subscriber));
        self
    }

    fn find_last_key_block_utime(storage: &CoreStorage) -> u32 {
        storage
            .block_handle_storage()
            .find_last_key_block()
            .map_or(0, |handle| handle.gen_utime())
    }
}

/// Persistent state subscriber.
#[derive(Clone)]
pub struct PsSubscriber {
    inner: Arc<Inner>,
}

impl PsSubscriber {
    pub fn builder(storage: CoreStorage) -> PsSubscriberBuilder {
        PsSubscriberBuilder::new(storage)
    }
}

impl StateSubscriber for PsSubscriber {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        Box::pin(self.inner.handle_state_impl(cx))
    }
}

struct Inner {
    last_key_block_utime: AtomicU32,
    storage: CoreStorage,
    completion_subscriber: BoxPsCompletionSubscriber,
    prev_state_task: tokio::sync::Mutex<Option<StorePersistentStateTask>>,
}

impl Inner {
    async fn handle_state_impl(self: &Arc<Self>, cx: &StateSubscriberContext) -> Result<()> {
        // Check if the previous persistent state save task has finished.
        // This allows us to detect errors early without waiting for the next key block
        let mut prev_task = self.prev_state_task.lock().await;
        if let Some(task) = &mut *prev_task
            && task.is_finished()
        {
            task.join().await?;
        }
        drop(prev_task);

        if cx.is_key_block {
            let block_info = cx.block.load_info()?;

            let prev_utime = self
                .last_key_block_utime
                .swap(block_info.gen_utime, Ordering::Relaxed);
            let is_persistent = BlockStuff::compute_is_persistent(block_info.gen_utime, prev_utime);

            if is_persistent && cx.block.id().seqno != 0 {
                let mut prev_task = self.prev_state_task.lock().await;
                if let Some(task) = &mut *prev_task {
                    task.join().await?;
                }

                let block = cx.block.clone();
                let inner = self.clone();
                let state_handle = cx.state.ref_mc_state_handle().clone();

                *prev_task = Some(StorePersistentStateTask {
                    mc_seqno: cx.mc_block_id.seqno,
                    handle: Some(tokio::spawn(async move {
                        inner.save_impl(block, state_handle).await
                    })),
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
        let block_handles = self.storage.block_handle_storage();

        let Some(mc_block_handle) = block_handles.load_handle(mc_block.id()) else {
            anyhow::bail!("masterchain block handle not found: {}", mc_block.id());
        };
        block_handles.set_block_persistent(&mc_block_handle);

        let (state_result, queue_result) = tokio::join!(
            self.save_persistent_shard_states(
                mc_block_handle.clone(),
                mc_block.clone(),
                mc_state_handle
            ),
            self.save_persistent_queue_states(mc_block_handle.clone(), mc_block),
        );
        state_result?;
        queue_result?;

        self.storage
            .persistent_state_storage()
            .rotate_persistent_states(&mc_block_handle)
            .await?;

        metrics::counter!("tycho_core_ps_subscriber_saved_persistent_states_count").increment(1);

        Ok(())
    }

    async fn save_persistent_shard_states(
        &self,
        mc_block_handle: BlockHandle,
        mc_block: BlockStuff,
        mc_state_handle: RefMcStateHandle,
    ) -> Result<()> {
        let block_handles = self.storage.block_handle_storage();
        let persistent_states = self.storage.persistent_state_storage();

        let mc_seqno = mc_block_handle.id().seqno;
        for entry in mc_block.load_custom()?.shards.latest_blocks() {
            let block_id = entry?;
            let Some(block_handle) = block_handles.load_handle(&block_id) else {
                anyhow::bail!("top shard block handle not found: {block_id}");
            };

            // NOTE: We could have also called the `set_block_persistent` here, but we
            //       only do this in the first part of the `save_persistent_queue_states`.

            persistent_states
                .store_shard_state(mc_seqno, &block_handle, mc_state_handle.clone())
                .await?;

            let cx = PsCompletionContext {
                block_id,
                kind: PersistentStateKind::Shard,
            };
            self.completion_subscriber.on_state_persisted(&cx).await?;
        }

        // NOTE: We intentionally store the masterchain state last to ensure that
        //       the handle will live long enough. And this way we don't mislead
        //       other nodes with the incomplete set of persistent states.
        persistent_states
            .store_shard_state(mc_seqno, &mc_block_handle, mc_state_handle)
            .await?;

        let cx = PsCompletionContext {
            block_id: *mc_block_handle.id(),
            kind: PersistentStateKind::Shard,
        };
        self.completion_subscriber.on_state_persisted(&cx).await?;

        Ok(())
    }

    async fn save_persistent_queue_states(
        &self,
        mc_block_handle: BlockHandle,
        mc_block: BlockStuff,
    ) -> Result<()> {
        if mc_block_handle.id().seqno == 0 {
            // No queue states for zerostate.
            return Ok(());
        }

        let blocks = self.storage.block_storage();
        let block_handles = self.storage.block_handle_storage();
        let persistent_states = self.storage.persistent_state_storage();

        let mut shard_block_handles = Vec::new();

        for entry in mc_block.load_custom()?.shards.latest_blocks() {
            let block_id = entry?;
            if block_id.seqno == 0 {
                // No queue states for zerostate.
                continue;
            }

            let Some(block_handle) = block_handles.load_handle(&block_id) else {
                anyhow::bail!("top shard block handle not found: {block_id}");
            };

            // NOTE: We set the flag only here because this part will be executed
            //       first, without waiting for other states or queues to be saved.
            block_handles.set_block_persistent(&block_handle);

            shard_block_handles.push(block_handle);
        }

        // Store queue state for each shard
        let mc_seqno = mc_block_handle.id().seqno;
        for block_handle in shard_block_handles {
            let block = blocks.load_block_data(&block_handle).await?;
            persistent_states
                .store_queue_state(mc_seqno, &block_handle, block)
                .await?;

            let cx = PsCompletionContext {
                block_id: *block_handle.id(),
                kind: PersistentStateKind::Queue,
            };
            self.completion_subscriber.on_state_persisted(&cx).await?;
        }

        persistent_states
            .store_queue_state(mc_seqno, &mc_block_handle, mc_block)
            .await?;

        let cx = PsCompletionContext {
            block_id: *mc_block_handle.id(),
            kind: PersistentStateKind::Queue,
        };
        self.completion_subscriber.on_state_persisted(&cx).await?;

        Ok(())
    }
}

struct StorePersistentStateTask {
    mc_seqno: u32,
    handle: Option<JoinHandle<Result<()>>>,
}

impl StorePersistentStateTask {
    async fn join(&mut self) -> Result<()> {
        // NOTE: Await on reference to make sure that the task is cancel safe
        if let Some(handle) = &mut self.handle {
            let result = handle
                .await
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
                    mc_seqno = self.mc_seqno,
                    "failed to save persistent state: {e:?}"
                );
            }

            return result;
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

pub struct BoxPsCompletionSubscriber {
    data: AtomicPtr<()>,
    vtable: &'static Vtable,
}

impl BoxPsCompletionSubscriber {
    pub fn new<S: PsCompletionSubscriber>(provider: S) -> Self {
        let ptr = Box::into_raw(Box::new(provider));

        Self {
            data: AtomicPtr::new(ptr.cast()),
            vtable: const { Vtable::new::<S>() },
        }
    }
}

impl PsCompletionSubscriber for BoxPsCompletionSubscriber {
    type OnStatePersistedFut<'a> = OnStatePersistedFut<'a>;

    fn on_state_persisted<'a>(
        &'a self,
        cx: &'a PsCompletionContext,
    ) -> Self::OnStatePersistedFut<'a> {
        unsafe { (self.vtable.on_state_persisted)(&self.data, cx) }
    }
}

impl Drop for BoxPsCompletionSubscriber {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(&mut self.data) }
    }
}

// Vtable must enforce this behavior
unsafe impl Send for BoxPsCompletionSubscriber {}
unsafe impl Sync for BoxPsCompletionSubscriber {}

struct Vtable {
    on_state_persisted: OnStatePersistedFn,
    drop: DropFn,
}

impl Vtable {
    const fn new<S: PsCompletionSubscriber>() -> &'static Self {
        &Self {
            on_state_persisted: |ptr, cx| {
                let subscriber = unsafe { &*ptr.load(Ordering::Relaxed).cast::<S>() };
                subscriber.on_state_persisted(cx).boxed()
            },
            drop: |ptr| {
                drop(unsafe { Box::<S>::from_raw(ptr.get_mut().cast::<S>()) });
            },
        }
    }
}

type OnStatePersistedFn =
    for<'a> unsafe fn(&AtomicPtr<()>, &'a PsCompletionContext) -> OnStatePersistedFut<'a>;
type DropFn = unsafe fn(&mut AtomicPtr<()>);

type OnStatePersistedFut<'a> = BoxFuture<'a, Result<()>>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use anyhow::Result;

    use super::*;

    #[tokio::test]
    async fn boxed_subscriber_works() -> Result<()> {
        struct SubscriberState {
            persisted_called: AtomicUsize,
            dropped: AtomicUsize,
        }

        struct TestSubscriber {
            state: Arc<SubscriberState>,
        }

        impl Drop for TestSubscriber {
            fn drop(&mut self) {
                self.state.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }

        impl PsCompletionSubscriber for TestSubscriber {
            type OnStatePersistedFut<'a> = futures_util::future::Ready<Result<()>>;

            fn on_state_persisted<'a>(
                &'a self,
                _cx: &'a PsCompletionContext,
            ) -> Self::OnStatePersistedFut<'a> {
                self.state.persisted_called.fetch_add(1, Ordering::Relaxed);
                futures_util::future::ready(Ok(()))
            }
        }

        let state = Arc::new(SubscriberState {
            persisted_called: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        });
        let boxed = BoxPsCompletionSubscriber::new(TestSubscriber {
            state: state.clone(),
        });

        assert_eq!(state.persisted_called.load(Ordering::Acquire), 0);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        let cx = PsCompletionContext {
            block_id: Default::default(),
            kind: PersistentStateKind::Shard,
        };
        assert!(boxed.on_state_persisted(&cx).await.is_ok());
        assert_eq!(state.persisted_called.load(Ordering::Acquire), 1);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        assert!(boxed.on_state_persisted(&cx).await.is_ok());
        assert_eq!(state.persisted_called.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 0);

        assert_eq!(Arc::strong_count(&state), 2);
        drop(boxed);

        assert_eq!(state.persisted_called.load(Ordering::Acquire), 2);
        assert_eq!(state.dropped.load(Ordering::Acquire), 1);

        assert_eq!(Arc::strong_count(&state), 1);

        Ok(())
    }
}
