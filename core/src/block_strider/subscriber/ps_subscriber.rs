use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
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
        let last_key_block_utime = storage
            .block_handle_storage()
            .find_last_key_block()
            .map_or(0, |handle| handle.gen_utime());

        Self {
            inner: Arc::new(Inner {
                last_key_block_utime: AtomicU32::new(last_key_block_utime),
                storage,
            }),
        }
    }
}

impl StateSubscriber for PsSubscriber {
    type HandleStateFut<'a> = futures_util::future::Ready<Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        if cx.is_key_block {
            let block_info = cx.block.load_info().unwrap();

            let prev_utime = self
                .inner
                .last_key_block_utime
                .swap(block_info.gen_utime, Ordering::Relaxed);
            let is_persistent = BlockStuff::compute_is_persistent(block_info.gen_utime, prev_utime);

            if is_persistent && cx.block.id().seqno != 0 {
                let block = cx.block.clone();
                let inner = self.inner.clone();
                let state_handle = cx.state.ref_mc_state_handle().clone();
                tokio::spawn(async move {
                    if let Err(e) = inner.save_impl(block, state_handle).await {
                        tracing::error!("failed to save persistent states: {e}");
                    }
                });
            }
        }

        futures_util::future::ready(Ok(()))
    }
}

struct Inner {
    last_key_block_utime: AtomicU32,
    storage: CoreStorage,
}

impl Inner {
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
            .await
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
        }

        // NOTE: We intentionally store the masterchain state last to ensure that
        //       the handle will live long enough. And this way we don't mislead
        //       other nodes with the incomplete set of persistent states.
        persistent_states
            .store_shard_state(mc_seqno, &mc_block_handle, mc_state_handle)
            .await
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
        }

        persistent_states
            .store_queue_state(mc_seqno, &mc_block_handle, mc_block)
            .await
    }
}
