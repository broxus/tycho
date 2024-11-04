use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::queue::QueueKey;
use tycho_block_util::state::RefMcStateHandle;
use tycho_storage::{BlockHandle, Storage};

use crate::block_strider::{StateSubscriber, StateSubscriberContext};

/// Persistent state subscriber.
#[derive(Clone)]
pub struct PsSubscriber {
    inner: Arc<Inner>,
}

impl PsSubscriber {
    pub fn new(storage: Storage) -> Self {
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
    storage: Storage,
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

        // Compute the minimal referenced LT for each shard
        let mut min_processed_upto = BTreeMap::new();
        let merge = |block_handle: BlockHandle,
                     mut processed_upto: BTreeMap<ShardIdent, QueueKey>| async move {
            let queue_diff = blocks.load_queue_diff(&block_handle).await?;
            for (&shard, &key) in &queue_diff.as_ref().processed_upto {
                let existing = processed_upto.entry(shard).or_insert(key);
                *existing = std::cmp::min(*existing, key);
            }
            Ok::<_, anyhow::Error>(processed_upto)
        };

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

            min_processed_upto = merge(block_handle.clone(), min_processed_upto).await?;
            shard_block_handles.push(block_handle);
        }
        min_processed_upto = merge(mc_block_handle.clone(), min_processed_upto).await?;

        // Store queue state for each shard
        let mc_seqno = mc_block_handle.id().seqno;
        for block_handle in shard_block_handles {
            let block = blocks.load_block_data(&block_handle).await?;

            let min_shard = min_processed_upto
                .get(&block.id().shard)
                .copied()
                .unwrap_or_default();
            persistent_states
                .store_queue_state(mc_seqno, &block_handle, block, min_shard.lt)
                .await?;
        }

        // Store queue state for masterchain
        let min_mc = min_processed_upto
            .get(&ShardIdent::MASTERCHAIN)
            .copied()
            .unwrap_or_default();
        persistent_states
            .store_queue_state(mc_seqno, &mc_block_handle, mc_block, min_mc.lt)
            .await
    }
}
