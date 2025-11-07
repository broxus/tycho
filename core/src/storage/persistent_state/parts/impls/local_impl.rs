use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use tycho_block_util::block::DisplayShardPrefix;
use tycho_storage::fs::Dir;
use tycho_types::models::BlockId;

use crate::storage::persistent_state::BASE_DIR;
use crate::storage::persistent_state::descriptor_cache::{
    DescriptorCache, ReusePersistentStateResult,
};
use crate::storage::persistent_state::parts::{
    PersistentStateStoragePart, StoreStatePartContext, StoreStatePartResult,
};
use crate::storage::shard_state::ShardStateStoragePart;
use crate::storage::{PersistentState, PersistentStateKind, ShardStateWriter};

pub struct PersistentStateStoragePartLocalImpl {
    inner: Arc<Inner>,
}

impl PersistentStateStoragePart for PersistentStateStoragePartLocalImpl {
    fn try_reuse_persistent_state(
        &self,
        mc_seqno: u32,
        block_id: BlockId,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send>> {
        let fut = self
            .inner
            .clone()
            .try_reuse_persistent_state(mc_seqno, block_id);
        Box::pin(fut)
    }
    fn store_shard_state_part(
        &self,
        cx: StoreStatePartContext,
    ) -> Pin<Box<dyn Future<Output = Result<Option<StoreStatePartResult>>> + Send>> {
        let fut = self.inner.clone().store_shard_state_part_impl(cx);
        Box::pin(fut)
    }
}

impl PersistentStateStoragePartLocalImpl {
    pub fn new(files_dir: &Dir, shard_states_part: Arc<dyn ShardStateStoragePart>) -> Result<Self> {
        let storage_dir = files_dir.create_subdir(BASE_DIR)?;

        Ok(Self {
            inner: Arc::new(Inner {
                shard_states_part,
                descriptor_cache: DescriptorCache::new(storage_dir),
            }),
        })
    }
}

struct Inner {
    shard_states_part: Arc<dyn ShardStateStoragePart>,
    descriptor_cache: DescriptorCache,
}

impl Inner {
    async fn try_reuse_persistent_state(
        self: Arc<Self>,
        mc_seqno: u32,
        block_id: BlockId,
    ) -> Result<bool> {
        let reused = self
            .try_reuse_persistent_state_inner(mc_seqno, block_id)
            .await?;
        Ok(reused.is_some())
    }

    async fn try_reuse_persistent_state_inner(
        &self,
        mc_seqno: u32,
        block_id: BlockId,
    ) -> Result<Option<ReusePersistentStateResult>> {
        self.descriptor_cache
            .try_reuse_persistent_state(mc_seqno, block_id, PersistentStateKind::Shard, None)
            .await
    }

    async fn store_shard_state_part_impl(
        self: Arc<Self>,
        cx: StoreStatePartContext,
    ) -> Result<Option<StoreStatePartResult>> {
        let StoreStatePartContext {
            mc_seqno,
            block_id,
            root_hash,
            tracker_handle,
            cancelled,
        } = cx;

        let into_store_result = |cached: Option<PersistentState>| {
            cached.map(|ps| StoreStatePartResult {
                prefix: self.shard_states_part.shard_prefix(),
                file_size: ps.file().len(),
            })
        };

        // try reuse
        let reused = self
            .try_reuse_persistent_state_inner(mc_seqno, block_id)
            .await?;
        if reused.is_some() {
            return Ok(into_store_result(reused.map(|r| r.into_state())));
        }

        // store
        let this = self.clone();

        let span = tracing::Span::current();
        let state = tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("part cancelled");
            });

            // NOTE: Ensure that the tracker handle will outlive the state writer.
            let _tracker_handle = tracker_handle;

            let states_dir = this
                .descriptor_cache
                .prepare_persistent_states_dir(mc_seqno)?;
            let writer = ShardStateWriter::new_for_shard(
                this.shard_states_part.cells_db().clone(),
                &states_dir,
                &block_id,
                this.shard_states_part.shard_prefix(),
            );

            let stored = match writer.write(&root_hash, cancelled.as_ref()) {
                Ok(()) => {
                    tracing::info!(
                        "persistent shard state part {} saved",
                        DisplayShardPrefix(&this.shard_states_part.shard_prefix()),
                    );
                    true
                }
                Err(e) => {
                    // NOTE: We are ignoring an error here. It might be intentional
                    tracing::error!(
                        "failed to write persistent shard state part {}: {:?}",
                        DisplayShardPrefix(&this.shard_states_part.shard_prefix()),
                        e,
                    );
                    false
                }
            };

            let mut res = None;

            // cache state part
            if stored {
                let cache_res = this.descriptor_cache.cache_shard_state(
                    mc_seqno,
                    &block_id,
                    Some(this.shard_states_part.shard_prefix()),
                    None,
                )?;
                res = Some(cache_res);
            }

            scopeguard::ScopeGuard::into_inner(guard);
            Ok::<_, anyhow::Error>(res)
        })
        .await??;

        Ok(into_store_result(state))
    }
}
