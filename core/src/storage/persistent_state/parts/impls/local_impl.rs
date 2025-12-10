use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::Semaphore;
use tycho_block_util::block::DisplayShardPrefix;
use tycho_storage::fs::Dir;
use tycho_types::cell::HashBytes;
use tycho_types::models::BlockId;

use crate::storage::persistent_state::BASE_DIR;
use crate::storage::persistent_state::descriptor_cache::{
    CacheKey, DescriptorCache, PersistentState, ReusePersistentStateResult,
};
use crate::storage::persistent_state::parts::{
    PersistentStateStoragePart, StoreStatePartContext, StoreStatePartFileContext,
    StoreStatePartResult,
};
use crate::storage::shard_state::ShardStateStoragePart;
use crate::storage::{PersistentStateKind, ShardStateWriter};

pub struct PersistentStateStoragePartLocalImpl {
    inner: Arc<Inner>,
}

impl PersistentStateStoragePart for PersistentStateStoragePartLocalImpl {
    fn preload_state(
        &self,
        mc_seqno: u32,
        block_id: &BlockId,
        part_root_hash: HashBytes,
    ) -> Result<()> {
        self.inner.preload_state(mc_seqno, block_id, part_root_hash)
    }

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

    fn store_shard_state_part_file(
        &self,
        cx: StoreStatePartFileContext,
    ) -> Pin<Box<dyn Future<Output = Result<Option<StoreStatePartResult>>> + Send>> {
        let fut = self.inner.clone().store_shard_state_part_file_impl(cx);
        Box::pin(fut)
    }

    fn state_part_size(&self, block_id: &BlockId) -> Result<Option<NonZeroU64>> {
        self.inner.state_part_size_impl(block_id)
    }

    fn read_state_part_chunk(
        &self,
        block_id: &BlockId,
        offset: u64,
        chunk_size: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>>> + Send>> {
        let fut = self
            .inner
            .clone()
            .read_state_part_chunk_impl(*block_id, offset, chunk_size);
        Box::pin(fut)
    }
}

impl PersistentStateStoragePartLocalImpl {
    pub fn new(files_dir: &Dir, shard_states_part: Arc<dyn ShardStateStoragePart>) -> Result<Self> {
        const MAX_PARALLEL_CHUNK_READS: usize = 20;

        let storage_dir = files_dir.create_subdir(BASE_DIR)?;

        Ok(Self {
            inner: Arc::new(Inner {
                shard_states_part,
                descriptor_cache: DescriptorCache::new(storage_dir),
                chunks_semaphore: Arc::new(Semaphore::new(MAX_PARALLEL_CHUNK_READS)),
            }),
        })
    }
}

struct Inner {
    shard_states_part: Arc<dyn ShardStateStoragePart>,
    descriptor_cache: DescriptorCache,
    chunks_semaphore: Arc<Semaphore>,
}

impl Inner {
    fn preload_state(
        &self,
        mc_seqno: u32,
        block_id: &BlockId,
        part_root_hash: HashBytes,
    ) -> Result<()> {
        self.descriptor_cache.cache_shard_state(
            mc_seqno,
            block_id,
            Some(self.shard_states_part.shard_prefix()),
            Some(part_root_hash),
            None,
        )?;
        Ok(())
    }

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

        // try reuse
        let reused = self
            .try_reuse_persistent_state_inner(mc_seqno, block_id)
            .await?;
        if reused.is_some() {
            return Ok(self.make_store_result(reused.map(|r| r.into_state())));
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
                Ok(_) => {
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
                    Some(root_hash),
                    None,
                )?;
                res = Some(cache_res);
            }

            scopeguard::ScopeGuard::into_inner(guard);
            Ok::<_, anyhow::Error>(res)
        })
        .await??;

        Ok(self.make_store_result(state))
    }

    fn make_store_result(&self, cached: Option<PersistentState>) -> Option<StoreStatePartResult> {
        cached.map(|ps| StoreStatePartResult {
            prefix: self.shard_states_part.shard_prefix(),
            file_size: ps.file().len(),
        })
    }

    async fn store_shard_state_part_file_impl(
        self: Arc<Self>,
        cx: StoreStatePartFileContext,
    ) -> Result<Option<StoreStatePartResult>> {
        let StoreStatePartFileContext {
            mc_seqno,
            block_id,
            root_hash,
            file,
            cancelled,
        } = cx;

        let this = self.clone();
        let span = tracing::Span::current();
        let cached = tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let guard = scopeguard::guard((), |_| {
                tracing::warn!("part file cancelled");
            });

            let states_dir = this
                .descriptor_cache
                .prepare_persistent_states_dir(mc_seqno)?;
            let writer = ShardStateWriter::new_for_shard(
                this.shard_states_part.cells_db().clone(),
                &states_dir,
                &block_id,
                this.shard_states_part.shard_prefix(),
            );

            let stored = match writer.write_file(file, cancelled.as_ref()) {
                Ok(()) => {
                    tracing::info!(
                        "persistent shard state part {} file saved",
                        DisplayShardPrefix(&this.shard_states_part.shard_prefix()),
                    );
                    true
                }
                Err(e) => {
                    // NOTE: We are ignoring an error here. It might be intentional
                    tracing::error!(
                        "failed to write persistent shard state part {} file: {:?}",
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
                    Some(root_hash),
                    None,
                )?;
                res = Some(cache_res);
            }

            scopeguard::ScopeGuard::into_inner(guard);
            Ok::<_, anyhow::Error>(res)
        })
        .await??;

        Ok(self.make_store_result(cached))
    }

    fn state_part_size_impl(&self, block_id: &BlockId) -> Result<Option<NonZeroU64>> {
        let cached = self
            .descriptor_cache
            .get(&CacheKey::from((*block_id, PersistentStateKind::Shard)));
        Ok(cached.and_then(|cs| NonZeroU64::new(cs.file.length() as u64)))
    }

    async fn read_state_part_chunk_impl(
        self: Arc<Self>,
        block_id: BlockId,
        offset: u64,
        chunk_size: usize,
    ) -> Result<Option<Vec<u8>>> {
        let offset = usize::try_from(offset)?;

        let permit = {
            let semaphore = self.chunks_semaphore.clone();
            semaphore.acquire_owned().await?
        };

        let key = CacheKey::from((block_id, PersistentStateKind::Shard));
        let cached = self.descriptor_cache.get(&key).with_context(|| {
            format!(
                "persistent shard state part {} not found in cache",
                DisplayShardPrefix(&self.shard_states_part.shard_prefix()),
            )
        })?;

        if offset > cached.file.length() {
            return Ok(None);
        }

        let data = tokio::task::spawn_blocking(move || {
            // Ensure that permit is dropped only after cached state is used.
            let _permit = permit;

            cached.file.read_chunk(offset, chunk_size)
        })
        .await?;

        Ok(data)
    }
}
