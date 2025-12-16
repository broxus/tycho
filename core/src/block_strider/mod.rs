use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::Future;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::time::Instant;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::{
    BlockIdExt, BlockIdRelation, BlockStuff, BlockStuffAug, ShardHeights,
};
use tycho_types::models::{BlockId, PrevBlockRef};
use tycho_util::FastHashMap;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;

pub use self::archive_handler::ArchiveHandler;
pub use self::block_saver::BlockSaver;
pub use self::provider::{
    ArchiveBlockProvider, ArchiveBlockProviderConfig, ArchiveClient, ArchiveDownloadContext,
    ArchiveResponse, BlockProvider, BlockProviderExt, BlockchainBlockProvider,
    BlockchainBlockProviderConfig, BoxBlockProvider, ChainBlockProvider, CheckProof,
    CycleBlockProvider, EmptyBlockProvider, FoundArchive, HybridArchiveClient, IntoArchiveClient,
    OptionalBlockStuff, ProofChecker, RetryBlockProvider, RetryConfig, StorageBlockProvider,
};
pub use self::starter::{
    ColdBootType, FileZerostateProvider, QueueStateHandler, Starter, StarterBuilder, StarterConfig,
    ValidateQueueState, ZerostateProvider,
};
use self::state::UpdateGcState;
pub use self::state::{
    BlockStriderState, CommitMasterBlock, CommitShardBlock, PersistentBlockStriderState,
    TempBlockStriderState,
};
pub use self::state_applier::ShardStateApplier;
#[cfg(any(test, feature = "test"))]
pub use self::subscriber::test::PrintSubscriber;
pub use self::subscriber::{
    ArchiveSubscriber, ArchiveSubscriberContext, ArchiveSubscriberExt, BlockSubscriber,
    BlockSubscriberContext, BlockSubscriberExt, ChainSubscriber, DelayedTasks,
    DelayedTasksJoinHandle, DelayedTasksSpawner, MetricsSubscriber, NoopSubscriber,
    StateSubscriber, StateSubscriberContext, StateSubscriberExt,
};
use crate::storage::CoreStorage;

mod archive_handler;
mod block_saver;
mod provider;
mod starter;
mod state;
mod state_applier;
mod subscriber;

pub struct BlockStriderBuilder<T, P, B> {
    state: T,
    provider: P,
    subscriber: B,
}

impl<T2, T3> BlockStriderBuilder<(), T2, T3> {
    #[inline]
    pub fn with_state<T: BlockStriderState>(self, state: T) -> BlockStriderBuilder<T, T2, T3> {
        BlockStriderBuilder {
            state,
            provider: self.provider,
            subscriber: self.subscriber,
        }
    }
}

impl<T1, T3> BlockStriderBuilder<T1, (), T3> {
    #[inline]
    pub fn with_provider<P: BlockProvider>(self, provider: P) -> BlockStriderBuilder<T1, P, T3> {
        BlockStriderBuilder {
            state: self.state,
            provider,
            subscriber: self.subscriber,
        }
    }
}

impl<T1, T2> BlockStriderBuilder<T1, T2, ()> {
    #[inline]
    pub fn with_block_subscriber<B>(self, subscriber: B) -> BlockStriderBuilder<T1, T2, B>
    where
        B: BlockSubscriber,
    {
        BlockStriderBuilder {
            state: self.state,
            provider: self.provider,
            subscriber,
        }
    }
}

impl<T1, T2> BlockStriderBuilder<T1, T2, ()> {
    pub fn with_state_subscriber<S>(
        self,
        storage: CoreStorage,
        state_subscriber: S,
    ) -> BlockStriderBuilder<T1, T2, ShardStateApplier<S>>
    where
        S: StateSubscriber,
    {
        BlockStriderBuilder {
            state: self.state,
            provider: self.provider,
            subscriber: ShardStateApplier::new(storage, state_subscriber),
        }
    }
}

impl<T, P, B> BlockStriderBuilder<T, P, B>
where
    T: BlockStriderState,
    P: BlockProvider,
    B: BlockSubscriber,
{
    pub fn build(self) -> BlockStrider<T, P, B> {
        BlockStrider {
            state: self.state,
            provider: Arc::new(self.provider),
            subscriber: Arc::new(self.subscriber),
        }
    }
}

pub struct BlockStrider<T, P, B> {
    state: T,
    provider: Arc<P>,
    subscriber: Arc<B>,
}

impl BlockStrider<(), (), ()> {
    pub fn builder() -> BlockStriderBuilder<(), (), ()> {
        BlockStriderBuilder {
            state: (),
            provider: (),
            subscriber: (),
        }
    }
}

impl<S, P, B> BlockStrider<S, P, B>
where
    S: BlockStriderState,
    P: BlockProvider,
    B: BlockSubscriber,
{
    /// Walks through blocks and handles them.
    ///
    /// Stops either when the provider is exhausted or it can't provide a requested block.
    pub async fn run(self) -> Result<()> {
        tracing::info!("block strider loop started");

        let mut next_master_fut =
            JoinTask::new(self.fetch_next_master_block(&self.state.load_last_mc_block_id()));

        while let Some(next) = next_master_fut.await.transpose()? {
            // NOTE: Start fetching the next master block in parallel to the processing of the current one
            // If we have a chain of providers, when switching to the next one, since blocks are processed
            // asynchronously and in parallel with requesting the next block, the processing of the
            // previous block may already use the new provider. Therefore, the next provider must
            // necessarily store the previous block.
            next_master_fut = JoinTask::new(self.fetch_next_master_block(next.id()));

            let mc_seqno = next.id().seqno;

            {
                let _histogram = HistogramGuard::begin("tycho_core_process_strider_step_time");
                self.process_mc_block(next.data, next.archive_data).await?;
            }

            {
                let _histogram = HistogramGuard::begin("tycho_core_provider_cleanup_time");
                self.provider.cleanup_until(mc_seqno).await?;
            }
        }

        tracing::info!("block strider loop finished");
        Ok(())
    }

    /// Processes a single masterchain block and its shard blocks.
    async fn process_mc_block(&self, block: BlockStuff, archive_data: ArchiveData) -> Result<()> {
        let mc_block_id = *block.id();
        tracing::debug!(%mc_block_id, "processing masterchain block");

        let started_at = Instant::now();

        let info = block.load_info()?;
        metrics::gauge!("tycho_core_last_mc_block_utime").set(info.gen_utime);
        metrics::gauge!("tycho_core_last_mc_block_seqno").set(mc_block_id.seqno);

        let custom = block.load_custom()?;
        let is_key_block = custom.config.is_some();

        // Begin preparing master block in the background
        let (delayed_handle, delayed) = DelayedTasks::new();
        let prepared_master = {
            let cx = Box::new(BlockSubscriberContext {
                mc_block_id,
                mc_is_key_block: is_key_block,
                is_key_block,
                block: block.clone(),
                archive_data,
                delayed,
            });
            let subscriber = self.subscriber.clone();
            JoinTask::new(async move {
                let _histogram = HistogramGuard::begin("tycho_core_prepare_mc_block_time");
                let prepared = subscriber.prepare_block(&cx).await;
                (cx, prepared)
            })
        };

        // Start downloading shard blocks
        let mut shard_heights = FastHashMap::default();
        let mut download_futures = FuturesUnordered::new();
        for entry in custom.shards.latest_blocks() {
            let top_block_id = entry?;
            shard_heights.insert(top_block_id.shard, top_block_id.seqno);
            download_futures.push(Box::pin(
                self.download_shard_blocks(mc_block_id, top_block_id),
            ));
        }

        // Start processing shard blocks in parallel
        let mut process_futures = FuturesUnordered::new();
        while let Some(blocks) = download_futures.next().await.transpose()? {
            process_futures.push(Box::pin(self.process_shard_blocks(
                &mc_block_id,
                is_key_block,
                blocks,
            )));
        }
        metrics::histogram!("tycho_core_download_sc_blocks_time").record(started_at.elapsed());

        // Wait for all shard blocks to be processed
        while process_futures.next().await.transpose()?.is_some() {}
        metrics::histogram!("tycho_core_process_sc_blocks_time").record(started_at.elapsed());

        // Finally handle the masterchain block
        let delayed_handle = delayed_handle.spawn();
        let (cx, prepared) = prepared_master.await;

        let _histogram = HistogramGuard::begin("tycho_core_process_mc_block_time");
        self.subscriber.handle_block(&cx, prepared?).await?;

        // Update GC state.
        self.state.update_gc_state(UpdateGcState {
            mc_block_id: &cx.mc_block_id,
            mc_is_key_block: cx.is_key_block,
            is_key_block: cx.is_key_block,
            block: &cx.block,
        })?;

        // Join delayed tasks after all processing is done.
        delayed_handle.join().await?;

        // Commit only when everything is ok.
        let shard_heights = ShardHeights::from(shard_heights);
        self.state.commit_master(CommitMasterBlock {
            block_id: &mc_block_id,
            is_key_block,
            shard_heights: &shard_heights,
        });

        // Update "applied" only after commit
        metrics::gauge!("tycho_core_last_mc_block_applied")
            .set(tycho_util::time::now_millis() as f64 / 1000.0);

        Ok(())
    }

    /// Downloads blocks for the single shard in descending order starting from the top block.
    async fn download_shard_blocks(
        &self,
        mc_block_id: BlockId,
        mut top_block_id: BlockId,
    ) -> Result<Vec<BlockStuffAug>> {
        const MAX_DEPTH: u32 = 32;

        tracing::debug!(
            mc_block_id = %mc_block_id.as_short_id(),
            %top_block_id,
            "downloading shard blocks"
        );

        let mut depth = 0;
        let mut result = Vec::new();
        while top_block_id.seqno > 0 && !self.state.is_committed(&top_block_id) {
            // Download block
            let block = {
                let _histogram = HistogramGuard::begin("tycho_core_download_sc_block_time");

                let block = self
                    .fetch_block(&top_block_id.relative_to(mc_block_id))
                    .await?;
                tracing::debug!(
                    mc_block_id = %mc_block_id.as_short_id(),
                    block_id = %top_block_id,
                    "fetched shard block",
                );
                debug_assert_eq!(block.id(), &top_block_id);

                block
            };

            // Parse info in advance to make borrow checker happy
            let info = block.data.load_info()?;

            // Add new block to result
            result.push(block.clone());

            // Process block refs
            if info.after_split || info.after_merge {
                // Blocks after split or merge are always the first blocks after
                // the previous master block
                break;
            }

            match info.load_prev_ref()? {
                PrevBlockRef::Single(id) => top_block_id = id.as_block_id(top_block_id.shard),
                PrevBlockRef::AfterMerge { .. } => anyhow::bail!("unexpected `AfterMerge` ref"),
            }

            depth += 1;
            if depth >= MAX_DEPTH {
                anyhow::bail!("max depth reached");
            }
        }

        Ok(result)
    }

    async fn process_shard_blocks(
        &self,
        mc_block_id: &BlockId,
        mc_is_key_block: bool,
        mut blocks: Vec<BlockStuffAug>,
    ) -> Result<()> {
        let start_preparing_block = |block: BlockStuffAug| {
            let (delayed_handle, delayed) = DelayedTasks::new();

            let cx = Box::new(BlockSubscriberContext {
                mc_block_id: *mc_block_id,
                mc_is_key_block,
                is_key_block: false,
                block: block.data,
                archive_data: block.archive_data,
                delayed,
            });
            let subscriber = self.subscriber.clone();
            JoinTask::new(async move {
                let _histogram = HistogramGuard::begin("tycho_core_prepare_sc_block_time");

                let prepared = subscriber.prepare_block(&cx).await;
                (delayed_handle, cx, prepared)
            })
        };

        let mut prepare_task = blocks.pop().map(start_preparing_block);

        while let Some(prepared) = prepare_task.take() {
            let (delayed_handle, cx, prepared) = prepared.await;
            let delayed_handle = delayed_handle.spawn();

            prepare_task = blocks.pop().map(start_preparing_block);

            let _histogram = HistogramGuard::begin("tycho_core_process_sc_block_time");

            // Handle block by subscribers chain.
            self.subscriber.handle_block(&cx, prepared?).await?;

            // Update GC state.
            self.state.update_gc_state(UpdateGcState {
                mc_block_id,
                mc_is_key_block,
                is_key_block: cx.is_key_block,
                block: &cx.block,
            })?;

            // Join delayed tasks after all processing is done.
            delayed_handle.join().await?;

            // Commit only when everything is ok.
            self.state.commit_shard(CommitShardBlock {
                block_id: cx.block.id(),
            });
        }

        Ok(())
    }

    fn fetch_next_master_block(
        &self,
        prev_block_id: &BlockId,
    ) -> impl Future<Output = OptionalBlockStuff> + Send + 'static {
        let _histogram = HistogramGuard::begin("tycho_core_download_mc_block_time");

        tracing::debug!(%prev_block_id, "fetching next master block");

        let provider = self.provider.clone();
        let prev_block_id = *prev_block_id;
        async move {
            let res = provider.get_next_block(&prev_block_id).await?;
            Some(res.with_context(|| {
                format!(
                    "BUGGY PROVIDER. failed to fetch next master block after prev: {prev_block_id}"
                )
            }))
        }
    }

    async fn fetch_block(&self, block_id_relation: &BlockIdRelation) -> Result<BlockStuffAug> {
        match self.provider.get_block(block_id_relation).await {
            Some(Ok(block)) => Ok(block),
            Some(Err(e)) => {
                anyhow::bail!(
                    "BUGGY PROVIDER. failed to fetch block for {block_id_relation:?}: {e:?}"
                )
            }
            None => {
                anyhow::bail!("BUGGY PROVIDER. block not found for {block_id_relation:?}")
            }
        }
    }
}
