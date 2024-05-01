use anyhow::Result;
use everscale_types::models::{BlockId, PrevBlockRef};
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::time::Instant;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::{BlockStuff, BlockStuffAug, ShardHeights};
use tycho_block_util::state::MinRefMcStateTracker;
use tycho_storage::Storage;
use tycho_util::FastHashMap;

pub use self::provider::{
    BlockProvider, BlockchainBlockProvider, BlockchainBlockProviderConfig, OptionalBlockStuff,
};
pub use self::state::{BlockStriderState, PersistentBlockStriderState, TempBlockStriderState};
pub use self::state_applier::ShardStateApplier;
pub use self::subscriber::{
    BlockSubscriber, BlockSubscriberContext, BlockSubscriberExt, ChainSubscriber, NoopSubscriber,
    StateSubscriber, StateSubscriberContext, StateSubscriberExt,
};

#[cfg(any(test, feature = "test"))]
pub use self::provider::ArchiveBlockProvider;
#[cfg(any(test, feature = "test"))]
pub use self::subscriber::test::PrintSubscriber;

mod provider;
mod state;
mod state_applier;
mod subscriber;

pub struct BlockStriderBuilder<T, P, B>(BlockStrider<T, P, B>);

impl<T2, T3> BlockStriderBuilder<(), T2, T3> {
    #[inline]
    pub fn with_state<T: BlockStriderState>(self, state: T) -> BlockStriderBuilder<T, T2, T3> {
        BlockStriderBuilder(BlockStrider {
            state,
            provider: self.0.provider,
            subscriber: self.0.subscriber,
        })
    }
}

impl<T1, T3> BlockStriderBuilder<T1, (), T3> {
    #[inline]
    pub fn with_provider<P: BlockProvider>(self, provider: P) -> BlockStriderBuilder<T1, P, T3> {
        BlockStriderBuilder(BlockStrider {
            state: self.0.state,
            provider,
            subscriber: self.0.subscriber,
        })
    }
}

impl<T1, T2> BlockStriderBuilder<T1, T2, ()> {
    #[inline]
    pub fn with_block_subscriber<B>(self, subscriber: B) -> BlockStriderBuilder<T1, T2, B>
    where
        B: BlockSubscriber,
    {
        BlockStriderBuilder(BlockStrider {
            state: self.0.state,
            provider: self.0.provider,
            subscriber,
        })
    }
}

impl<T1, T2> BlockStriderBuilder<T1, T2, ()> {
    pub fn with_state_subscriber<S>(
        self,
        mc_state_tracker: MinRefMcStateTracker,
        storage: Storage,
        state_subscriber: S,
    ) -> BlockStriderBuilder<T1, T2, ShardStateApplier<S>>
    where
        S: StateSubscriber,
    {
        BlockStriderBuilder(BlockStrider {
            state: self.0.state,
            provider: self.0.provider,
            subscriber: ShardStateApplier::new(mc_state_tracker, storage, state_subscriber),
        })
    }
}

impl<T, P, B> BlockStriderBuilder<T, P, B>
where
    T: BlockStriderState,
    P: BlockProvider,
    B: BlockSubscriber,
{
    pub fn build(self) -> BlockStrider<T, P, B> {
        self.0
    }
}

pub struct BlockStrider<T, P, B> {
    state: T,
    provider: P,
    subscriber: B,
}

impl BlockStrider<(), (), ()> {
    pub fn builder() -> BlockStriderBuilder<(), (), ()> {
        BlockStriderBuilder(BlockStrider {
            state: (),
            provider: (),
            subscriber: (),
        })
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

        while let Some(next) = self.fetch_next_master_block().await {
            let started_at = Instant::now();
            self.process_mc_block(next.data, next.archive_data).await?;
            metrics::histogram!("tycho_process_mc_block_time").record(started_at.elapsed());
        }

        tracing::info!("block strider loop finished");
        Ok(())
    }

    /// Processes a single masterchain block and its shard blocks.
    async fn process_mc_block(&self, block: BlockStuff, archive_data: ArchiveData) -> Result<()> {
        let mc_block_id = *block.id();
        tracing::debug!(%mc_block_id, "processing masterchain block");

        let started_at = Instant::now();

        // Start downloading shard blocks
        let mut shard_heights = FastHashMap::default();
        let mut download_futures = FuturesUnordered::new();
        for entry in block.load_custom()?.shards.latest_blocks() {
            let top_block_id = entry?;
            shard_heights.insert(top_block_id.shard, top_block_id.seqno);
            download_futures.push(Box::pin(self.download_shard_blocks(top_block_id)));
        }

        // Start processing shard blocks in parallel
        let mut process_futures = FuturesUnordered::new();
        while let Some(blocks) = download_futures.next().await.transpose()? {
            process_futures.push(Box::pin(self.process_shard_blocks(&mc_block_id, blocks)));
        }
        metrics::histogram!("tycho_download_shard_blocks_time").record(started_at.elapsed());

        // Wait for all shard blocks to be processed
        while process_futures.next().await.transpose()?.is_some() {}
        metrics::histogram!("tycho_process_shard_blocks_time").record(started_at.elapsed());

        // Process masterchain block
        let cx = BlockSubscriberContext {
            mc_block_id,
            block,
            archive_data,
        };
        self.subscriber.handle_block(&cx).await?;

        let shard_heights = ShardHeights::from(shard_heights);
        self.state.commit_master(&mc_block_id, &shard_heights);

        Ok(())
    }

    /// Downloads blocks for the single shard in descending order starting from the top block.
    async fn download_shard_blocks(&self, mut top_block_id: BlockId) -> Result<Vec<BlockStuffAug>> {
        const MAX_DEPTH: u32 = 32;

        tracing::debug!(%top_block_id, "downloading shard blocks");

        let mut depth = 0;
        let mut result = Vec::new();
        while top_block_id.seqno > 0 && !self.state.is_commited(&top_block_id) {
            // Download block
            let started_at = Instant::now();
            let block = self.fetch_block(&top_block_id).await?;
            tracing::debug!(block_id = %top_block_id, "fetched shard block");
            debug_assert_eq!(block.id(), &top_block_id);

            metrics::histogram!("tycho_fetch_shard_block_time").record(started_at.elapsed());

            // Parse info in advance to make borrow checker happy
            let info = block.data.load_info()?;

            // Add new block to result
            result.push(block);

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
        mut blocks: Vec<BlockStuffAug>,
    ) -> Result<()> {
        while let Some(block) = blocks.pop() {
            let block_id = *block.id();

            let cx: BlockSubscriberContext = BlockSubscriberContext {
                mc_block_id: *mc_block_id,
                block: block.data,
                archive_data: block.archive_data,
            };

            let started_at = Instant::now();
            self.subscriber.handle_block(&cx).await?;
            metrics::histogram!("tycho_process_shard_block_time").record(started_at.elapsed());

            self.state.commit_shard(&block_id);
        }

        Ok(())
    }

    async fn fetch_next_master_block(&self) -> Option<BlockStuffAug> {
        let prev_block_id = self.state.load_last_mc_block_id();
        tracing::debug!(%prev_block_id, "fetching next master block");

        loop {
            match self.provider.get_next_block(&prev_block_id).await? {
                Ok(block) => break Some(block),
                Err(e) => {
                    tracing::error!(?prev_block_id, "error while fetching master block: {e:?}",);
                    // TODO: backoff
                }
            }
        }
    }

    async fn fetch_block(&self, block_id: &BlockId) -> Result<BlockStuffAug> {
        loop {
            match self.provider.get_block(block_id).await {
                Some(Ok(block)) => break Ok(block),
                Some(Err(e)) => {
                    tracing::error!("error while fetching block: {e:?}");
                    // TODO: backoff
                }
                None => {
                    anyhow::bail!("block not found: {block_id}")
                }
            }
        }
    }
}
