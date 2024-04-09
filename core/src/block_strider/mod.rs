use anyhow::{Context, Result};
use everscale_types::models::{BlockId, PrevBlockRef};
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesOrdered;
use futures_util::{FutureExt, TryStreamExt};
use itertools::Itertools;
use std::sync::Arc;
use tokio::time::Instant;

pub mod provider;
pub mod state;
pub mod subscriber;

mod state_applier;

#[cfg(test)]
pub mod test_provider;
#[cfg(test)]
pub(crate) use state_applier::test::prepare_state_apply;

use crate::block_strider::state_applier::ShardStateUpdater;
use provider::BlockProvider;
use state::BlockStriderState;
use subscriber::BlockSubscriber;
use tycho_block_util::block::BlockStuff;
use tycho_block_util::state::MinRefMcStateTracker;
use tycho_storage::Storage;
use tycho_util::FastDashMap;

pub struct BlockStriderBuilder<S, P, B>(BlockStrider<S, P, B>);

impl<T2, T3> BlockStriderBuilder<(), T2, T3> {
    pub fn with_state<S: BlockStriderState>(self, state: S) -> BlockStriderBuilder<S, T2, T3> {
        BlockStriderBuilder(BlockStrider {
            state,
            provider: self.0.provider,
            subscriber: self.0.subscriber,
        })
    }
}

impl<T1, T3> BlockStriderBuilder<T1, (), T3> {
    pub fn with_provider<P: BlockProvider>(self, provider: P) -> BlockStriderBuilder<T1, P, T3> {
        BlockStriderBuilder(BlockStrider {
            state: self.0.state,
            provider,
            subscriber: self.0.subscriber,
        })
    }
}

impl<T1, T2> BlockStriderBuilder<T1, T2, ()> {
    pub fn with_subscriber<B: BlockSubscriber>(
        self,
        subscriber: B,
    ) -> BlockStriderBuilder<T1, T2, B> {
        BlockStriderBuilder(BlockStrider {
            state: self.0.state,
            provider: self.0.provider,
            subscriber,
        })
    }
}

impl<S, P, B> BlockStriderBuilder<S, P, B>
where
    S: BlockStriderState,
    P: BlockProvider,
    B: BlockSubscriber,
{
    pub fn build(self) -> BlockStrider<S, P, B> {
        self.0
    }

    pub fn build_with_state_applier(
        self,
        min_ref_mc_state_tracker: MinRefMcStateTracker,
        storage: Arc<Storage>,
    ) -> BlockStrider<S, P, ShardStateUpdater<B>> {
        BlockStrider {
            state: self.0.state,
            provider: self.0.provider,
            subscriber: ShardStateUpdater::new(
                min_ref_mc_state_tracker,
                storage,
                self.0.subscriber,
            ),
        }
    }
}

pub struct BlockStrider<S, P, B> {
    state: S,
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

        let mut map = BlocksGraph::new();
        while let Some(master_block) = self.fetch_next_master_block().await {
            let master_id = master_block.id();
            tracing::debug!(id=?master_id, "Fetched next master block");
            let extra = master_block.block().load_extra()?;
            let mc_extra = extra
                .load_custom()?
                .with_context(|| format!("failed to load custom for block: {:?}", master_id))?;
            let shard_hashes = mc_extra.shards.latest_blocks();
            // todo: is order important?
            let mut futures = FuturesOrdered::new();

            let start = Instant::now();
            for shard_block_id in shard_hashes {
                let this = &self;
                let blocks_graph = &map;
                let block_id = shard_block_id.expect("Invalid shard block id");
                futures.push_back(async move {
                    this.find_prev_shard_blocks(block_id, blocks_graph).await
                });
            }
            let blocks: Vec<_> = futures
                .try_collect()
                .await
                .expect("failed to collect shard blocks");
            let blocks = blocks.into_iter().flatten().collect_vec();
            let elapsed = start.elapsed();
            metrics::histogram!("tycho_find_prev_shard_blocks_seconds").record(elapsed);

            map.set_bottom_blocks(blocks);
            map.walk_topo(&self.subscriber, &self.state).await;
            self.state.commit_traversed(*master_id);
        }

        tracing::info!("block strider loop finished");
        Ok(())
    }

    fn find_prev_shard_blocks<'a>(
        &'a self,
        mut shard_block_id: BlockId,
        blocks: &'a BlocksGraph,
    ) -> BoxFuture<'a, Result<Vec<BlockId>>> {
        async move {
            let mut prev_shard_block_id = shard_block_id;
            let mut traversed_blocks = Vec::new();

            tracing::debug!(id=?shard_block_id, "Finding prev shard blocks");
            while shard_block_id.seqno > 0 && !self.state.is_traversed(&shard_block_id) {
                prev_shard_block_id = shard_block_id;

                let start = Instant::now();
                let block = self
                    .fetch_block(&shard_block_id)
                    .await
                    .expect("provider failed to fetch shard block");
                let elapsed = start.elapsed();
                metrics::histogram!("tycho_fetch_block_time").record(elapsed);

                tracing::debug!(id=?block.id(), "Fetched shard block");
                let info = block.block().load_info()?;

                match info.load_prev_ref()? {
                    PrevBlockRef::Single(id) => {
                        let shard = if info.after_split {
                            info.shard
                                .merge()
                                .expect("Merge should succeed after split")
                        } else {
                            info.shard
                        };
                        shard_block_id = id.as_block_id(shard);
                        blocks.add_connection(shard_block_id, prev_shard_block_id);
                    }
                    PrevBlockRef::AfterMerge { left, right } => {
                        let (left_shard, right_shard) =
                            info.shard.split().expect("split on unsplitable shard");
                        let left_block_id = left.as_block_id(left_shard);
                        let right_block_id = right.as_block_id(right_shard);
                        blocks.add_connection(left_block_id, prev_shard_block_id);
                        blocks.add_connection(right_block_id, prev_shard_block_id);

                        let left_blocks =
                            self.find_prev_shard_blocks(left_block_id, blocks).await?;
                        let right_blocks =
                            self.find_prev_shard_blocks(right_block_id, blocks).await?;
                        traversed_blocks.extend(left_blocks);
                        traversed_blocks.extend(right_blocks);
                        break;
                    }
                }

                blocks.store_block(block);
            }

            if prev_shard_block_id.seqno > 0 {
                traversed_blocks.push(prev_shard_block_id);
            }

            Ok(traversed_blocks)
        }
        .boxed()
    }

    async fn fetch_next_master_block(&self) -> Option<BlockStuff> {
        let last_traversed_master_block = self.state.load_last_traversed_master_block_id();
        tracing::debug!(?last_traversed_master_block, "Fetching next master block");
        loop {
            match self
                .provider
                .get_next_block(&last_traversed_master_block)
                .await?
            {
                Ok(block) => break Some(block),
                Err(e) => {
                    tracing::error!(
                        ?last_traversed_master_block,
                        "error while fetching master block: {e:?}",
                    );
                    // TODO: backoff
                }
            }
        }
    }

    async fn fetch_block(&self, block_id: &BlockId) -> Result<BlockStuff> {
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

struct BlocksGraph {
    block_store_map: FastDashMap<BlockId, BlockStuff>,
    connections: FastDashMap<BlockId, BlockId>,
    bottom_blocks: Vec<BlockId>,
}

impl BlocksGraph {
    fn new() -> Self {
        Self {
            block_store_map: FastDashMap::default(),
            connections: FastDashMap::default(),
            bottom_blocks: Vec::new(),
        }
    }

    fn store_block(&self, block: BlockStuff) {
        self.block_store_map.insert(*block.id(), block);
    }

    // connection between the block and it child
    fn add_connection(&self, id: BlockId, prev: BlockId) {
        self.connections.insert(id, prev);
    }

    fn set_bottom_blocks(&mut self, blocks: Vec<BlockId>) {
        self.bottom_blocks = blocks;
    }

    async fn walk_topo<Sub>(&mut self, subscriber: &Sub, state: &dyn BlockStriderState)
    where
        Sub: BlockSubscriber + Send + Sync + 'static,
    {
        let mut next_blocks = Vec::with_capacity(self.bottom_blocks.len());
        loop {
            if self.bottom_blocks.is_empty() {
                break;
            }
            self.bottom_blocks.sort_unstable();
            for block_id in &self.bottom_blocks {
                let block = self
                    .block_store_map
                    .get(block_id)
                    .expect("should be in map");
                subscriber
                    .handle_block(&block, None)
                    .await
                    .expect("subscriber failed");
                state.commit_traversed(*block_id);
                let next_block = self.connections.get(block_id);
                if let Some(next_block) = next_block {
                    next_blocks.push(*next_block.value());
                }
            }
            std::mem::swap(&mut next_blocks, &mut self.bottom_blocks);
            next_blocks.clear();
        }
    }
}

#[cfg(test)]
mod test {
    use super::state::InMemoryBlockStriderState;
    use super::subscriber::PrintSubscriber;
    use super::test_provider::TestBlockProvider;
    use crate::block_strider::BlockStrider;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_block_strider() {
        let provider = TestBlockProvider::new(3);
        provider.validate();

        let subscriber = PrintSubscriber;
        let state = InMemoryBlockStriderState::new(provider.first_master_block());

        let strider = BlockStrider::builder()
            .with_state(state)
            .with_provider(provider)
            .with_subscriber(subscriber)
            .build();
        strider.run().await.unwrap();
    }
}
