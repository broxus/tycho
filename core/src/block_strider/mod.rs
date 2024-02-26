use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use everscale_types::models::{Block, BlockId};
use futures_util::future::BoxFuture;

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
    pub fn with_subscriber<B: BlockStriderState>(
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

        while let Some(master_block) = self.fetch_next_master_block().await {
            // TODO: replace with block stuff
            let master_id = get_block_id(&master_block);
            let shard_hashes = get_shard_hashes(&master_block);

            for hash in shard_hashes {
                if !self.state.is_traversed(&hash) {
                    let block = self.fetch_block(&hash).await?;

                    if let Err(e) = self.subscriber.handle_block(&block).await {
                        tracing::error!("error while handling block: {e:?}");
                        // TODO: retry + backoff?
                    }

                    self.state.commit_traversed(hash);
                }
            }

            self.state.commit_traversed(master_id);
        }

        tracing::info!("block strider loop finished");
        Ok(())
    }

    async fn fetch_next_master_block(&self) -> Option<Block> {
        let last_traversed_master_block = self.state.load_last_traversed_master_block_id();
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

    async fn fetch_block(&self, block_id: &BlockId) -> Result<Block> {
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

pub trait BlockStriderState: Send + Sync + 'static {
    fn load_last_traversed_master_block_id(&self) -> BlockId;
    fn is_traversed(&self, block_id: &BlockId) -> bool;
    fn commit_traversed(&self, block_id: BlockId);
}

impl<T: BlockStriderState> BlockStriderState for Box<T> {
    fn load_last_traversed_master_block_id(&self) -> BlockId {
        <T as BlockStriderState>::load_last_traversed_master_block_id(self)
    }

    fn is_traversed(&self, block_id: &BlockId) -> bool {
        <T as BlockStriderState>::is_traversed(self, block_id)
    }

    fn commit_traversed(&self, block_id: BlockId) {
        <T as BlockStriderState>::commit_traversed(self, block_id);
    }
}

/// Block provider *MUST* validate the block before returning it.
pub trait BlockProvider: Send + Sync + 'static {
    type GetNextBlockFut<'a>: Future<Output = Option<Result<Block>>> + Send + 'a;
    type GetBlockFut<'a>: Future<Output = Option<Result<Block>>> + Send + 'a;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a>;
    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a>;
}

impl<T: BlockProvider> BlockProvider for Box<T> {
    type GetNextBlockFut<'a> = T::GetNextBlockFut<'a>;
    type GetBlockFut<'a> = T::GetBlockFut<'a>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        <T as BlockProvider>::get_next_block(self, prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        <T as BlockProvider>::get_block(self, block_id)
    }
}

pub trait BlockSubscriber: Send + Sync + 'static {
    type HandleBlockFut: Future<Output = Result<()>> + Send + 'static;

    fn handle_block(&self, block: &Block) -> Self::HandleBlockFut;
}

impl<T: BlockSubscriber> BlockSubscriber for Box<T> {
    type HandleBlockFut = T::HandleBlockFut;

    fn handle_block(&self, block: &Block) -> Self::HandleBlockFut {
        <T as BlockSubscriber>::handle_block(self, block)
    }
}

fn get_shard_hashes(_block: &Block) -> impl IntoIterator<Item = BlockId> {
    vec![].into_iter()
}

fn get_block_id(_block: &Block) -> BlockId {
    unimplemented!()
}

// === Provider combinators ===
struct ChainBlockProvider<T1, T2> {
    left: T1,
    right: T2,
    is_right: AtomicBool,
}

impl<T1: BlockProvider, T2: BlockProvider> BlockProvider for ChainBlockProvider<T1, T2> {
    type GetNextBlockFut<'a> = BoxFuture<'a, Option<Result<Block>>>;
    type GetBlockFut<'a> = BoxFuture<'a, Option<Result<Block>>>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(async move {
            if !self.is_right.load(Ordering::Acquire) {
                let res = self.left.get_next_block(prev_block_id).await;
                if res.is_some() {
                    return res;
                }
                self.is_right.store(true, Ordering::Release);
            }
            self.right.get_next_block(prev_block_id).await
        })
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(async {
            let res = self.left.get_block(block_id).await;
            if res.is_some() {
                return res;
            }
            self.right.get_block(block_id).await
        })
    }
}
