use anyhow::Result;
use everscale_types::models::{Block, BlockId};

pub mod provider;
pub mod state;
pub mod subscriber;

use provider::BlockProvider;
use state::BlockStriderState;
use subscriber::BlockSubscriber;

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

fn get_shard_hashes(_block: &Block) -> impl IntoIterator<Item = BlockId> {
    vec![].into_iter()
}

fn get_block_id(_block: &Block) -> BlockId {
    unimplemented!()
}
