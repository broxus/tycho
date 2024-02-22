use async_trait::async_trait;

use everscale_types::models::{Block, BlockId};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::future::Future;

#[async_trait]
pub trait OnStriderStep {
    async fn handle_block(&self, block: &Block) -> anyhow::Result<()>;
}

/// Block provider *MUST* validate the block before returning it.
pub trait BlockProvider {
    type GetNextBlockFut: Future<Output = ProviderResult>;
    type GetBlockFut: Future<Output = ProviderResult>;

    fn get_next_block(&self, prev_block_id: &BlockId) -> Self::GetNextBlockFut;
    fn get_block(&self, block_id: &BlockId) -> Self::GetBlockFut;

    fn status(&self) -> ProviderStatus;
}

pub trait PersistenceProvider {
    fn load_last_traversed_master_block_seqno(&self) -> BlockId;
    fn commit_last_traversed_master_block_seqno(&self, block_id: BlockId);

    fn shard_block_traversed(&self, block_id: &BlockId) -> bool;
    fn commit_shard_block_traversed(&self, block_id: BlockId);

    // drop all shard blocks in the same shard with seqno < block_id
    fn gc_shard_blocks(&self, block_id: &BlockId);
}

pub enum ProviderResult {
    Error(anyhow::Error),
    NotFound, // or should provider backoff until the block is found?
    Found(Block),
}

#[derive(Debug, PartialEq)]
pub enum ProviderStatus {
    Ready,
    NotReady,
}

pub struct StriderBuilder<Provider> {
    subscribers: Vec<Box<dyn OnStriderStep>>,
    persistence_provider: Provider, // or it also should be a vec?
}

impl<Provider> StriderBuilder<Provider>
where
    Provider: PersistenceProvider,
{
    pub fn new(persistence_provider: Provider) -> Self {
        Self {
            subscribers: Vec::new(),
            persistence_provider,
        }
    }

    pub fn add_subscriber(&mut self, subscriber: Box<dyn OnStriderStep>) {
        self.subscribers.push(subscriber);
    }

    // this function gurarantees at least once delivery
    pub async fn start(self, block_provider: impl BlockProvider) {
        loop {
            let master_block = self.fetch_next_master_block(&block_provider).await;
            let master_id = get_block_id(&master_block);
            let shard_hashes = get_shard_hashes(&master_block);
            for hash in shard_hashes {
                if !self.persistence_provider.shard_block_traversed(&hash) {
                    let block = self.fetch_block(&hash, &block_provider).await;
                    let mut subscribers: FuturesUnordered<_> = self
                        .subscribers
                        .iter()
                        .map(|subscriber| subscriber.handle_block(&block))
                        .collect();
                    // wait for all subscribers to finish
                    while subscribers.next().await.is_some() {}
                    self.persistence_provider.commit_shard_block_traversed(hash);
                }
            }
            self.persistence_provider
                .commit_last_traversed_master_block_seqno(master_id);
        }
    }

    async fn fetch_next_master_block(&self, block_provider: &impl BlockProvider) -> Block {
        let last_traversed_master_block = self
            .persistence_provider
            .load_last_traversed_master_block_seqno();
        loop {
            match block_provider
                .get_next_block(&last_traversed_master_block)
                .await
            {
                ProviderResult::Error(e) => {
                    tracing::error!(
                        ?last_traversed_master_block,
                        "error while fetching master block: {:?}",
                        e
                    );
                }
                ProviderResult::NotFound => {
                    tracing::info!(?last_traversed_master_block, "master block not found");
                }
                ProviderResult::Found(block) => break block,
            }
        }
    }

    async fn fetch_block(&self, id: &BlockId, block_provider: &impl BlockProvider) -> Block {
        loop {
            match block_provider.get_block(id).await {
                ProviderResult::Error(e) => {
                    tracing::error!("error while fetching block: {:?}", e);
                }
                ProviderResult::NotFound => {
                    tracing::info!(?id, "no block found");
                }
                ProviderResult::Found(block) => break block,
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
