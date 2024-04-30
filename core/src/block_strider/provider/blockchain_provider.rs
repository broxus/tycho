use std::time::Duration;

use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tycho_block_util::block::{BlockStuff, BlockStuffAug};
use tycho_storage::Storage;

use crate::block_strider::provider::OptionalBlockStuff;
use crate::block_strider::BlockProvider;
use crate::blockchain_rpc::BlockchainRpcClient;
use crate::proto::blockchain::BlockFull;

// TODO: Use backoff instead of simple polling.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct BlockchainBlockProviderConfig {
    /// Polling interval for `get_next_block` method.
    ///
    /// Default: 1 second.
    pub get_next_block_polling_interval: Duration,

    /// Polling interval for `get_block` method.
    ///
    /// Default: 1 second.
    pub get_block_polling_interval: Duration,
}

impl Default for BlockchainBlockProviderConfig {
    fn default() -> Self {
        Self {
            get_next_block_polling_interval: Duration::from_secs(1),
            get_block_polling_interval: Duration::from_secs(1),
        }
    }
}

pub struct BlockchainBlockProvider {
    client: BlockchainRpcClient,
    storage: Storage,
    config: BlockchainBlockProviderConfig,
}

impl BlockchainBlockProvider {
    pub fn new(
        client: BlockchainRpcClient,
        storage: Storage,
        config: BlockchainBlockProviderConfig,
    ) -> Self {
        Self {
            client,
            storage,
            config,
        }
    }

    // TODO: Validate block with proof.
    async fn get_next_block_impl(&self, prev_block_id: &BlockId) -> OptionalBlockStuff {
        let mut interval = tokio::time::interval(self.config.get_next_block_polling_interval);
        loop {
            let res = self.client.get_next_block_full(prev_block_id).await;
            let block = match res {
                Ok(res) => match res.data() {
                    BlockFull::Found {
                        block_id,
                        block: block_data,
                        ..
                    } => match BlockStuff::deserialize_checked(*block_id, block_data) {
                        Ok(block) => {
                            let block_data = block_data.clone();
                            res.accept();
                            Some(Ok(BlockStuffAug::new(block, block_data)))
                        }
                        Err(e) => {
                            res.reject();
                            tracing::error!("failed to deserialize block: {e}");
                            None
                        }
                    },
                    BlockFull::Empty => None,
                },
                Err(e) => {
                    tracing::error!("failed to get next block: {e}");
                    None
                }
            };

            // TODO: Use storage to get the previous key block
            _ = &self.storage;

            if block.is_some() {
                break block;
            }

            interval.tick().await;
        }
    }

    async fn get_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let mut interval = tokio::time::interval(self.config.get_block_polling_interval);
        loop {
            let res = match self.client.get_block_full(block_id).await {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!("failed to get block: {:?}", e);
                    interval.tick().await;
                    continue;
                }
            };

            // TODO: refactor

            let block = match res.data() {
                BlockFull::Found {
                    block_id,
                    block: data,
                    ..
                } => match BlockStuff::deserialize_checked(*block_id, data) {
                    Ok(block) => Some(Ok(BlockStuffAug::new(block, data.clone()))),
                    Err(e) => {
                        res.accept();
                        tracing::error!("failed to deserialize block: {:?}", e);
                        interval.tick().await;
                        continue;
                    }
                },
                BlockFull::Empty => {
                    interval.tick().await;
                    continue;
                }
            };

            break block;
        }
    }
}

impl BlockProvider for BlockchainBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(self.get_next_block_impl(prev_block_id))
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(self.get_block_impl(block_id))
    }
}
