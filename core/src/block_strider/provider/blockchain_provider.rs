use std::time::Duration;

use everscale_types::models::*;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tycho_block_util::block::{BlockProofStuff, BlockStuff};
use tycho_storage::Storage;
use tycho_util::serde_helpers;

use crate::block_strider::provider::{OptionalBlockStuff, ProofChecker};
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
    #[serde(with = "serde_helpers::humantime")]
    pub get_next_block_polling_interval: Duration,

    /// Polling interval for `get_block` method.
    ///
    /// Default: 1 second.
    #[serde(with = "serde_helpers::humantime")]
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
    config: BlockchainBlockProviderConfig,
    proof_checker: ProofChecker,
}

impl BlockchainBlockProvider {
    pub fn new(
        client: BlockchainRpcClient,
        storage: Storage,
        config: BlockchainBlockProviderConfig,
    ) -> Self {
        let proof_checker = ProofChecker::new(storage);

        Self {
            client,
            config,
            proof_checker,
        }
    }

    async fn get_next_block_impl(&self, prev_block_id: &BlockId) -> OptionalBlockStuff {
        let mut interval = tokio::time::interval(self.config.get_next_block_polling_interval);

        loop {
            let res = self.client.get_next_block_full(prev_block_id).await;
            'res: {
                let (handle, data) = match res {
                    Ok(res) => res.split(),
                    Err(e) => {
                        tracing::error!("failed to get next block: {e}");
                        break 'res;
                    }
                };

                let BlockFull::Found {
                    block_id,
                    block: block_data,
                    proof: proof_data,
                    is_link,
                } = data
                else {
                    handle.accept();
                    break 'res;
                };

                match (
                    BlockStuff::deserialize_checked(&block_id, &block_data),
                    BlockProofStuff::deserialize(&block_id, &proof_data, is_link),
                ) {
                    (Ok(block), Ok(proof)) => {
                        if let (Err(e), _) | (_, Err(e)) = (
                            self.proof_checker.check_proof(&block, &proof).await,
                            self.proof_checker
                                .store_block_proof(&block, proof, proof_data.into())
                                .await,
                        ) {
                            handle.reject();
                            tracing::error!("got invalid mc block proof: {e}");
                            break 'res;
                        }

                        handle.accept();
                        return Some(Ok(block.with_archive_data(block_data)));
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        handle.reject();
                        tracing::error!("failed to deserialize mc block or block proof: {e}");
                    }
                }
            }

            interval.tick().await;
        }
    }

    async fn get_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let mut interval = tokio::time::interval(self.config.get_block_polling_interval);
        loop {
            // TODO: refactor

            let res = self.client.get_block_full(block_id).await;
            'res: {
                let (handle, data) = match res {
                    Ok(res) => res.split(),
                    Err(e) => {
                        tracing::error!("failed to get block: {e}");
                        break 'res;
                    }
                };

                let BlockFull::Found {
                    block_id,
                    block: block_data,
                    proof: proof_data,
                    is_link,
                } = data
                else {
                    handle.accept();
                    break 'res;
                };

                match (
                    BlockStuff::deserialize_checked(&block_id, &block_data),
                    BlockProofStuff::deserialize(&block_id, &proof_data, is_link),
                ) {
                    (Ok(block), Ok(proof)) => {
                        if let (Err(e), _) | (_, Err(e)) = (
                            self.proof_checker.check_proof(&block, &proof).await,
                            self.proof_checker
                                .store_block_proof(&block, proof, proof_data.into())
                                .await,
                        ) {
                            handle.reject();
                            tracing::error!("got invalid shard block proof: {e}");
                            break 'res;
                        }

                        handle.accept();
                        return Some(Ok(block.with_archive_data(block_data)));
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        handle.reject();
                        tracing::error!("failed to deserialize shard block or block proof: {e}");
                    }
                }
            }

            interval.tick().await;
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
