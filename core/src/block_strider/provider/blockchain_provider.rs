use std::time::Duration;

use anyhow::anyhow;
use everscale_types::models::{BlockId, PrevBlockRef};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tycho_block_util::block::{BlockProofStuff, BlockStuff};
use tycho_storage::Storage;
use tycho_util::serde_helpers;

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
                        if let Err(e) = self.check_proof(&block, &proof).await {
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
                        if let Err(e) = self.check_proof(&block, &proof).await {
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

    async fn check_proof(&self, block: &BlockStuff, proof: &BlockProofStuff) -> anyhow::Result<()> {
        let started_at = Instant::now();
        anyhow::ensure!(
            block.id() == &proof.proof().proof_for,
            "proof_for and block id mismatch: proof_for={}, block_id={}",
            proof.proof().proof_for,
            block.id(),
        );

        if !block.id().is_masterchain() {
            proof.pre_check_block_proof()?;
            metrics::histogram!("tycho_core_check_shard_block_proof_time")
                .record(started_at.elapsed());
            return Ok(());
        }

        let block_info = match block.load_info() {
            Ok(block_info) => block_info,
            Err(e) => anyhow::bail!("failed to parse block info: {e}"),
        };

        let previous_block = block_info.load_prev_ref()?;
        let prev_block_seqno = match previous_block {
            PrevBlockRef::Single(block_ref) => block_ref.seqno,
            _ => {
                anyhow::bail!(
                    "Master block can't have more than 1 prev ref. Block: {:?}",
                    block.id()
                );
            }
        };

        let previous_master_block = self
            .storage
            .block_handle_storage()
            .load_key_block_handle(prev_block_seqno);

        if let Some(previous_master_block_handle) = previous_master_block {
            let shard_state_stuff = match self
                .storage
                .shard_state_storage()
                .load_state(previous_master_block_handle.id())
                .await
            {
                Ok(shard_state_stuff) => shard_state_stuff,
                Err(e) => {
                    tracing::error!(
                        "Failed to load shard state for block {} info: {e}",
                        block.id()
                    );
                    return Err(anyhow!("Request block is invalid"));
                }
            };

            if let Err(e) = proof.check_with_master_state(&shard_state_stuff) {
                tracing::error!(
                    "Failed to check proof for block {} with master state: {e}",
                    block.id()
                );
                return Err(anyhow!("Request block is invalid"));
            }
        }

        metrics::histogram!("tycho_core_check_master_block_proof_time")
            .record(started_at.elapsed());

        Ok(())
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
