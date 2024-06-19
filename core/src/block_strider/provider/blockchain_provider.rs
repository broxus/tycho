use std::time::Duration;

use anyhow::Context;
use arc_swap::ArcSwapAny;
use everscale_types::models::*;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tycho_block_util::block::{
    check_with_master_state, check_with_prev_key_block_proof, BlockProofStuff, BlockStuff,
};
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::Storage;
use tycho_util::metrics::HistogramGuard;
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
    cached_zerostate: ArcSwapAny<Option<ShardStateStuff>>,
    cached_prev_key_block_proof: ArcSwapAny<Option<BlockProofStuff>>,
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
            cached_zerostate: Default::default(),
            cached_prev_key_block_proof: Default::default(),
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
        // TODO: Add labels with shard?
        let _histogram = HistogramGuard::begin("tycho_core_check_block_proof_time");

        anyhow::ensure!(
            block.id() == &proof.proof().proof_for,
            "proof_for and block id mismatch: proof_for={}, block_id={}",
            proof.proof().proof_for,
            block.id(),
        );

        let is_masterchain = block.id().is_masterchain();
        anyhow::ensure!(is_masterchain ^ proof.is_link(), "unexpected proof type");

        let (virt_block, virt_block_info) = proof.pre_check_block_proof()?;
        if !is_masterchain {
            return Ok(());
        }

        let handle = {
            let block_handles = self.storage.block_handle_storage();
            block_handles
                .load_key_block_handle(virt_block_info.prev_key_block_seqno)
                .context("failed to load prev key block handle")?
        };

        if handle.id().seqno == 0 {
            let zerostate = 'zerostate: {
                if let Some(zerostate) = self.cached_zerostate.load_full() {
                    break 'zerostate zerostate;
                }

                let shard_states = self.storage.shard_state_storage();
                let zerostate = shard_states
                    .load_state(handle.id())
                    .await
                    .context("failed to load mc zerostate")?;

                self.cached_zerostate.store(Some(zerostate.clone()));

                zerostate
            };

            check_with_master_state(proof, &zerostate, &virt_block, &virt_block_info)
        } else {
            let prev_key_block_proof = 'prev_proof: {
                if let Some(prev_proof) = self.cached_prev_key_block_proof.load_full() {
                    if &prev_proof.as_ref().proof_for == handle.id() {
                        break 'prev_proof prev_proof;
                    }
                }

                let blocks = self.storage.block_storage();
                let prev_key_block_proof = blocks
                    .load_block_proof(&handle, false)
                    .await
                    .context("failed to load prev key block proof")?;

                // NOTE: Assume that there is only one masterchain block using this cache.
                // Otherwise, it will be overwritten every time. Maybe use `rcu`.
                self.cached_prev_key_block_proof
                    .store(Some(prev_key_block_proof.clone()));

                prev_key_block_proof
            };

            check_with_prev_key_block_proof(
                proof,
                &prev_key_block_proof,
                &virt_block,
                &virt_block_info,
            )
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
