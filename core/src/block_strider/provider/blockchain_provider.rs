use std::time::Duration;

use anyhow::anyhow;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tycho_block_util::archive::WithArchiveData;
use tycho_block_util::block::{
    BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug, ValidatorSubsetInfo,
};
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
            let block_data: Option<
                anyhow::Result<(
                    WithArchiveData<BlockStuff>,
                    WithArchiveData<BlockProofStuff>,
                )>,
            > = match res {
                Ok(res) => match res.data() {
                    BlockFull::Found {
                        block_id,
                        block: block_data,
                        proof: proof_data,
                        is_link,
                    } => match (
                        BlockStuff::deserialize_checked(*block_id, block_data),
                        BlockProofStuff::deserialize(*block_id, proof_data, *is_link),
                    ) {
                        (Ok(block), Ok(proof)) => {
                            let block_data = block_data.clone();
                            let block_proof_data = proof_data.clone();
                            res.accept();
                            Some(Ok((
                                BlockStuffAug::new(block, block_data),
                                BlockProofStuffAug::new(proof, block_proof_data),
                            )))
                        }
                        (Err(e), _) | (_, Err(e)) => {
                            res.reject();
                            tracing::error!(
                                "Failed to deserialize block or block proof for block: {e}"
                            );
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

            if let Some(Ok((block, proof))) = block_data {
                if let Err(e) = self.check_proof(&block.data, &proof.data).await {
                    tracing::error!("Failed to check block proof: {e}");
                    break Some(Err(anyhow!("Invalid block")));
                }

                break Some(Ok(block));
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

            let block: Option<anyhow::Result<WithArchiveData<BlockStuff>>> = match res.data() {
                BlockFull::Found {
                    block_id,
                    block: data,
                    proof: proof_data,
                    is_link,
                } => match (
                    BlockStuff::deserialize_checked(*block_id, data),
                    BlockProofStuff::deserialize(*block_id, proof_data, *is_link),
                ) {
                    (Ok(block), Ok(proof)) => {
                        if let Err(e) = self.check_proof(&block, &proof).await {
                            Some(Err(anyhow!("Invalid block: {e}")))
                        } else {
                            Some(Ok(BlockStuffAug::new(block, data.clone())))
                        }
                    }
                    (Err(e), _) | (_, Err(e)) => {
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

    async fn check_proof(&self, block: &BlockStuff, proof: &BlockProofStuff) -> anyhow::Result<()> {
        if block.id() != &proof.proof().proof_for {
            tracing::error!(
                "Block proof in created not for block {:?} but for block {:?}",
                block.id(),
                &proof.proof().proof_for
            );
            return Err(anyhow!("Request block is invalid"));
        }
        let block_info = match block.load_info() {
            Ok(block_info) => block_info,
            Err(e) => {
                tracing::error!("Failed to load block {} info: {e}", block.id());
                return Err(anyhow!("Request block is invalid"));
            }
        };

        let previous_key_block = self
            .storage
            .block_handle_storage()
            .load_key_block_handle(block_info.prev_key_block_seqno);

        if let Some(key_block_handle) = previous_key_block {
            let shard_state_stuff = match self
                .storage
                .shard_state_storage()
                .load_state(key_block_handle.id())
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
