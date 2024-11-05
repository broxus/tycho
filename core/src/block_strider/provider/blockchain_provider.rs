use std::pin::pin;
use std::time::Duration;

use everscale_types::models::*;
use futures_util::future::{self, BoxFuture};
use serde::{Deserialize, Serialize};
use tycho_block_util::archive::WithArchiveData;
use tycho_block_util::block::{BlockIdRelation, BlockProofStuff, BlockStuff};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_storage::Storage;
use tycho_util::serde_helpers;
use tycho_util::sync::rayon_run;

use crate::block_strider::provider::{CheckProof, OptionalBlockStuff, ProofChecker};
use crate::block_strider::BlockProvider;
use crate::blockchain_rpc::{BlockDataFull, BlockchainRpcClient, DataRequirement};
use crate::overlay_client::{Neighbour, PunishReason};

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
        // TODO: Backoff?
        let mut interval = tokio::time::interval(self.config.get_next_block_polling_interval);

        loop {
            tracing::debug!(%prev_block_id, "get_next_block_full requested");
            match self
                .client
                .get_next_block_full(prev_block_id, DataRequirement::Optional)
                .await
            {
                Ok(res) => match res.data {
                    Some(data)
                        if data.block_id.shard != prev_block_id.shard
                            || data.block_id.seqno != prev_block_id.seqno + 1 =>
                    {
                        res.neighbour.punish(PunishReason::Malicious);
                        tracing::warn!("got response for an unknown block id");
                    }
                    Some(data) => {
                        let mc_block_id = data.block_id;
                        let parsed = self
                            .process_received_block(&mc_block_id, data, res.neighbour)
                            .await;
                        if parsed.is_some() {
                            return parsed;
                        }
                    }
                    None => tracing::warn!(?prev_block_id, "block not found"),
                },
                Err(e) => tracing::error!("failed to get block: {e}"),
            }

            interval.tick().await;
        }
    }

    async fn get_block_impl(&self, block_id_relation: &BlockIdRelation) -> OptionalBlockStuff {
        // TODO: Backoff?
        let mut interval = tokio::time::interval(self.config.get_block_polling_interval);

        let BlockIdRelation {
            mc_block_id,
            block_id,
        } = block_id_relation;

        loop {
            tracing::debug!(%block_id, "get_block_full requested");
            match self
                .client
                .get_block_full(block_id, DataRequirement::Expected)
                .await
            {
                Ok(res) => match res.data {
                    Some(data) => {
                        let parsed = self
                            .process_received_block(mc_block_id, data, res.neighbour)
                            .await;
                        if parsed.is_some() {
                            return parsed;
                        }
                    }
                    None => tracing::warn!(%block_id, "block not found"),
                },
                Err(e) => tracing::error!("failed to get block: {e}"),
            }

            interval.tick().await;
        }
    }

    async fn process_received_block(
        &self,
        mc_block_id: &BlockId,
        block_full: BlockDataFull,
        neighbour: Neighbour,
    ) -> OptionalBlockStuff {
        let block_stuff_fut = pin!(rayon_run({
            let block_id = block_full.block_id;
            let block_data = block_full.block_data.clone();
            move || BlockStuff::deserialize_checked(&block_id, &block_data)
        }));

        let other_data_fut = pin!(rayon_run({
            let block_id = block_full.block_id;
            let proof_data = block_full.proof_data.clone();
            let queue_diff_data = block_full.queue_diff_data.clone();
            move || {
                (
                    BlockProofStuff::deserialize(&block_id, &proof_data),
                    QueueDiffStuff::deserialize(&block_id, &queue_diff_data),
                )
            }
        }));

        let (block_stuff, (block_proof, queue_diff)) =
            futures_util::future::join(block_stuff_fut, other_data_fut).await;

        match (block_stuff, block_proof, queue_diff) {
            (Ok(block), Ok(proof), Ok(diff)) => {
                let proof = WithArchiveData::new(proof, block_full.proof_data);
                let diff = WithArchiveData::new(diff, block_full.queue_diff_data);
                if let Err(e) = self
                    .proof_checker
                    .check_proof(CheckProof {
                        mc_block_id,
                        block: &block,
                        proof: &proof,
                        queue_diff: &diff,
                        store_on_success: true,
                    })
                    .await
                {
                    neighbour.punish(PunishReason::Malicious);
                    tracing::error!("got invalid block proof: {e}");
                    return None;
                }

                Some(Ok(block.with_archive_data(block_full.block_data)))
            }
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                neighbour.punish(PunishReason::Malicious);
                tracing::error!("failed to deserialize shard block or block proof: {e}");
                None
            }
        }
    }
}

impl BlockProvider for BlockchainBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type ResetFut<'a> = future::Ready<()>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(self.get_next_block_impl(prev_block_id))
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        Box::pin(self.get_block_impl(block_id_relation))
    }

    fn reset(&self) -> Self::ResetFut<'_> {
        future::ready(())
    }
}
