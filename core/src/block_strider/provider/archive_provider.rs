#![allow(clippy::map_err_ignore)]

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tycho_block_util::archive::{Archive, ArchiveWritersPool};
use tycho_block_util::block::{BlockStuff, BlockStuffAug};
use tycho_storage::Storage;
use tycho_util::time::now_sec;

use crate::block_strider::provider::{BlockProvider, OptionalBlockStuff, ProofChecker};
use crate::blockchain_rpc::BlockchainRpcClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ArchiveBlockProviderConfig {
    /// Default: 1073741824 (1 GB)
    pub save_to_disk_threshold: usize,
}

impl Default for ArchiveBlockProviderConfig {
    fn default() -> Self {
        Self {
            save_to_disk_threshold: 1024 * 1024 * 1024,
        }
    }
}

pub struct ArchiveBlockProvider {
    client: BlockchainRpcClient,
    writers_pool: ArchiveWritersPool,
    proof_checker: ProofChecker,
    last_known_archive: ArcSwapOption<Archive>,
}

impl ArchiveBlockProvider {
    pub fn new(
        client: BlockchainRpcClient,
        storage: Storage,
        config: ArchiveBlockProviderConfig,
    ) -> Self {
        let writers_pool =
            ArchiveWritersPool::new(storage.root().path(), config.save_to_disk_threshold);
        let proof_checker = ProofChecker::new(storage);

        Self {
            client,
            writers_pool,
            proof_checker,
            last_known_archive: Default::default(),
        }
    }

    async fn get_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let res = self.last_known_archive.load_full().map(|archive| {
            (
                archive.get_block_by_id(block_id),
                archive.get_proof_by_id(block_id),
            )
        });

        let (block, proof) = match res {
            Some((Ok(block), Ok(proof))) => (block, proof),
            _ => match self.download_archive(block_id.seqno).await {
                Ok(archive) => {
                    let block = archive.get_block_by_id(block_id);
                    let proof = archive.get_proof_by_id(block_id);

                    match (block, proof) {
                        (Ok(block), Ok(proof)) => {
                            self.last_known_archive.store(Some(Arc::new(archive)));
                            (block, proof)
                        }
                        _ => return Some(Err(ArchiveProviderError::InvalidArchive.into())),
                    }
                }
                Err(e) => return Some(Err(e)),
            },
        };

        if let Err(e) = self.proof_checker.check_proof(&block, &proof).await {
            return Some(Err(e));
        }

        match Self::is_sync(&block) {
            Ok(true) => return None,
            Ok(false) => { /* do nothing */ }
            Err(e) => return Some(Err(e)),
        }

        Some(Self::construct_block(block))
    }

    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let next_block_seqno = block_id.seqno + 1;

        let res = self.last_known_archive.load_full().map(|archive| {
            (
                archive.get_block_by_seqno(next_block_seqno),
                archive.get_proof_by_seqno(next_block_seqno),
            )
        });

        let (block, proof) = match res {
            Some((Ok(block), Ok(proof))) => (block, proof),
            _ => match self.download_archive(next_block_seqno).await {
                Ok(archive) => {
                    let block = archive.get_block_by_seqno(next_block_seqno);
                    let proof = archive.get_proof_by_seqno(next_block_seqno);

                    match (block, proof) {
                        (Ok(block), Ok(proof)) => {
                            self.last_known_archive.store(Some(Arc::new(archive)));
                            (block, proof)
                        }
                        (Err(e), _) | (_, Err(e)) => return Some(Err(e)),
                    }
                }
                Err(e) => return Some(Err(e)),
            },
        };

        if let Err(e) = self.proof_checker.check_proof(&block, &proof).await {
            return Some(Err(e));
        }

        match Self::is_sync(&block) {
            Ok(true) => return None,
            Ok(false) => { /* do nothing */ }
            Err(e) => return Some(Err(e)),
        }

        Some(Self::construct_block(block))
    }

    async fn download_archive(&self, seqno: u32) -> anyhow::Result<Archive> {
        let writers_pool = self.writers_pool.clone();
        let blockchain_rpc_client = self.client.clone();

        loop {
            let mut writer = writers_pool.acquire();

            blockchain_rpc_client
                .download_archive(seqno, &mut writer)
                .await?;

            let archive = match writer.parse_archive() {
                Ok(archive) => archive,
                Err(e) => {
                    tracing::error!(seqno, "failed to parse downloaded archive: {e}");
                    continue;
                }
            };

            return Ok(archive);
        }
    }

    fn is_sync(block: &BlockStuff) -> anyhow::Result<bool> {
        Ok(block.load_info()?.gen_utime + 600 > now_sec())
    }

    fn construct_block(block: BlockStuff) -> anyhow::Result<BlockStuffAug> {
        match everscale_types::boc::BocRepr::encode(block.block().clone()) {
            Ok(archive_data) => Ok(BlockStuffAug::new(
                BlockStuff::with_block(*block.id(), block.into_block()),
                archive_data,
            )),
            Err(e) => Err(e.into()),
        }
    }
}

impl BlockProvider for ArchiveBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(self.get_next_block_impl(prev_block_id))
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(self.get_block_impl(block_id))
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ArchiveProviderError {
    #[error("Invalid archive")]
    InvalidArchive,
}
