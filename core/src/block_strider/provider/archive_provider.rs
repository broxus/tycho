#![allow(clippy::map_err_ignore)]

use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tycho_block_util::archive::{Archive, ArchiveWritersPool};
use tycho_block_util::block::BlockStuffAug;
use tycho_util::time::now_sec;

use crate::block_strider::provider::{BlockProvider, OptionalBlockStuff};
use crate::blockchain_rpc::BlockchainRpcClient;

pub struct ArchiveBlockProvider {
    client: BlockchainRpcClient,
    archive: ArcSwapOption<Archive>,
    writers_pool: ArchiveWritersPool,
}

impl ArchiveBlockProvider {
    pub fn new(client: BlockchainRpcClient, archive_path: &Path) -> Self {
        // TODO: add to config
        let save_to_disk_threshold = 1024 * 1024 * 1024;

        Self {
            client,
            archive: Default::default(),
            writers_pool: ArchiveWritersPool::new(archive_path, save_to_disk_threshold),
        }
    }

    async fn get_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let block = match self.archive.load_full() {
            Some(archive) => archive.get_block_by_id(block_id),
            None => None,
        };

        let block = match block {
            Some(block) => Ok(block),
            None => match self.download_archive(block_id.seqno).await {
                Ok(archive) => {
                    let block = archive
                        .get_block_by_id(block_id)
                        .ok_or(ArchiveProviderError::BLockNotFound(block_id.seqno).into());

                    if block.is_ok() {
                        self.archive.store(Some(Arc::new(archive)));
                    }

                    block
                }
                Err(e) => return Some(Err(e)),
            },
        };

        match Self::is_sync(&block) {
            Ok(true) => None,
            Ok(false) => Some(block),
            Err(e) => Some(Err(e)),
        }
    }

    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let block = match self.archive.load_full() {
            Some(archive) => archive.get_next_block(block_id).await,
            None => None,
        };

        let next_block_seqno = block_id.seqno + 1;
        let next_block = match block {
            Some(Ok(block)) => Ok(block),
            Some(Err(e)) => return Some(Err(e)),
            None => match self.download_archive(next_block_seqno).await {
                Ok(archive) => {
                    let block = archive
                        .get_block_by_seqno(next_block_seqno)
                        .ok_or(ArchiveProviderError::BLockNotFound(next_block_seqno).into());

                    if block.is_ok() {
                        self.archive.store(Some(Arc::new(archive)));
                    }

                    block
                }
                Err(e) => return Some(Err(e)),
            },
        };

        match Self::is_sync(&next_block) {
            Ok(true) => None,
            Ok(false) => Some(next_block),
            Err(e) => Some(Err(e)),
        }
    }

    fn is_sync(block: &anyhow::Result<BlockStuffAug>) -> anyhow::Result<bool> {
        if let Ok(block) = block {
            if block.data.load_info()?.gen_utime + 300 > now_sec() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn download_archive(&self, seqno: u32) -> anyhow::Result<Archive> {
        // Download archive
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

impl BlockProvider for Archive {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        let id = match self.block_ids.get(&(prev_block_id.seqno + 1)) {
            Some(id) => id,
            None => return Box::pin(futures_util::future::ready(None)),
        };

        self.get_block(id)
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        futures_util::future::ready(self.get_block_by_id(block_id).map(Ok)).boxed()
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ArchiveProviderError {
    #[error("BLock not found in archive {0}")]
    BLockNotFound(u32),
}
