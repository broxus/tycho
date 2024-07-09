use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use bytes::{BufMut, BytesMut};
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tycho_block_util::archive::Archive;
use tycho_storage::{BlockMetaData, Storage};
use tycho_util::time::now_sec;

use crate::block_strider::provider::{BlockProvider, OptionalBlockStuff, ProofChecker};
use crate::blockchain_rpc::BlockchainRpcClient;

#[derive(Clone)]
#[repr(transparent)]
pub struct ArchiveBlockProvider {
    inner: Arc<Inner>,
}

impl ArchiveBlockProvider {
    pub fn new(client: BlockchainRpcClient, storage: Storage) -> Self {
        let proof_checker = ProofChecker::new(storage);

        Self {
            inner: Arc::new(Inner {
                client,
                proof_checker,
                last_known_archive: ArcSwapOption::empty(),
            }),
        }
    }

    async fn get_next_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let this = self.inner.as_ref();

        let next_block_seqno = block_id.seqno + 1;

        let block_id;
        let archive = loop {
            if let Some(archive) = this.last_known_archive.load_full() {
                if let Some(mc_block_id) = archive.mc_block_ids.get(&next_block_seqno) {
                    block_id = *mc_block_id;
                    break archive;
                }
            }

            // TODO: Impl parallel download
            match self.download_archive(next_block_seqno).await {
                Ok(archive) => this.last_known_archive.store(Some(Arc::new(archive))),
                Err(e) => return Some(Err(e)),
            }
        };

        let (block, proof) = match (
            archive.get_block_by_id(&block_id),
            archive.get_proof_by_id(&block_id),
        ) {
            (Ok(block), Ok(proof)) => (block, proof),
            (Err(e), _) | (_, Err(e)) => return Some(Err(e.into())),
        };

        match this.proof_checker.check_proof(block, proof, true).await {
            Ok(meta) if is_block_recent(&meta) => Some(Ok(block.clone())),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        }
    }

    async fn get_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let this = self.inner.as_ref();

        let Some(archive) = this.last_known_archive.load_full() else {
            return Some(Err(anyhow::anyhow!("no archive available")));
        };

        let (block, proof) = match (
            archive.get_block_by_id(block_id),
            archive.get_proof_by_id(block_id),
        ) {
            (Ok(block), Ok(proof)) => (block, proof),
            (Err(e), _) | (_, Err(e)) => return Some(Err(e.into())),
        };

        if let Err(e) = this.proof_checker.check_proof(block, proof, true).await {
            return Some(Err(e));
        }

        // NOTE: Always return the block by id even if it's not recent
        Some(Ok(block.clone()))
    }

    async fn download_archive(&self, seqno: u32) -> Result<Archive> {
        let client = &self.inner.client;

        loop {
            let mut archive_data = BytesMut::new().writer();
            client.download_archive(seqno, &mut archive_data).await?;
            let archive_data = archive_data.into_inner().freeze();

            match Archive::new(archive_data) {
                Ok(archive) => return Ok(archive),
                Err(e) => {
                    tracing::error!(seqno, "failed to parse downloaded archive: {e}");

                    // TODO: backoff
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }
    }
}

fn is_block_recent(meta: &BlockMetaData) -> bool {
    meta.gen_utime + 600 > now_sec()
}

struct Inner {
    client: BlockchainRpcClient,
    proof_checker: ProofChecker,
    last_known_archive: ArcSwapOption<Archive>,
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
