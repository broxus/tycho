#![allow(clippy::map_err_ignore)]

use everscale_types::models::{BlockId, BlockIdShort};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tycho_block_util::archive::Archive;
use tycho_block_util::block::{BlockStuff, BlockStuffAug};

use crate::block_strider::provider::{BlockProvider, OptionalBlockStuff};

impl BlockProvider for Archive {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        let id = match self.mc_block_ids.get(&(prev_block_id.seqno + 1)) {
            Some(id) => id,
            None => return Box::pin(futures_util::future::ready(None)),
        };

        self.get_block(id)
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        futures_util::future::ready(self.get_block_by_id(block_id).map(|b| {
            Ok(BlockStuffAug::new(
                BlockStuff::with_block(*block_id, b.clone()),
                everscale_types::boc::BocRepr::encode(b).unwrap(),
            ))
        }))
        .boxed()
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ArchiveDataError {
    #[error("Invalid file hash {0}")]
    InvalidFileHash(BlockIdShort),
    #[error("Invalid root hash")]
    InvalidRootHash,
    #[error("Invalid block data")]
    InvalidBlockData,
    #[error("Invalid block proof")]
    InvalidBlockProof,
    #[error("Proof for another block")]
    ProofForAnotherBlock,
    #[error("Proof for non-masterchain block")]
    ProofForNonMasterchainBlock,
    #[error(transparent)]
    TypeError(#[from] everscale_types::error::Error),
}
