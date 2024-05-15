#![allow(clippy::map_err_ignore)]

use std::collections::BTreeMap;

use anyhow::Result;
use everscale_types::cell::Load;
use everscale_types::models::{Block, BlockId, BlockIdShort, BlockProof};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use sha2::Digest;
use tycho_block_util::archive::{ArchiveEntryId, ArchiveReader};
use tycho_block_util::block::{BlockStuff, BlockStuffAug};

use crate::block_strider::provider::{BlockProvider, OptionalBlockStuff};

pub struct ArchiveBlockProvider {
    pub mc_block_ids: BTreeMap<u32, BlockId>,
    pub blocks: BTreeMap<BlockId, ArchiveDataEntry>,
}

impl ArchiveBlockProvider {
    pub fn new(data: &[u8]) -> Result<Self> {
        let reader = ArchiveReader::new(data)?;

        let mut res = ArchiveBlockProvider {
            mc_block_ids: Default::default(),
            blocks: Default::default(),
        };

        for data in reader {
            let entry = data?;
            match ArchiveEntryId::from_filename(entry.name)? {
                ArchiveEntryId::Block(id) => {
                    let block = deserialize_block(&id, entry.data)?;

                    res.blocks.entry(id).or_default().block = Some(block);
                    if id.shard.workchain() == -1 {
                        // todo: add is_masterchain() method
                        res.mc_block_ids.insert(id.seqno, id);
                    }
                }
                ArchiveEntryId::Proof(id) if id.shard.workchain() == -1 => {
                    let proof = deserialize_block_proof(&id, entry.data, false)?;

                    res.blocks.entry(id).or_default().proof = Some(proof);
                    res.mc_block_ids.insert(id.seqno, id);
                }
                ArchiveEntryId::ProofLink(id) if id.shard.workchain() != -1 => {
                    let proof = deserialize_block_proof(&id, entry.data, true)?;

                    res.blocks.entry(id).or_default().proof = Some(proof);
                }
                _ => continue,
            }
        }
        Ok(res)
    }

    pub fn lowest_mc_id(&self) -> Option<&BlockId> {
        self.mc_block_ids.values().next()
    }

    pub fn highest_mc_id(&self) -> Option<&BlockId> {
        self.mc_block_ids.values().next_back()
    }

    pub fn get_block_by_id(&self, id: &BlockId) -> Option<Block> {
        self.blocks
            .get(id)
            .map(|entry| entry.block.as_ref().unwrap())
            .cloned()
    }
}

impl BlockProvider for ArchiveBlockProvider {
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

#[derive(Default)]
pub struct ArchiveDataEntry {
    pub block: Option<Block>,
    pub proof: Option<BlockProof>,
}

pub(crate) fn deserialize_block(id: &BlockId, data: &[u8]) -> Result<Block, ArchiveDataError> {
    let file_hash = sha2::Sha256::digest(data);
    if id.file_hash.as_slice() != file_hash.as_slice() {
        Err(ArchiveDataError::InvalidFileHash(id.as_short_id()))
    } else {
        let root = everscale_types::boc::Boc::decode(data)
            .map_err(|_| ArchiveDataError::InvalidBlockData)?;
        if &id.root_hash != root.repr_hash() {
            return Err(ArchiveDataError::InvalidRootHash);
        }

        Block::load_from(&mut root.as_slice()?).map_err(|_| ArchiveDataError::InvalidBlockData)
    }
}

pub(crate) fn deserialize_block_proof(
    block_id: &BlockId,
    data: &[u8],
    is_link: bool,
) -> Result<BlockProof, ArchiveDataError> {
    let root =
        everscale_types::boc::Boc::decode(data).map_err(|_| ArchiveDataError::InvalidBlockProof)?;
    let proof = everscale_types::models::BlockProof::load_from(&mut root.as_slice()?)
        .map_err(|_| ArchiveDataError::InvalidBlockProof)?;

    if &proof.proof_for != block_id {
        return Err(ArchiveDataError::ProofForAnotherBlock);
    }

    if !block_id.shard.workchain() == -1 && !is_link {
        Err(ArchiveDataError::ProofForNonMasterchainBlock)
    } else {
        Ok(proof)
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
