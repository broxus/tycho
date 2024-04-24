#![allow(clippy::map_err_ignore)]

use std::collections::BTreeMap;

use anyhow::Result;
use everscale_types::cell::Load;
use everscale_types::models::{Block, BlockId, BlockIdShort, BlockProof};
use sha2::Digest;

use tycho_block_util::archive::{ArchiveEntryId, ArchiveReader, WithArchiveData};

pub struct Archive {
    pub blocks: BTreeMap<BlockId, ArchiveDataEntry>,
}

impl Archive {
    pub fn new(data: &[u8]) -> Result<Self> {
        let reader = ArchiveReader::new(data)?;

        let mut res = Archive {
            blocks: Default::default(),
        };

        for entry_data in reader {
            let entry = entry_data?;
            match ArchiveEntryId::from_filename(entry.name)? {
                ArchiveEntryId::Block(id) => {
                    let block = deserialize_block(&id, entry.data)?;
                    res.blocks.entry(id).or_default().block =
                        Some(WithArchiveData::new(block, entry.data.to_vec()));
                }
                ArchiveEntryId::Proof(id) if id.shard.workchain() == -1 => {
                    let proof = deserialize_block_proof(&id, entry.data, false)?;
                    res.blocks.entry(id).or_default().proof =
                        Some(WithArchiveData::new(proof, entry.data.to_vec()));
                }
                ArchiveEntryId::ProofLink(id) if id.shard.workchain() != -1 => {
                    let proof = deserialize_block_proof(&id, entry.data, true)?;
                    res.blocks.entry(id).or_default().proof =
                        Some(WithArchiveData::new(proof, entry.data.to_vec()));
                }
                _ => continue,
            }
        }
        Ok(res)
    }
}

#[derive(Default)]
pub struct ArchiveDataEntry {
    pub block: Option<WithArchiveData<Block>>,
    pub proof: Option<WithArchiveData<BlockProof>>,
}

pub fn deserialize_block(id: &BlockId, data: &[u8]) -> Result<Block, ArchiveDataError> {
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

pub fn deserialize_block_proof(
    block_id: &BlockId,
    data: &[u8],
    is_link: bool,
) -> Result<BlockProof, ArchiveDataError> {
    let root =
        everscale_types::boc::Boc::decode(data).map_err(|_| ArchiveDataError::InvalidBlockProof)?;
    let proof = BlockProof::load_from(&mut root.as_slice()?)
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
