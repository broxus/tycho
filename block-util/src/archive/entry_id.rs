use std::borrow::Borrow;
use std::hash::Hash;
use std::str::FromStr;

use anyhow::Result;
use everscale_types::models::*;
use everscale_types::prelude::HashBytes;
use smallvec::SmallVec;

/// Package entry id.
#[derive(Debug, Hash, Eq, PartialEq)]
pub enum ArchiveEntryId<I = BlockId> {
    /// Block data entry.
    Block(I),
    /// Block proof entry.
    Proof(I),
    /// Block proof link entry.
    ProofLink(I),
}

impl ArchiveEntryId<BlockId> {
    /// Parses package entry id from string.
    pub fn from_filename(filename: &str) -> Result<Self> {
        let block_id_pos = match filename.find('(') {
            Some(pos) => pos,
            None => return Err(ParseArchiveEntryIdError::InvalidFileName.into()),
        };

        let (prefix, block_id) = filename.split_at(block_id_pos);

        Ok(match prefix {
            ENTRY_BLOCK => Self::Block(parse_block_id(block_id)?),
            ENTRY_PROOF => Self::Proof(parse_block_id(block_id)?),
            ENTRY_PROOF_LINK => Self::ProofLink(parse_block_id(block_id)?),
            _ => return Err(ParseArchiveEntryIdError::InvalidFileName.into()),
        })
    }
}

impl<T> ArchiveEntryId<T> {
    pub fn extract_kind(data: &[u8]) -> Option<ArchiveEntryIdKind> {
        if data.len() < SERIALIZED_LEN {
            return None;
        }

        Some(match data[SERIALIZED_LEN - 1] {
            0 => ArchiveEntryIdKind::Block,
            1 => ArchiveEntryIdKind::Proof,
            2 => ArchiveEntryIdKind::ProofLink,
            _ => return None,
        })
    }

    pub fn kind(&self) -> ArchiveEntryIdKind {
        match self {
            Self::Block(_) => ArchiveEntryIdKind::Block,
            Self::Proof(_) => ArchiveEntryIdKind::Proof,
            Self::ProofLink(_) => ArchiveEntryIdKind::ProofLink,
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
#[repr(u8)]
pub enum ArchiveEntryIdKind {
    Block = 0,
    Proof = 1,
    ProofLink = 2,
}

impl<I> ArchiveEntryId<I>
where
    I: Borrow<BlockId>,
{
    /// Returns package entry prefix.
    fn filename_prefix(&self) -> &'static str {
        match self {
            Self::Block(_) => ENTRY_BLOCK,
            Self::Proof(_) => ENTRY_PROOF,
            Self::ProofLink(_) => ENTRY_PROOF_LINK,
        }
    }

    /// Constructs on-stack buffer with the serialized object
    pub fn to_vec(&self) -> SmallVec<[u8; SERIALIZED_LEN]> {
        let mut result = SmallVec::with_capacity(SERIALIZED_LEN);
        let (block_id, ty) = match self {
            Self::Block(id) => (id, ArchiveEntryIdKind::Block as u8),
            Self::Proof(id) => (id, ArchiveEntryIdKind::Proof as u8),
            Self::ProofLink(id) => (id, ArchiveEntryIdKind::ProofLink as u8),
        };
        let block_id = block_id.borrow();

        result.extend_from_slice(&block_id.shard.workchain().to_be_bytes());
        result.extend_from_slice(&block_id.shard.prefix().to_be_bytes());
        result.extend_from_slice(&block_id.seqno.to_be_bytes());
        result.extend_from_slice(block_id.root_hash.as_slice());
        result.push(ty);

        result
    }
}

const SERIALIZED_LEN: usize = 4 + 8 + 4 + 32 + 1;

pub trait GetFileName {
    fn filename(&self) -> String;
}

impl GetFileName for BlockId {
    fn filename(&self) -> String {
        format!(
            "({},{:016x},{}):{}:{}",
            self.shard.workchain(),
            self.shard.prefix(),
            self.seqno,
            hex::encode_upper(self.root_hash.as_slice()),
            hex::encode_upper(self.file_hash.as_slice())
        )
    }
}

impl<I> GetFileName for ArchiveEntryId<I>
where
    I: Borrow<BlockId> + Hash,
{
    fn filename(&self) -> String {
        match self {
            Self::Block(block_id) | Self::Proof(block_id) | Self::ProofLink(block_id) => {
                format!("{}{}", self.filename_prefix(), block_id.borrow().filename())
            }
        }
    }
}

fn parse_block_id(filename: &str) -> Result<BlockId> {
    let mut parts = filename.split(':');

    let shard_id = match parts.next() {
        Some(part) => part,
        None => return Err(ParseArchiveEntryIdError::ShardIdNotFound.into()),
    };

    let mut shard_id_parts = shard_id.split(',');
    let workchain_id = match shard_id_parts
        .next()
        .and_then(|part| part.strip_prefix('('))
    {
        Some(part) => i32::from_str(part)?,
        None => return Err(ParseArchiveEntryIdError::WorkchainIdNotFound.into()),
    };

    let shard_prefix_tagged = match shard_id_parts.next() {
        Some(part) => u64::from_str_radix(part, 16)?,
        None => return Err(ParseArchiveEntryIdError::ShardPrefixNotFound.into()),
    };

    let shard = ShardIdent::new(workchain_id, shard_prefix_tagged)
        .ok_or(ParseArchiveEntryIdError::InvalidShardIdent)?;

    let seqno = match shard_id_parts
        .next()
        .and_then(|part| part.strip_suffix(')'))
    {
        Some(part) => u32::from_str(part)?,
        None => return Err(ParseArchiveEntryIdError::SeqnoNotFound.into()),
    };

    let root_hash = match parts.next() {
        Some(part) => hex::decode(part)?
            .try_into()
            .map(HashBytes)
            .map_err(|_e| ParseArchiveEntryIdError::InvalidHash)?,
        None => return Err(ParseArchiveEntryIdError::RootHashNotFound.into()),
    };

    let file_hash = match parts.next() {
        Some(part) => hex::decode(part)?
            .try_into()
            .map(HashBytes)
            .map_err(|_e| ParseArchiveEntryIdError::InvalidHash)?,
        None => return Err(ParseArchiveEntryIdError::FileHashNotFound.into()),
    };

    Ok(BlockId {
        shard,
        seqno,
        root_hash,
        file_hash,
    })
}

const ENTRY_BLOCK: &str = "block_";
const ENTRY_PROOF: &str = "proof_";
const ENTRY_PROOF_LINK: &str = "prooflink_";

#[derive(thiserror::Error, Debug)]
enum ParseArchiveEntryIdError {
    #[error("invalid filename")]
    InvalidFileName,
    #[error("shard id not found")]
    ShardIdNotFound,
    #[error("workchain id not found")]
    WorkchainIdNotFound,
    #[error("shard prefix not found")]
    ShardPrefixNotFound,
    #[error("invalid shard id")]
    InvalidShardIdent,
    #[error("seqno not found")]
    SeqnoNotFound,
    #[error("root hash not found")]
    RootHashNotFound,
    #[error("file hash not found")]
    FileHashNotFound,
    #[error("invalid hash")]
    InvalidHash,
}

#[cfg(test)]
mod tests {
    use rand::random;

    use super::*;

    #[test]
    fn test_store_load() {
        fn check_package_id(package_id: ArchiveEntryId<BlockId>) {
            assert_eq!(
                ArchiveEntryId::from_filename(&package_id.filename()).unwrap(),
                package_id
            );
        }

        let block_id = BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: random(),
            root_hash: HashBytes(random()),
            file_hash: HashBytes(random()),
        };

        check_package_id(ArchiveEntryId::Block(block_id.clone()));
        check_package_id(ArchiveEntryId::Proof(block_id.clone()));
        check_package_id(ArchiveEntryId::ProofLink(block_id));
    }
}
