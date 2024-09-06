use std::hash::Hash;

use anyhow::Context;
use everscale_types::cell::HashBytes;
use everscale_types::models::*;
use tycho_block_util::archive::ArchiveEntryType;

/// Package entry id.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct PackageEntryKey {
    pub short_block_id: EntryBlockId,
    pub ty: ArchiveEntryType,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub struct EntryBlockId {
    pub shard: ShardIdent,
    pub root_hash: HashBytes,
    pub seqno: u32,
}

impl EntryBlockId {
    pub fn as_block_id(&self, file_hash: HashBytes) -> BlockId {
        BlockId {
            shard: self.shard,
            root_hash: self.root_hash,
            file_hash,
            seqno: self.seqno,
        }
    }
}

impl From<&BlockId> for EntryBlockId {
    fn from(block_id: &BlockId) -> Self {
        Self {
            shard: block_id.shard,
            root_hash: block_id.root_hash,
            seqno: block_id.seqno,
        }
    }
}

impl From<BlockId> for EntryBlockId {
    fn from(block_id: BlockId) -> Self {
        Self {
            shard: block_id.shard,
            root_hash: block_id.root_hash,
            seqno: block_id.seqno,
        }
    }
}

impl PackageEntryKey {
    pub const KEY_LEN: usize = 49;

    pub fn block(block_id: &BlockId) -> Self {
        Self {
            short_block_id: EntryBlockId::from(block_id),
            ty: ArchiveEntryType::Block,
        }
    }

    pub fn proof(block_id: &BlockId) -> Self {
        Self {
            short_block_id: EntryBlockId::from(block_id),
            ty: ArchiveEntryType::Proof,
        }
    }

    pub fn queue_diff(block_id: &BlockId) -> Self {
        Self {
            short_block_id: EntryBlockId::from(block_id),
            ty: ArchiveEntryType::QueueDiff,
        }
    }

    pub fn as_bytes(&self) -> [u8; PackageEntryKey::KEY_LEN] {
        let mut result = [0; Self::KEY_LEN];
        result[..4].copy_from_slice(&self.short_block_id.shard.workchain().to_be_bytes());
        result[4..12].copy_from_slice(&self.short_block_id.shard.prefix().to_be_bytes());
        result[12..16].copy_from_slice(&self.short_block_id.seqno.to_be_bytes());
        result[16..48].copy_from_slice(self.short_block_id.root_hash.as_slice());
        result[48] = self.ty as u8;
        result
    }

    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() == Self::KEY_LEN,
            "Invalid key length. Expected {} bytes, got {}",
            Self::KEY_LEN,
            bytes.len()
        );
        let workchain = i32::from_be_bytes(bytes[..4].try_into()?);
        let prefix = u64::from_be_bytes(bytes[4..12].try_into()?);
        let seqno = u32::from_be_bytes(bytes[12..16].try_into()?);

        let root_hash = HashBytes::from_slice(&bytes[16..48]);
        let block_id = EntryBlockId {
            shard: ShardIdent::new(workchain, prefix).context("Invalid shard ident")?,
            seqno,
            root_hash,
        };
        let ty = ArchiveEntryType::from_byte(bytes[48]).context("Invalid entry type")?;

        Ok(Self {
            short_block_id: block_id,
            ty,
        })
    }
}
