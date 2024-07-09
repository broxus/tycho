use std::collections::BTreeMap;

use anyhow::Result;
use bytes::Bytes;
use everscale_types::models::BlockId;
use tycho_util::FastHashMap;

pub use self::entry_id::{ArchiveEntryId, ArchiveEntryIdKind, GetFileName};
pub use self::reader::{ArchiveEntry, ArchiveReader, ArchiveReaderError, ArchiveVerifier};
use crate::block::{BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug};

mod entry_id;
mod reader;

pub const ARCHIVE_PREFIX: [u8; 4] = u32::to_le_bytes(0xae8fdd01);
pub const ARCHIVE_ENTRY_PREFIX: [u8; 2] = u16::to_le_bytes(0x1e8b);
pub const ARCHIVE_ENTRY_HEADER_LEN: usize = ARCHIVE_ENTRY_PREFIX.len() + 2 + 4; // magic + filename len + data len

pub struct Archive {
    pub mc_block_ids: BTreeMap<u32, BlockId>,
    pub blocks: FastHashMap<BlockId, ArchiveDataEntry>,
}

impl Archive {
    pub fn new<T>(data: T) -> Result<Self>
    where
        Bytes: From<T>,
    {
        let data = Bytes::from(data);
        let reader = ArchiveReader::new(&data)?;

        let mut res = Archive {
            mc_block_ids: Default::default(),
            blocks: Default::default(),
        };

        for entry_data in reader {
            let entry = entry_data?;
            match ArchiveEntryId::from_filename(entry.name)? {
                ArchiveEntryId::Block(id) => {
                    let block = BlockStuff::deserialize_checked(&id, entry.data)?;

                    if id.shard.is_masterchain() {
                        res.mc_block_ids.insert(id.seqno, id);
                    }

                    let parsed = res.blocks.entry(id).or_default();
                    anyhow::ensure!(parsed.block.is_none(), "duplicate block data for: {id}");
                    parsed.block = Some(WithArchiveData::new::<Bytes>(
                        block,
                        data.slice_ref(entry.data),
                    ));
                }
                ArchiveEntryId::Proof(id) => {
                    let proof = BlockProofStuff::deserialize(&id, entry.data, false)?;

                    res.mc_block_ids.insert(id.seqno, id);

                    let parsed = res.blocks.entry(id).or_default();
                    anyhow::ensure!(parsed.proof.is_none(), "duplicate block proof for: {id}");
                    parsed.proof = Some(WithArchiveData::new::<Bytes>(
                        proof,
                        data.slice_ref(entry.data),
                    ));
                }
                ArchiveEntryId::ProofLink(id) => {
                    let proof = BlockProofStuff::deserialize(&id, entry.data, true)?;

                    let parsed = res.blocks.entry(id).or_default();
                    anyhow::ensure!(parsed.proof.is_none(), "duplicate block proof for: {id}");
                    parsed.proof = Some(WithArchiveData::new::<Bytes>(
                        proof,
                        data.slice_ref(entry.data),
                    ));
                }
            }
        }

        Ok(res)
    }

    pub fn get_block_by_id(&self, id: &BlockId) -> Result<&BlockStuffAug, ArchiveError> {
        let entry = self.blocks.get(id).ok_or(ArchiveError::OutOfRange)?;
        entry.block.as_ref().ok_or(ArchiveError::BlockNotFound)
    }

    pub fn get_proof_by_id(&self, id: &BlockId) -> Result<&BlockProofStuffAug, ArchiveError> {
        let entry = self.blocks.get(id).ok_or(ArchiveError::OutOfRange)?;
        entry.proof.as_ref().ok_or(ArchiveError::ProofNotFound)
    }

    pub fn get_mc_block_by_seqno(&self, seqno: u32) -> Result<&BlockStuffAug, ArchiveError> {
        let id = self
            .mc_block_ids
            .get(&seqno)
            .ok_or(ArchiveError::BlockNotFound)?;

        self.get_block_by_id(id)
    }

    pub fn get_mc_proof_by_seqno(&self, seqno: u32) -> Result<&BlockProofStuffAug, ArchiveError> {
        let id = self
            .mc_block_ids
            .get(&seqno)
            .ok_or(ArchiveError::BlockNotFound)?;

        self.get_proof_by_id(id)
    }
}

#[derive(Default)]
pub struct ArchiveDataEntry {
    pub block: Option<BlockStuffAug>,
    pub proof: Option<BlockProofStuffAug>,
}

#[derive(Clone)]
pub enum ArchiveData {
    /// The raw data is known.
    New(Bytes),
    /// Raw data is not known (due to nondeterministic serialization).
    Existing,
}

impl ArchiveData {
    /// Assumes that the object is constructed with known raw data.
    pub fn as_new_archive_data(&self) -> Result<&[u8], WithArchiveDataError> {
        match self {
            ArchiveData::New(data) => Ok(data),
            ArchiveData::Existing => Err(WithArchiveDataError),
        }
    }
}

/// Parsed data wrapper, augmented with the optional raw data.
///
/// Stores the raw data only in the context of the archive parser, or received block.
///
/// NOTE: Can be safely cloned, all raw bytes are shared (see [`Bytes`])
///
/// See: [`ArchiveData`]
#[derive(Clone)]
pub struct WithArchiveData<T> {
    pub data: T,
    pub archive_data: ArchiveData,
}

impl<T> WithArchiveData<T> {
    /// Constructs a new object from the context with known raw data.
    pub fn new<A>(data: T, archive_data: A) -> Self
    where
        Bytes: From<A>,
    {
        Self {
            data,
            archive_data: ArchiveData::New(Bytes::from(archive_data)),
        }
    }

    /// Constructs a new object from the context without known raw data.
    pub fn loaded(data: T) -> Self {
        Self {
            data,
            archive_data: ArchiveData::Existing,
        }
    }

    /// Assumes that the object is constructed with known raw data.
    pub fn as_new_archive_data(&self) -> Result<&[u8], WithArchiveDataError> {
        self.archive_data.as_new_archive_data()
    }
}

impl<T> std::ops::Deref for WithArchiveData<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug, Copy, Clone, thiserror::Error)]
#[error("archive data not loaded")]
pub struct WithArchiveDataError;

#[derive(thiserror::Error, Debug)]
pub enum ArchiveError {
    #[error("mc block seqno out of range")]
    OutOfRange,
    #[error("block not found")]
    BlockNotFound,
    #[error("proof not found")]
    ProofNotFound,
}

/// Encodes archive package segment.
pub fn make_archive_entry(filename: &str, data: &[u8]) -> Vec<u8> {
    let mut vec = Vec::with_capacity(2 + 2 + 4 + filename.len() + data.len());
    vec.extend_from_slice(&ARCHIVE_ENTRY_PREFIX);
    vec.extend_from_slice(&(filename.len() as u16).to_le_bytes());
    vec.extend_from_slice(&(data.len() as u32).to_le_bytes());
    vec.extend_from_slice(filename.as_bytes());
    vec.extend_from_slice(data);
    vec
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn correct_context() {
        const DATA: &[u8] = &[1, 2, 3];

        assert_eq!(
            WithArchiveData::new((), DATA.to_vec())
                .as_new_archive_data()
                .unwrap(),
            DATA
        );
        assert!(WithArchiveData::loaded(()).as_new_archive_data().is_err());
    }
}
