//! # Archive structure
//!
//! - Archive prefix (4 bytes): `0x65 0x8F 0x14 0x29`
//! - For each archive entry:
//!  * Archive entry header ([`ArchiveEntryHeader`] as TL)
//!  * Archive entry data

use std::collections::BTreeMap;

use anyhow::Result;
use bytes::Bytes;
use everscale_types::models::BlockId;
use tycho_util::FastHashMap;

pub use self::proto::{
    ArchiveEntryHeader, ArchiveEntryType, ARCHIVE_ENTRY_HEADER_LEN, ARCHIVE_PREFIX,
};
pub use self::reader::{ArchiveEntry, ArchiveReader, ArchiveReaderError, ArchiveVerifier};
use crate::block::{BlockProofStuff, BlockProofStuffAug, BlockStuff, BlockStuffAug};
use crate::queue::{QueueDiffStuff, QueueDiffStuffAug};

mod entry_id;
mod proto;
mod reader;

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

            let id = entry.block_id;
            if id.is_masterchain() {
                res.mc_block_ids.insert(id.seqno, id);
            }

            let parsed = res.blocks.entry(id).or_default();

            match entry.ty {
                ArchiveEntryType::Block => {
                    anyhow::ensure!(parsed.block.is_none(), "duplicate block data for: {id}");
                    parsed.block = Some(data.slice_ref(entry.data));
                }
                ArchiveEntryType::Proof => {
                    anyhow::ensure!(parsed.proof.is_none(), "duplicate block proof for: {id}");
                    parsed.proof = Some(data.slice_ref(entry.data));
                }
                ArchiveEntryType::QueueDiff => {
                    anyhow::ensure!(
                        parsed.queue_diff.is_none(),
                        "duplicate queue diff for: {id}"
                    );
                    parsed.queue_diff = Some(data.slice_ref(entry.data));
                }
            }
        }

        Ok(res)
    }

    // TODO: Make async
    pub fn get_entry_by_id(
        &self,
        id: &BlockId,
    ) -> Result<(BlockStuffAug, BlockProofStuffAug, QueueDiffStuffAug), ArchiveError> {
        // TODO: Rayon go brr
        let block = self.get_block_by_id(id)?;
        let proof = self.get_proof_by_id(id)?;
        let queue_diff = self.get_queue_diff_by_id(id)?;

        Ok((block, proof, queue_diff))
    }

    pub fn get_block_by_id(&self, id: &BlockId) -> Result<BlockStuffAug, ArchiveError> {
        let entry = self.blocks.get(id).ok_or(ArchiveError::OutOfRange)?;
        entry
            .block
            .as_ref()
            .ok_or(ArchiveError::BlockNotFound)
            .and_then(|data| {
                let block = BlockStuff::deserialize_checked(id, data)?;
                Ok(WithArchiveData::new::<Bytes>(block, data.clone()))
            })
    }

    pub fn get_proof_by_id(&self, id: &BlockId) -> Result<BlockProofStuffAug, ArchiveError> {
        let entry = self.blocks.get(id).ok_or(ArchiveError::OutOfRange)?;
        entry
            .proof
            .as_ref()
            .ok_or(ArchiveError::BlockNotFound)
            .and_then(|data| {
                let proof = BlockProofStuff::deserialize(id, data)?;
                Ok(WithArchiveData::new::<Bytes>(proof, data.clone()))
            })
    }

    pub fn get_queue_diff_by_id(&self, id: &BlockId) -> Result<QueueDiffStuffAug, ArchiveError> {
        let entry = self.blocks.get(id).ok_or(ArchiveError::OutOfRange)?;
        entry
            .queue_diff
            .as_ref()
            .ok_or(ArchiveError::BlockNotFound)
            .and_then(|data| {
                let diff = QueueDiffStuff::deserialize(id, data)?;
                Ok(WithArchiveData::new::<Bytes>(diff, data.clone()))
            })
    }
}

#[derive(Default)]
pub struct ArchiveDataEntry {
    pub block: Option<Bytes>,
    pub proof: Option<Bytes>,
    pub queue_diff: Option<Bytes>,
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
    #[error("block id is out of range")]
    OutOfRange,
    #[error("block not found")]
    BlockNotFound,
    #[error("proof not found")]
    ProofNotFound,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
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
