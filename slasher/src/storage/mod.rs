use std::sync::Arc;

use anyhow::Result;
use tycho_slasher_traits::ValidationSessionId;
use tycho_storage::StorageContext;
use tycho_types::cell::HashBytes;
use weedb::OwnedSnapshot;

use self::db::{SlasherDb, tables};
use self::models::StoredBlocksBatch;
use crate::BlocksBatch;

pub mod db;
pub mod models;

const SLASHER_DB_SUBDIR: &str = "slasher";

#[derive(Clone)]
#[repr(transparent)]
pub struct SlasherStorage {
    inner: Arc<Inner>,
}

impl SlasherStorage {
    pub fn open(ctx: &StorageContext) -> Result<Self> {
        let db = ctx.open_preconfigured(SLASHER_DB_SUBDIR)?;

        Ok(Self {
            inner: Arc::new(Inner { db }),
        })
    }

    pub fn db(&self) -> &SlasherDb {
        &self.inner.db
    }

    /// Creates a new snapshot.
    pub fn snapshot(&self) -> SlasherStorageSnapshot {
        SlasherStorageSnapshot {
            snapshot: Arc::new(self.inner.db.owned_snapshot()),
        }
    }

    pub fn store_blocks_batch(
        &self,
        session_id: ValidationSessionId,
        validator_idx: u16,
        batch: &BlocksBatch,
    ) -> Result<()> {
        let mut key = [0u8; tables::BlockBatches::KEY_LEN];
        key[0..4].copy_from_slice(&session_id.seqno.to_be_bytes());
        key[4..8].copy_from_slice(&session_id.vset_switch_round.to_be_bytes());
        key[8..12].copy_from_slice(&session_id.catchain_seqno.to_be_bytes());
        key[12..14].copy_from_slice(&validator_idx.to_be_bytes());
        key[14..18].copy_from_slice(&batch.start_seqno.to_be_bytes());

        let value = tl_proto::serialize(StoredBlocksBatch::wrap(batch));

        self.inner.db.block_batches.insert(key.as_slice(), value)?;
        Ok(())
    }

    /// Removes all block batches for sessions BEFORE the specified.
    pub fn remove_outdated_batches(&self, latest_session_id: ValidationSessionId) -> Result<()> {
        let db = &self.inner.db;

        let mut key = [0u8; tables::BlockBatches::KEY_LEN];
        key[0..4].copy_from_slice(&latest_session_id.seqno.to_be_bytes());

        db.rocksdb().delete_range_cf_opt(
            &db.block_batches.cf(),
            [0u8; tables::BlockBatches::KEY_LEN],
            key,
            db.block_batches.write_config(),
        )?;
        Ok(())
    }
}

struct Inner {
    db: SlasherDb,
}

#[derive(Clone)]
pub struct SlasherStorageSnapshot {
    snapshot: Arc<OwnedSnapshot>,
}
