use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut};
use rayon::ThreadPool;
use weedb::rocksdb::{self, WriteBatch};

use super::counters::{Counters, CountersError, NextIdx};
use crate::storage::CellsDb;
use crate::storage::db::is_table_empty;

pub(super) const CELL_HASH_RANGE_START: [u8; 32] = [0x00; 32];
// RocksDB range deletion has an exclusive end bound. 33 bytes keeps [0xff; 32]
// inside the fixed-size hash keyspace cleanup.
pub(super) const CELL_HASH_RANGE_END: [u8; 33] = [0xff; 33];

const COUNTER_SNAPSHOT_LATEST_KEY: &[u8] = b"counter_snapshot/latest";
const MIGRATION_LAST_FULLY_MIGRATED_KEY: &[u8] = b"migration/last_fully_migrated_key";
const MIGRATION_COUNTER_SNAPSHOT_KEY: &[u8] = b"migration/counter_snapshot";
const RAW_IMPORT_IN_PROGRESS_KEY: &[u8] = b"raw_import/in_progress";
const CELL_NURSERY_VERSION_KEY: &[u8] = b"cell_nursery/version";
const CELL_NURSERY_COMMITTED_ID_KEY: &[u8] = b"cell_nursery/committed_id";
const CELL_NURSERY_CHECKPOINT_ID_KEY: &[u8] = b"cell_nursery/checkpoint_id";
const CELL_NURSERY_CHECKPOINT_BLAKE3_KEY: &[u8] = b"cell_nursery/checkpoint_blake3";
const COUNTER_SNAPSHOT_VERSION: u8 = 1;
const COUNTER_SNAPSHOT_CODEC_RAW: u8 = 0;
const COUNTER_SNAPSHOT_HEADER_LEN: usize = 2;

pub struct CountersStore {
    pub(crate) counters: Counters,
    cells_db: CellsDb,
    worker_pool: Arc<ThreadPool>,
}

impl CountersStore {
    pub fn open(cells_db: CellsDb, worker_pool: Arc<ThreadPool>) -> Self {
        Self {
            cells_db,
            counters: Counters::new(NextIdx::new(0), worker_pool.clone()),
            worker_pool,
        }
    }

    pub fn new_empty_counters(&self) -> Counters {
        Counters::new(NextIdx::new(0), self.worker_pool.clone())
    }

    pub fn load_latest_or_empty(&mut self) -> Result<(), CellsDbStateError> {
        self.counters = match self.load_snapshot(CellsDbStateKey::CounterSnapshotLatest) {
            Ok(counters) => counters,
            Err(CellsDbStateError::MissingKey(CellsDbStateKey::CounterSnapshotLatest)) => {
                if !is_table_empty(&self.cells_db.cells)
                    .map_err(|err| CellsDbStateError::Custom(err.to_string()))?
                {
                    return Err(CellsDbStateError::Custom(
                        "indexed cells DB is missing latest counters snapshot".to_owned(),
                    ));
                }
                self.new_empty_counters()
            }
            Err(err) => return Err(err),
        };

        Ok(())
    }

    pub fn load_snapshot(&self, key: CellsDbStateKey) -> Result<Counters, CellsDbStateError> {
        self.load_optional_snapshot(key)?
            .ok_or(CellsDbStateError::MissingKey(key))
    }

    pub fn load_optional_snapshot(
        &self,
        key: CellsDbStateKey,
    ) -> Result<Option<Counters>, CellsDbStateError> {
        let Some(value) = self.cells_db.state.get(key)? else {
            return Ok(None);
        };

        let payload = decode_counter_snapshot_value(value.as_ref())?;
        Ok(Some(Counters::deserialize_trusted(
            payload,
            self.worker_pool.clone(),
        )?))
    }

    pub fn put_snapshot(
        &mut self,
        batch: &mut WriteBatch,
        key: CellsDbStateKey,
    ) -> Result<(), CellsDbStateError> {
        let serialize_started = std::time::Instant::now();
        let payload = self.counters.serialize();
        metrics::histogram!("tycho_storage_cell_counters_snapshot_serialize_time")
            .record(serialize_started.elapsed());
        metrics::gauge!("tycho_storage_cell_counters_snapshot_raw_bytes").set(payload.len() as f64);
        self.counters.record_metrics();

        let stored = encode_counter_snapshot_value(&payload);
        batch.put_cf(&self.cells_db.state.cf(), key, stored);
        Ok(())
    }

    // Raw zerostate import is only complete after the marker is cleared. If we
    // crash before that, imported cell rows, temp rows, shard roots and counter
    // snapshots are suspect and must be hidden before normal shard-state
    // storage is constructed. The marker is bootstrap-scoped; it is not a
    // complete cross-DB rollback for block handles or persistent-state files.
    pub fn cleanup_interrupted_raw_import(&mut self) -> Result<(), CellsDbStateError> {
        let Some(_) = self
            .cells_db
            .state
            .get(CellsDbStateKey::RawImportInProgress)?
        else {
            return Ok(());
        };

        tracing::warn!("cleaning interrupted bootstrap raw import state");

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(
            &self.cells_db.cells.cf(),
            CELL_HASH_RANGE_START.as_slice(),
            CELL_HASH_RANGE_END.as_slice(),
        );
        batch.delete_range_cf(
            &self.cells_db.temp_cells.cf(),
            CELL_HASH_RANGE_START.as_slice(),
            CELL_HASH_RANGE_END.as_slice(),
        );
        // `BlockId` keys are encoded as 80 bytes.
        let from = [0x00; 80];
        let to = [0xff; 81];
        batch.delete_range_cf(
            &self.cells_db.shard_states.cf(),
            from.as_slice(),
            to.as_slice(),
        );

        batch.delete_cf(
            &self.cells_db.state.cf(),
            CellsDbStateKey::CounterSnapshotLatest,
        );
        batch.delete_cf(
            &self.cells_db.state.cf(),
            CellsDbStateKey::RawImportInProgress,
        );
        self.cells_db.rocksdb().write(batch)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellsDbStateKey {
    CounterSnapshotLatest,
    MigrationLastFullyMigratedKey,
    MigrationCounterSnapshot,
    RawImportInProgress,
    CellNurseryVersion,
    CellNurseryCommittedId,
    CellNurseryCheckpointId,
    CellNurseryCheckpointBlake3,
}

impl AsRef<[u8]> for CellsDbStateKey {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::CounterSnapshotLatest => COUNTER_SNAPSHOT_LATEST_KEY,
            Self::MigrationLastFullyMigratedKey => MIGRATION_LAST_FULLY_MIGRATED_KEY,
            Self::MigrationCounterSnapshot => MIGRATION_COUNTER_SNAPSHOT_KEY,
            Self::RawImportInProgress => RAW_IMPORT_IN_PROGRESS_KEY,
            Self::CellNurseryVersion => CELL_NURSERY_VERSION_KEY,
            Self::CellNurseryCommittedId => CELL_NURSERY_COMMITTED_ID_KEY,
            Self::CellNurseryCheckpointId => CELL_NURSERY_CHECKPOINT_ID_KEY,
            Self::CellNurseryCheckpointBlake3 => CELL_NURSERY_CHECKPOINT_BLAKE3_KEY,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CellsDbStateError {
    #[error("cells state DB access failed: {0}")]
    Db(#[from] rocksdb::Error),
    #[error("state key missing: {0:?}")]
    MissingKey(CellsDbStateKey),
    #[error("counter snapshot header is invalid")]
    InvalidCounterSnapshotHeader,
    #[error("counter snapshot payload is invalid: {0}")]
    InvalidCounterSnapshot(#[from] CountersError),
    #[error("counter snapshot state operation failed: {0}")]
    Custom(String),
}

fn encode_counter_snapshot_value(payload: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(COUNTER_SNAPSHOT_HEADER_LEN + payload.len());
    data.put_u8(COUNTER_SNAPSHOT_VERSION);
    data.put_u8(COUNTER_SNAPSHOT_CODEC_RAW);
    data.put_slice(payload);
    data
}

fn decode_counter_snapshot_value(mut data: &[u8]) -> Result<&[u8], CellsDbStateError> {
    if data.remaining() < COUNTER_SNAPSHOT_HEADER_LEN {
        return Err(CellsDbStateError::InvalidCounterSnapshotHeader);
    }

    let version = data.get_u8();
    let codec = data.get_u8();
    if version != COUNTER_SNAPSHOT_VERSION || codec != COUNTER_SNAPSHOT_CODEC_RAW {
        return Err(CellsDbStateError::InvalidCounterSnapshotHeader);
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use tycho_storage::StorageContext;

    use super::*;
    use crate::storage::CoreStorage;

    #[tokio::test]
    async fn counter_snapshot_state_roundtrip_and_invalid_value() -> Result<()> {
        let (ctx, _tempdir) = StorageContext::new_temp().await?;
        let db: CellsDb = CoreStorage::open_cells_db(&ctx)?;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let mut store = CountersStore::open(db.clone(), Arc::new(pool));

        let idx = store.counters.alloc_idx();
        let mut batch = store.counters.begin();
        batch.update_raw(idx, 0, 2);
        batch.apply();

        let mut batch = WriteBatch::default();
        store.put_snapshot(&mut batch, CellsDbStateKey::CounterSnapshotLatest)?;
        db.rocksdb().write(batch)?;

        let value = db
            .state
            .get(CellsDbStateKey::CounterSnapshotLatest)?
            .expect("snapshot key must be stored");
        assert!(value.len() > COUNTER_SNAPSHOT_HEADER_LEN);
        assert_eq!(value[0], COUNTER_SNAPSHOT_VERSION);
        assert_eq!(value[1], COUNTER_SNAPSHOT_CODEC_RAW);

        let loaded = store.load_snapshot(CellsDbStateKey::CounterSnapshotLatest)?;
        assert_eq!(loaded.next_idx.get(), 1);
        assert_eq!(loaded.get(idx), 2);

        let mut batch = WriteBatch::default();
        batch.put_cf(
            &db.state.cf(),
            CellsDbStateKey::CounterSnapshotLatest,
            b"invalid",
        );
        db.rocksdb().write(batch)?;

        let err = store
            .load_snapshot(CellsDbStateKey::CounterSnapshotLatest)
            .expect_err("invalid snapshot value must fail");
        assert!(matches!(
            err,
            CellsDbStateError::InvalidCounterSnapshotHeader
        ));

        Ok(())
    }
}
