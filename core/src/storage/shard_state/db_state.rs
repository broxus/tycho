use std::io::Cursor;

use anyhow::Result;
use tycho_util::compression::{ZstdDecompress, ZstdError, zstd_compress};
use weedb::rocksdb::{self, WriteBatch};

use super::counters::{Counters, CountersError};
use crate::storage::CellsDb;

const COUNTER_SNAPSHOT_LATEST_KEY: &[u8] = b"counter_snapshot/latest";
const MIGRATION_LAST_FULLY_MIGRATED_KEY: &[u8] = b"migration/last_fully_migrated_key";
const MIGRATION_COUNTER_SNAPSHOT_KEY: &[u8] = b"migration/counter_snapshot";
const RAW_IMPORT_IN_PROGRESS_KEY: &[u8] = b"raw_import/in_progress";
const COUNTERS_SNAPSHOT_COMPRESSION_LEVEL: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellsDbStateKey {
    CounterSnapshotLatest,
    MigrationLastFullyMigratedKey,
    MigrationCounterSnapshot,
    RawImportInProgress,
}

impl CellsDbStateKey {
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::CounterSnapshotLatest => COUNTER_SNAPSHOT_LATEST_KEY,
            Self::MigrationLastFullyMigratedKey => MIGRATION_LAST_FULLY_MIGRATED_KEY,
            Self::MigrationCounterSnapshot => MIGRATION_COUNTER_SNAPSHOT_KEY,
            Self::RawImportInProgress => RAW_IMPORT_IN_PROGRESS_KEY,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CellsDbStateError {
    #[error("cells state DB access failed: {0}")]
    Db(#[from] rocksdb::Error),
    #[error("state key missing: {0:?}")]
    MissingKey(CellsDbStateKey),
    #[error("counter snapshot zstd decompression failed: {0}")]
    CounterSnapshotDecompression(#[from] ZstdError),
    #[error("counter snapshot payload is invalid: {0}")]
    InvalidCounterSnapshot(#[from] CountersError),
}

pub fn load_counter_snapshot(
    db: &CellsDb,
    key: CellsDbStateKey,
) -> Result<Counters, CellsDbStateError> {
    let Some(value) = db.state.get(key.as_bytes())? else {
        return Err(CellsDbStateError::MissingKey(key));
    };
    let mut payload = Vec::new();
    ZstdDecompress::begin(value.as_ref())?.decompress(&mut payload)?;
    let mut reader = Cursor::new(payload);
    Ok(Counters::load_from(&mut reader)?)
}

pub fn put_counter_snapshot(
    batch: &mut WriteBatch,
    db: &CellsDb,
    key: CellsDbStateKey,
    counters: &Counters,
) -> Result<(), CellsDbStateError> {
    let mut payload = Vec::new();
    counters.write_to(&mut payload)?;

    let mut compressed = Vec::new();
    zstd_compress(
        payload.as_slice(),
        &mut compressed,
        COUNTERS_SNAPSHOT_COMPRESSION_LEVEL,
    );

    batch.put_cf(&db.state.cf(), key.as_bytes(), compressed);
    Ok(())
}

pub fn cleanup_interrupted_raw_import(db: &CellsDb) -> Result<(), CellsDbStateError> {
    let Some(_) = db
        .state
        .get(CellsDbStateKey::RawImportInProgress.as_bytes())?
    else {
        return Ok(());
    };

    tracing::warn!("cleaning interrupted bootstrap raw import state");

    let mut batch = WriteBatch::default();
    batch.delete_range_cf(&db.cells.cf(), &[0x00; 32], &[0xff; 32]);
    batch.delete_range_cf(&db.temp_cells.cf(), &[0x00; 32], &[0xff; 32]);

    for item in db.shard_states.iterator(rocksdb::IteratorMode::Start) {
        let (key, _) = item?;
        batch.delete_cf(&db.shard_states.cf(), key);
    }

    batch.delete_cf(
        &db.state.cf(),
        CellsDbStateKey::CounterSnapshotLatest.as_bytes(),
    );
    batch.delete_cf(
        &db.state.cf(),
        CellsDbStateKey::RawImportInProgress.as_bytes(),
    );
    db.rocksdb().write(batch)?;
    Ok(())
}

pub fn is_table_empty<T>(table: &weedb::Table<T>) -> Result<bool>
where
    T: weedb::ColumnFamily,
{
    let mut iterator = table.raw_iterator();
    iterator.seek_to_first();
    iterator.status()?;
    Ok(iterator.item().is_none())
}

pub fn ensure_table_is_empty<T>(table: &weedb::Table<T>, error: &'static str) -> Result<()>
where
    T: weedb::ColumnFamily,
{
    anyhow::ensure!(is_table_empty(table)?, "{error}");
    Ok(())
}
