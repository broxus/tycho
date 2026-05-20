//! Persistence works like this:
//! We append data to WAL and get `committed_id`.
//! After this we publish `committed_id` to `RocksDB`.
//!
//! After that we can run checkpoint, truncate WAL to 0, and write latest nursery
//! snapshot to disk. It is `checkpoint_id`.
//!
//! On replay we skip all records with `id <= checkpoint_id` because checkpoint
//! already contains them. We replay records with `checkpoint_id < id <= committed_id`.
//! Everything after `committed_id` is orphan records and is truncated.

use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail, ensure};
use rayon::ThreadPool;
use tycho_storage::fs::Dir;
use weedb::rocksdb::WriteBatch;

use self::format::{EncodedRecord, HASH_LEN, decode_record, encode_record};
use crate::storage::CellsDb;
use crate::storage::shard_state::cell_nursery::{CellNursery, NurseryDelta};
use crate::storage::shard_state::db_state::CellsDbStateKey;
use crate::storage::shard_state::util::elapsed_us;

mod format;

#[cfg(test)]
mod tests;

pub struct NurseryPersistence {
    dir: Dir,
    wal: std::fs::File,
    committed_id: u64,
    checkpoint_id: u64,
    wal_size: u64,
    checkpoint_wal_threshold: u64,
}

#[derive(Clone, Copy, Default)]
pub struct WalAppendProfile {
    pub wal_bytes: u64,
    pub total_us: u64,
}

const WAL_FILE_NAME: &str = "cell_nursery.wal";
const STATE_VERSION: u64 = 1;

impl NurseryPersistence {
    pub fn open(
        cells_db: &CellsDb,
        cell_nursery_dir: &Path,
        entry_stripes: NonZeroUsize,
        worker_pool: Arc<ThreadPool>,
        checkpoint_wal_threshold: u64,
    ) -> Result<(Self, CellNursery)> {
        let dir = Dir::new(cell_nursery_dir)?;
        let state = load_state(cells_db)?;

        let nursery = CellNursery::new(entry_stripes, worker_pool);

        if state.checkpoint_id > 0 {
            let checkpoint_hash = state
                .checkpoint_hash
                .as_ref()
                .context("checkpoint hash must be present")?;
            Self::load_checkpoint(&dir, state.checkpoint_id, checkpoint_hash, &nursery)?;
        }

        if !dir.file_exists(WAL_FILE_NAME) && state.committed_id != state.checkpoint_id {
            bail!("WAL missing while committed id is ahead of checkpoint id");
        }

        let mut wal = dir
            .file(WAL_FILE_NAME)
            .create(true)
            .append(true)
            .read(true)
            .open()
            .context("failed to open cell nursery WAL")?;

        let wal_size = replay_wal(&mut wal, state.committed_id, state.checkpoint_id, &nursery)?;

        let persistence = Self {
            dir,
            wal,
            committed_id: state.committed_id,
            checkpoint_id: state.checkpoint_id,
            wal_size,
            checkpoint_wal_threshold,
        };
        persistence.record_state_metrics();

        Ok((persistence, nursery))
    }

    pub fn append_commit(
        &mut self,
        cells_db: &CellsDb,
        batch: &mut WriteBatch,
        commit: &NurseryDelta,
    ) -> Result<WalAppendProfile> {
        let total_started_at = Instant::now();
        if commit.is_empty() {
            return Ok(WalAppendProfile::default());
        }

        let nursery_id = self.committed_id + 1;
        let record = encode_record(nursery_id, commit);

        self.wal.write_all(record.bytes())?;

        self.committed_id = nursery_id;
        let entry_len = record.bytes().len() as u64;
        self.wal_size += entry_len;

        let state_cf = &cells_db.state.cf();
        batch.put_cf(
            state_cf,
            CellsDbStateKey::CellNurseryVersion,
            STATE_VERSION.to_le_bytes(),
        );
        batch.put_cf(
            state_cf,
            CellsDbStateKey::CellNurseryCommittedId,
            nursery_id.to_le_bytes(),
        );

        metrics::counter!("tycho_storage_cell_nursery_wal_records").increment(1);
        metrics::counter!("tycho_storage_cell_nursery_wal_bytes").increment(entry_len);
        record.stats().record_wal_write_metrics();
        self.record_state_metrics();

        Ok(WalAppendProfile {
            wal_bytes: entry_len,
            total_us: elapsed_us(total_started_at),
        })
    }

    pub fn checkpoint_if_needed(
        &mut self,
        cells_db: &CellsDb,
        nursery: &CellNursery,
    ) -> Result<()> {
        if self.wal_size < self.checkpoint_wal_threshold || self.checkpoint_id == self.committed_id
        {
            return Ok(());
        }

        let started_at = Instant::now();
        let delta = NurseryDelta {
            inserts: nursery.snapshot_entries(),
            ..Default::default()
        };
        let record = encode_record(self.committed_id, &delta);
        let wal_size = self.wal_size;

        self.write_checkpoint_file(&record)?;

        self.update_checkpoint_state(cells_db, record.hash())?;
        self.reset_wal()?;
        self.remove_stale_checkpoints();

        let stats = record.stats();
        tracing::info!(
            checkpoint_id = self.checkpoint_id,
            wal_bytes = wal_size,
            threshold_bytes = self.checkpoint_wal_threshold,
            checkpoint_payload_bytes = stats.payload_bytes,
            checkpoint_metadata_bytes = stats.metadata_bytes,
            checkpoint_cell_bytes = stats.cell_bytes,
            checkpoint_insert_ops = stats.insert_ops,
            "checkpointed cell nursery WAL",
        );

        metrics::gauge!("tycho_storage_cell_nursery_checkpoint_bytes")
            .set(stats.payload_bytes as f64);
        metrics::histogram!("tycho_storage_cell_nursery_checkpoint_time")
            .record(started_at.elapsed());
        metrics::counter!("tycho_storage_cell_nursery_checkpoint_count").increment(1);
        self.record_state_metrics();

        Ok(())
    }

    fn load_checkpoint(
        dir: &Dir,
        checkpoint_id: u64,
        checkpoint_hash: &[u8; HASH_LEN],
        nursery: &CellNursery,
    ) -> Result<()> {
        let mut file = dir
            .file(checkpoint_file_name(checkpoint_id))
            .read(true)
            .open()
            .context("failed to open cell nursery checkpoint")?;

        let mut buffer = Vec::new();
        let decoded = decode_record(&mut file, &mut buffer)?;
        drop(buffer);

        ensure!(
            decoded.id == checkpoint_id,
            "checkpoint record id does not match checkpoint state"
        );
        ensure!(
            &decoded.hash == checkpoint_hash,
            "unexpected checkpoint hash"
        );

        tracing::info!(stats = ?decoded.stats, "loaded nurcery checkpoint");
        ensure!(
            decoded.delta.removes.is_empty(),
            "checkpoint payload must not contain removes"
        );
        nursery.apply_commit(decoded.delta);
        Ok(())
    }

    fn update_checkpoint_state(&mut self, cells_db: &CellsDb, hash: &[u8; HASH_LEN]) -> Result<()> {
        let mut batch = WriteBatch::default();
        let state_cf = &cells_db.state.cf();

        batch.put_cf(
            state_cf,
            CellsDbStateKey::CellNurseryVersion,
            STATE_VERSION.to_le_bytes(),
        );
        batch.put_cf(
            state_cf,
            CellsDbStateKey::CellNurseryCheckpointId,
            self.committed_id.to_le_bytes(),
        );
        batch.put_cf(state_cf, CellsDbStateKey::CellNurseryCheckpointBlake3, hash);

        cells_db.rocksdb().write(batch)?;
        self.checkpoint_id = self.committed_id;
        Ok(())
    }

    fn reset_wal(&mut self) -> Result<()> {
        self.wal.flush()?;
        self.wal.set_len(0)?;
        self.wal.seek(SeekFrom::Start(0))?;
        self.wal_size = 0;
        Ok(())
    }

    fn write_checkpoint_file(&self, record: &EncodedRecord) -> Result<()> {
        let name = checkpoint_file_name(record.id());
        let tmp = checkpoint_tmp_file_name(record.id());

        let mut file = self
            .dir
            .file(&tmp)
            .create(true)
            .write(true)
            .truncate(true)
            .open()
            .context("failed to open temporary cell nursery checkpoint")?;

        file.write_all(record.bytes())?;
        file.flush()?;
        drop(file);

        self.dir.file(&tmp).rename(name)?;
        Ok(())
    }

    fn remove_stale_checkpoints(&self) {
        let current = checkpoint_file_name(self.checkpoint_id);
        let Ok(entries) = self.dir.entries() else {
            return;
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            if is_stale_checkpoint_file(Path::new(&name), &current) {
                _ = self.dir.remove_file(&name);
            }
        }
    }

    fn record_state_metrics(&self) {
        metrics::gauge!("tycho_storage_cell_nursery_wal_live_bytes").set(self.wal_size as f64);
        metrics::gauge!("tycho_storage_cell_nursery_commits_since_checkpoint")
            .set(self.committed_id.saturating_sub(self.checkpoint_id) as f64);
        metrics::gauge!("tycho_storage_cell_nursery_checkpoint_wal_threshold_bytes")
            .set(self.checkpoint_wal_threshold as f64);
    }
}

#[derive(Clone, Copy)]
enum NurseryStateVersion {
    Fresh,
    V1,
}

struct LoadedNurseryState {
    committed_id: u64,
    checkpoint_id: u64,
    checkpoint_hash: Option<[u8; HASH_LEN]>,
}

fn replay_wal(
    file: &mut std::fs::File,
    committed_id: u64,
    checkpoint_id: u64,
    nursery: &CellNursery,
) -> Result<u64> {
    let file_len = file.metadata()?.len();
    file.seek(SeekFrom::Start(0))?;

    if committed_id == checkpoint_id {
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        return Ok(0);
    }

    let mut next_id = checkpoint_id + 1;
    let mut replayed_records = 0u64;
    let mut skipped_records = 0u64;
    let mut cursor = 0u64;

    let mut buffer = Vec::new();
    while next_id <= committed_id {
        let record = match decode_record(file, &mut buffer) {
            Ok(header) => header,
            Err(e) => {
                tracing::warn!("partial entry found in nurcery WAL: {e:?}");
                break;
            }
        };

        if record.id > committed_id {
            tracing::warn!(found_id = record.id, committed_id, "truncating WAL entires");
            break;
        }

        cursor = file.stream_position()?;

        if record.id <= checkpoint_id {
            skipped_records += 1;
            continue;
        }
        ensure!(record.id == next_id, "WAL committed record id gap detected");

        nursery.apply_commit(record.delta);
        tracing::info!(
            record.id,
            stats = ?record.stats,
            "applied WAL entry"
        );

        replayed_records += 1;
        next_id += 1;
    }

    ensure!(
        next_id == committed_id + 1,
        "WAL replay stopped before committed id boundary"
    );

    file.set_len(cursor)?;
    file.seek(SeekFrom::Start(cursor))?;

    tracing::info!(
        committed_id,
        checkpoint_id,
        replayed_records,
        skipped_records,
        truncated_wal_bytes = file_len - cursor,
        wal_size = cursor,
        "replayed cell nursery WAL",
    );

    Ok(cursor)
}

fn load_state(cells_db: &CellsDb) -> Result<LoadedNurseryState> {
    let load_u64 = |key| -> Result<Option<u64>> {
        let value = cells_db.state.get(key)?;
        let Some(value) = value.as_deref() else {
            return Ok(None);
        };
        let bytes: [u8; size_of::<u64>()] = value
            .try_into()
            .context("invalid cell nursery u64 state value length")?;
        Ok(Some(u64::from_le_bytes(bytes)))
    };
    let load_hash = |key| -> Result<Option<[u8; HASH_LEN]>> {
        let value = cells_db.state.get(key)?;
        let Some(value) = value.as_deref() else {
            return Ok(None);
        };
        let bytes: [u8; HASH_LEN] = value
            .try_into()
            .context("invalid cell nursery checkpoint hash value length")?;
        Ok(Some(bytes))
    };

    let committed_id = load_u64(CellsDbStateKey::CellNurseryCommittedId)?.unwrap_or_default();
    let checkpoint_id = load_u64(CellsDbStateKey::CellNurseryCheckpointId)?.unwrap_or_default();
    let checkpoint_hash = load_hash(CellsDbStateKey::CellNurseryCheckpointBlake3)?;
    let version = match load_u64(CellsDbStateKey::CellNurseryVersion)? {
        None => NurseryStateVersion::Fresh,
        Some(v) if v == STATE_VERSION => NurseryStateVersion::V1,
        Some(_) => bail!("unsupported cell nursery state version"),
    };

    ensure!(
        checkpoint_id <= committed_id,
        "cell nursery checkpoint id is greater than committed id"
    );

    match version {
        NurseryStateVersion::Fresh => {
            ensure!(
                committed_id == 0 && checkpoint_id == 0 && checkpoint_hash.is_none(),
                "fresh cell nursery state cannot have committed/checkpoint ids or hash"
            );
        }
        NurseryStateVersion::V1 => {
            ensure!(
                (checkpoint_id > 0) == checkpoint_hash.is_some(),
                "cell nursery checkpoint hash presence must match non-zero checkpoint id"
            );
        }
    }

    Ok(LoadedNurseryState {
        committed_id,
        checkpoint_id,
        checkpoint_hash,
    })
}

fn checkpoint_file_name(id: u64) -> PathBuf {
    PathBuf::from(format!("cell_nursery.checkpoint.{id}"))
}

fn checkpoint_tmp_file_name(id: u64) -> PathBuf {
    PathBuf::from(format!("cell_nursery.checkpoint.tmp.{id}"))
}

fn is_stale_checkpoint_file(path: &Path, current: &Path) -> bool {
    let Some(name) = path.to_str() else {
        return false;
    };
    path != current
        && (name
            .strip_prefix("cell_nursery.checkpoint.")
            .is_some_and(|id| id.parse::<u64>().is_ok())
            || name
                .strip_prefix("cell_nursery.checkpoint.tmp.")
                .is_some_and(|id| id.parse::<u64>().is_ok()))
}
