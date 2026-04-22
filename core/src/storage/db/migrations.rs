use tycho_storage::kv::refcount;
use tycho_types::cell::{CellDescriptor, HashBytes};
use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, rocksdb};

use super::CellsDb;
use crate::storage::shard_state::counters::{Counters, NextIdx, RefCount};
use crate::storage::shard_state::db_state::{
    CellsDbStateKey, load_counter_snapshot, put_counter_snapshot,
};
use crate::storage::shard_state::encode_indexed_value;

/// Removes `repr_hash` from the cell data and rearranges repr depth.
pub fn cells_v1_to_v2(db: &CellsDb, cancelled: &CancellationFlag) -> Result<(), MigrationError> {
    const MAX_BATCH_SIZE: usize = 100_000;

    let cf = &db.cells.cf();
    let rocksdb = db.raw().rocksdb();

    let mut batch = rocksdb::WriteBatch::default();
    let mut batch_len = 0usize;
    let mut total_removed = 0usize;
    let mut total_migrated = 0usize;

    let mut buffer = Vec::<u8>::with_capacity(512);
    let mut cancelled = cancelled.debounce(100);

    let mut iterator = db.cells.raw_iterator();
    iterator.seek_to_first();
    loop {
        if cancelled.check() {
            return Err(MigrationError::Custom(Box::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "cancelled".to_owned(),
            ))));
        }

        if batch_len >= MAX_BATCH_SIZE {
            tracing::info!(total_migrated, total_removed, "applying intermediate batch");
            rocksdb.write_opt(std::mem::take(&mut batch), db.cells.write_config())?;
            batch_len = 0;
        }

        let Some((key, value)) = iterator.item() else {
            iterator.status()?;
            break;
        };

        let (rc, value) = refcount::decode_value_with_rc(value);
        let Some(value) = value else {
            assert_eq!(rc, 0);
            batch.delete_cf(cf, key);
            batch_len += 1;
            total_removed += 1;
            iterator.next();
            continue;
        };
        assert!(rc > 0);

        assert!(value.len() >= 4);
        let descriptor = CellDescriptor::new([value[0], value[1]]);
        let hash_count = descriptor.hash_count() as usize;
        let ref_count = descriptor.reference_count() as usize;
        let data_len = descriptor.byte_len() as usize;

        let old_len = 4 + data_len + hash_count * (32 + 2) + ref_count * 32;
        let old_len_with_stats = old_len + 16;

        let new_len = 6 + data_len + (hash_count - 1) * (32 + 2) + ref_count * 32;

        if value.len() == new_len {
            iterator.next();
            continue;
        }

        // NOTE: We also check here for initial cells with unused tree counters.
        assert!(
            value.len() == old_len || value.len() == old_len_with_stats,
            "unexpected cell value len: {}, expected {old_len} or {old_len_with_stats}",
            value.len(),
        );

        let repr_hash_offset = 4 + data_len + (hash_count - 1) * (32 + 2);
        let repr_hash = HashBytes::from_slice(&value[repr_hash_offset..repr_hash_offset + 32]);
        if key != repr_hash.as_slice() {
            return Err(MigrationError::Custom(Box::new(std::io::Error::other(
                format!(
                    "repr hash differs from the cell key: key={}, hash={repr_hash}",
                    HashBytes::from_slice(key)
                ),
            ))));
        }

        let repr_depth = &value[repr_hash_offset + 32..repr_hash_offset + 34];
        assert_eq!(repr_depth.len(), 2);

        buffer.clear();
        buffer.extend_from_slice(&rc.to_le_bytes());
        buffer.extend_from_slice(&value[0..4]);
        buffer.extend_from_slice(repr_depth);
        buffer.extend_from_slice(&value[4..repr_hash_offset]);
        buffer.extend_from_slice(&value[repr_hash_offset + 34..old_len]);

        batch.put_cf(cf, key, buffer.as_slice());
        batch_len += 1;
        total_migrated += 1;
        iterator.next();
    }

    if batch_len > 0 {
        tracing::info!(total_migrated, total_removed, "applying intermediate batch");
        rocksdb.write_opt(batch, db.cells.write_config())?;
    }

    tracing::info!(
        total_migrated,
        total_removed,
        "migrated all cells to a new version"
    );
    Ok(())
}

pub fn cells_v2_to_v3(db: &CellsDb, cancelled: &CancellationFlag) -> Result<(), MigrationError> {
    let mut cancelled = cancelled.debounce(100);
    cells_v2_to_v3_impl(db, || cancelled.check())
}

fn cells_v2_to_v3_impl<F>(db: &CellsDb, mut should_stop: F) -> Result<(), MigrationError>
where
    F: FnMut() -> bool,
{
    const MAX_BATCH_SIZE: usize = 100_000;

    let cf = &db.cells.cf();
    let rocksdb = db.raw().rocksdb();

    match load_counter_snapshot(db, CellsDbStateKey::CounterSnapshotLatest) {
        Ok(_) => {
            let mut batch = rocksdb::WriteBatch::default();
            batch.delete_cf(
                &db.state.cf(),
                CellsDbStateKey::MigrationCounterSnapshot.as_bytes(),
            );
            batch.delete_cf(
                &db.state.cf(),
                CellsDbStateKey::MigrationLastFullyMigratedKey.as_bytes(),
            );
            db.raw()
                .rocksdb()
                .write_opt(batch, db.cells.write_config())
                .map_err(MigrationError::DbError)?;
            return Ok(());
        }
        Err(crate::storage::shard_state::db_state::CellsDbStateError::MissingKey(
            CellsDbStateKey::CounterSnapshotLatest,
        )) => {}
        Err(err) => return Err(MigrationError::Custom(Box::new(err))),
    }

    let mut counters = match load_counter_snapshot(db, CellsDbStateKey::MigrationCounterSnapshot) {
        Ok(counters) => counters,
        Err(crate::storage::shard_state::db_state::CellsDbStateError::MissingKey(
            CellsDbStateKey::MigrationCounterSnapshot,
        )) => Counters::new(NextIdx::new(0)),
        Err(err) => return Err(MigrationError::Custom(Box::new(err))),
    };
    let last_migrated = match db
        .state
        .get(CellsDbStateKey::MigrationLastFullyMigratedKey.as_bytes())
    {
        Ok(Some(value)) => {
            let bytes: [u8; 32] = value.as_ref().try_into().map_err(|_| {
                MigrationError::Custom(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid migration key",
                )))
            })?;
            Some(HashBytes(bytes))
        }
        Ok(None) => None,
        Err(err) => return Err(MigrationError::Custom(Box::new(err))),
    };

    let mut batch = rocksdb::WriteBatch::default();
    let mut batch_len = 0usize;
    let mut total_removed = 0usize;
    let mut total_migrated = 0usize;
    let mut buffer = Vec::<u8>::with_capacity(512);
    let mut latest_key = last_migrated;
    let mut stop_requested = false;

    let mut iterator = db.cells.raw_iterator();
    if let Some(last_migrated) = last_migrated {
        iterator.seek(last_migrated.as_slice());
        iterator.next();
    } else {
        iterator.seek_to_first();
    }

    loop {
        if batch_len >= MAX_BATCH_SIZE {
            if let Some(latest_key) = latest_key {
                batch.put_cf(
                    &db.state.cf(),
                    CellsDbStateKey::MigrationLastFullyMigratedKey.as_bytes(),
                    latest_key.as_slice(),
                );
                put_counter_snapshot(
                    &mut batch,
                    db,
                    CellsDbStateKey::MigrationCounterSnapshot,
                    &counters,
                )
                .map_err(|err| MigrationError::Custom(Box::new(err)))?;
            }
            tracing::info!(total_migrated, total_removed, "applying intermediate batch");
            rocksdb.write_opt(std::mem::take(&mut batch), db.cells.write_config())?;
            batch_len = 0;

            if stop_requested {
                return Err(interrupted_migration());
            }
        }

        if should_stop() {
            if batch_len == 0 {
                return Err(interrupted_migration());
            }
            stop_requested = true;
        }

        let Some((key, value)) = iterator.item() else {
            iterator.status()?;
            break;
        };

        let key = HashBytes::from_slice(key);
        let (rc, value) = refcount::decode_value_with_rc(value);
        match value {
            Some(value) if rc > 0 => {
                let idx = counters.alloc_idx();
                encode_indexed_value(idx, value, &mut buffer);
                batch.put_cf(cf, key.as_slice(), buffer.as_slice());

                let rc = RefCount::new(rc as u64);
                if rc != RefCount::ONE {
                    counters
                        .set(idx, rc)
                        .map_err(|err| MigrationError::Custom(Box::new(err)))?;
                }

                total_migrated += 1;
            }
            _ => {
                batch.delete_cf(cf, key.as_slice());
                total_removed += 1;
            }
        }

        latest_key = Some(key);
        batch_len += 1;
        iterator.next();
    }

    put_counter_snapshot(
        &mut batch,
        db,
        CellsDbStateKey::CounterSnapshotLatest,
        &counters,
    )
    .map_err(|err| MigrationError::Custom(Box::new(err)))?;
    batch.delete_cf(
        &db.state.cf(),
        CellsDbStateKey::MigrationCounterSnapshot.as_bytes(),
    );
    batch.delete_cf(
        &db.state.cf(),
        CellsDbStateKey::MigrationLastFullyMigratedKey.as_bytes(),
    );

    tracing::info!(
        total_migrated,
        total_removed,
        "migrated all cells to indexed rows"
    );
    rocksdb.write_opt(batch, db.cells.write_config())?;
    Ok(())
}

fn interrupted_migration() -> MigrationError {
    MigrationError::Custom(Box::new(std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "cancelled".to_owned(),
    )))
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tycho_storage::StorageContext;
    use tycho_storage::kv::StateVersionProvider;
    use weedb::VersionProvider;

    use super::*;
    use crate::storage::db::CellsTables;
    use crate::storage::shard_state::counters::Idx;
    use crate::storage::shard_state::db_state::{CellsDbStateKey, load_counter_snapshot};

    #[tokio::test]
    async fn cells_v3_migration_resumes_large_interrupted_dataset() {
        const TOTAL: usize = 200_000;
        const ONES: usize = 190_000;
        const ZEROS: usize = 1_000;

        let (ctx, _tmp_dir) = StorageContext::new_temp().await.unwrap();
        let db: CellsDb = ctx
            .open_preconfigured(crate::storage::CELLS_DB_SUBDIR)
            .unwrap();

        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let mut expected = Vec::with_capacity(TOTAL);

        let mut batch = rocksdb::WriteBatch::default();
        for i in 0..TOTAL {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&(i as u64).to_be_bytes());
            let key = HashBytes(key);

            let mut payload = vec![0u8; 256];
            payload[..8].copy_from_slice(&(i as u64).to_le_bytes());
            payload[8..16].copy_from_slice(&((i as u64) ^ 0xA5A5_A5A5_A5A5_A5A5).to_le_bytes());
            for (offset, byte) in payload[16..].iter_mut().enumerate() {
                *byte = ((i + offset) % 251) as u8;
            }

            let rc = if i < ONES {
                1u64
            } else if i < ONES + ZEROS {
                0u64
            } else {
                rng.random_range(2..=10_000)
            };

            let mut value = Vec::with_capacity(size_of::<i64>() + payload.len());
            value.extend_from_slice(&(rc as i64).to_le_bytes());
            if rc > 0 {
                value.extend_from_slice(&payload);
            }
            batch.put_cf(&db.cells.cf(), key.as_slice(), value);
            expected.push((key, payload, rc));
        }
        db.rocksdb()
            .write_opt(batch, db.cells.write_config())
            .unwrap();

        let provider = StateVersionProvider::<crate::storage::tables::State>::new::<CellsTables>();
        provider.set_version(db.raw(), [0, 0, 2]).unwrap();
        drop(db);

        let db: CellsDb = ctx
            .open_preconfigured(crate::storage::CELLS_DB_SUBDIR)
            .unwrap();

        let mut stop_checks = 0usize;
        let err = cells_v2_to_v3_impl(&db, || {
            stop_checks += 1;
            stop_checks == 50_000
        })
        .unwrap_err();
        assert!(matches!(
            err,
            MigrationError::Custom(err)
                if err
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|err| err.kind() == std::io::ErrorKind::Interrupted)
        ));

        assert!(
            db.state
                .get(CellsDbStateKey::MigrationCounterSnapshot.as_bytes())
                .unwrap()
                .is_some()
        );
        assert!(
            db.state
                .get(CellsDbStateKey::MigrationLastFullyMigratedKey.as_bytes())
                .unwrap()
                .is_some()
        );
        assert!(
            db.state
                .get(CellsDbStateKey::CounterSnapshotLatest.as_bytes())
                .unwrap()
                .is_none()
        );
        let interrupted_counters =
            load_counter_snapshot(&db, CellsDbStateKey::MigrationCounterSnapshot).unwrap();
        assert_eq!(interrupted_counters.next_idx().get(), 100_000);
        let expected_last_key = {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&(99_999_u64).to_be_bytes());
            HashBytes(key)
        };
        assert_eq!(
            db.state
                .get(CellsDbStateKey::MigrationLastFullyMigratedKey.as_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            expected_last_key.as_slice(),
        );
        assert_eq!(provider.get_version(db.raw()).unwrap(), Some([0, 0, 2]));

        cells_v2_to_v3_impl(&db, || false).unwrap();

        let counters = load_counter_snapshot(&db, CellsDbStateKey::CounterSnapshotLatest).unwrap();
        assert!(
            db.state
                .get(CellsDbStateKey::MigrationCounterSnapshot.as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            db.state
                .get(CellsDbStateKey::MigrationLastFullyMigratedKey.as_bytes())
                .unwrap()
                .is_none()
        );
        assert_eq!(counters.next_idx().get(), (TOTAL - ZEROS) as u64);

        let mut deleted_rows = 0usize;
        let mut indexed_rows = 0usize;
        for (key, payload, rc) in expected {
            let value = db.cells.get(key.as_slice()).unwrap();
            if rc == 0 {
                assert!(value.is_none());
                deleted_rows += 1;
                continue;
            }

            let value = value.unwrap();
            let prefix: [u8; 8] = value.as_ref()[..8].try_into().unwrap();
            let idx = Idx::new(u64::from_le_bytes(prefix));
            let loaded_payload = &value.as_ref()[8..];
            assert_eq!(loaded_payload, payload.as_slice());
            assert_eq!(counters.get(idx).unwrap().get(), rc);
            indexed_rows += 1;
        }
        assert_eq!(deleted_rows, ZEROS);
        assert_eq!(indexed_rows, TOTAL - ZEROS);
    }
}
