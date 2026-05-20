use tycho_storage::kv::refcount;
use tycho_types::cell::{CellDescriptor, HashBytes};
use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, rocksdb};

use super::CellsDb;
use crate::storage::shard_state::db_state::{CellsDbStateKey, CountersStore};
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

pub fn cells_v2_to_v3(
    db: &CellsDb,
    cell_counters: &mut CountersStore,
    cancelled: &CancellationFlag,
) -> Result<(), MigrationError> {
    let mut cancelled = cancelled.debounce(100);
    cells_v2_to_v3_impl(db, cell_counters, || cancelled.check())
}

fn cells_v2_to_v3_impl<F>(
    db: &CellsDb,
    cell_counters: &mut CountersStore,
    mut should_stop: F,
) -> Result<(), MigrationError>
where
    F: FnMut() -> bool,
{
    const MAX_BATCH_SIZE: usize = 100_000;

    let cf = &db.cells.cf();
    let state_cf = &db.state.cf();
    let rocksdb = db.raw().rocksdb();

    // The latest snapshot can exist if we crashed after writing the final migration
    // batch but before the migration runner bumped the DB version.
    if cell_counters
        .load_optional_snapshot(CellsDbStateKey::CounterSnapshotLatest)
        .map_err(|err| MigrationError::Custom(Box::new(err)))?
        .is_some()
    {
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_cf(state_cf, CellsDbStateKey::MigrationCounterSnapshot);
        batch.delete_cf(state_cf, CellsDbStateKey::MigrationLastFullyMigratedKey);
        rocksdb
            .write_opt(batch, db.cells.write_config())
            .map_err(MigrationError::DbError)?;
        return Ok(());
    }

    cell_counters.counters = match cell_counters
        .load_optional_snapshot(CellsDbStateKey::MigrationCounterSnapshot)
        .map_err(|err| MigrationError::Custom(Box::new(err)))?
    {
        Some(counters) => counters,
        None => cell_counters.new_empty_counters(),
    };
    let last_migrated: Option<[u8; 32]> = db
        .state
        .get(CellsDbStateKey::MigrationLastFullyMigratedKey)
        .map_err(|err| MigrationError::Custom(Box::new(err)))?
        .map(|value| {
            value.as_ref().try_into().map_err(|_err| {
                MigrationError::Custom(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid migration key",
                )))
            })
        })
        .transpose()?;

    let mut batch = rocksdb::WriteBatch::default();
    let mut batch_len = 0usize;
    let mut total_removed = 0usize;
    let mut total_migrated = 0usize;
    let mut buffer = Vec::<u8>::with_capacity(512);
    let mut counter_batch = cell_counters.counters.begin();
    counter_batch.reserve(MAX_BATCH_SIZE);

    let mut iterator = db.cells.raw_iterator();
    if let Some(last_migrated) = last_migrated {
        iterator.seek(last_migrated.as_slice());
        if iterator
            .item()
            .is_some_and(|(key, _)| key == last_migrated.as_slice())
        {
            iterator.next();
        }
    } else {
        iterator.seek_to_first();
    }

    loop {
        if should_stop() {
            return Err(MigrationError::Custom(Box::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "cancelled",
            ))));
        }

        let Some((key, value)) = iterator.item() else {
            iterator.status()?;
            break;
        };

        let (rc, value) = refcount::decode_value_with_rc(value);
        match value {
            Some(value) if rc > 0 => {
                let idx = counter_batch.alloc_idx();
                encode_indexed_value(idx, value, &mut buffer);
                batch.delete_cf(cf, key);
                batch.put_cf(cf, key, &buffer);

                counter_batch.update_raw(idx, 0, rc as u64);

                total_migrated += 1;
            }
            _ => {
                batch.delete_cf(cf, key);
                total_removed += 1;
            }
        }

        batch_len += 1;

        if batch_len >= MAX_BATCH_SIZE {
            counter_batch.apply();
            cell_counters.counters.shrink_if_needed();
            batch.put_cf(
                state_cf,
                CellsDbStateKey::MigrationLastFullyMigratedKey,
                key,
            );
            cell_counters
                .put_snapshot(&mut batch, CellsDbStateKey::MigrationCounterSnapshot)
                .map_err(|err| MigrationError::Custom(Box::new(err)))?;
            tracing::info!(total_migrated, total_removed, "applying intermediate batch");
            rocksdb.write_opt(std::mem::take(&mut batch), db.cells.write_config())?;
            batch_len = 0;
            counter_batch = cell_counters.counters.begin();
            counter_batch.reserve(MAX_BATCH_SIZE);
        }

        iterator.next();
    }

    counter_batch.apply();
    cell_counters.counters.shrink_if_needed();
    cell_counters
        .put_snapshot(&mut batch, CellsDbStateKey::CounterSnapshotLatest)
        .map_err(|err| MigrationError::Custom(Box::new(err)))?;
    batch.delete_cf(state_cf, CellsDbStateKey::MigrationCounterSnapshot);
    batch.delete_cf(state_cf, CellsDbStateKey::MigrationLastFullyMigratedKey);

    tracing::info!(
        total_migrated,
        total_removed,
        "migrated all cells to indexed rows"
    );
    rocksdb.write_opt(batch, db.cells.write_config())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tycho_storage::StorageContext;
    use tycho_storage::kv::StateVersionProvider;
    use weedb::VersionProvider;

    use super::*;
    use crate::storage::CoreStorage;
    use crate::storage::db::CellsTables;
    use crate::storage::shard_state::db_state::{CellsDbStateKey, CountersStore};
    use crate::storage::shard_state::decode_indexed_value;

    fn pool() -> Arc<rayon::ThreadPool> {
        Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn cells_v3_migration_resumes_large_interrupted_dataset() {
        // realistic distribution or something
        const TOTAL: usize = 200_000;
        const ONES: usize = 190_000;
        const ZEROS: usize = 1_000;

        let (ctx, _tmp_dir) = StorageContext::new_temp()
            .await
            .expect("failed to create temp storage context");
        let db = CoreStorage::open_cells_db(&ctx).expect("failed to open cells DB");
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let mut expected = Vec::with_capacity(TOTAL);

        let mut batch = rocksdb::WriteBatch::default();
        for i in 0..TOTAL {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&(i as u64).to_be_bytes());
            let key = HashBytes(key);

            let mut payload = vec![0u8; 256];
            payload[..8].copy_from_slice(&(i as u64).to_le_bytes());
            payload[8..16].copy_from_slice(&((i as u64) ^ 0xDEAD_BEEF_FEED_DEAD).to_le_bytes());
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
            .expect("failed to write legacy cells");

        let provider = StateVersionProvider::<crate::storage::tables::State>::new::<CellsTables>();
        provider
            .set_version(db.raw(), [0, 0, 2])
            .expect("failed to set legacy cells DB version");
        drop(db);

        let db = CoreStorage::open_cells_db(&ctx).unwrap();
        let mut cell_counters = CountersStore::open(db.clone(), pool());

        let mut stop_checks = 0usize;
        let err = cells_v2_to_v3_impl(&db, &mut cell_counters, || {
            stop_checks += 1;
            stop_checks == 150_000
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
                .get(CellsDbStateKey::MigrationCounterSnapshot)
                .unwrap()
                .is_some()
        );
        assert!(
            db.state
                .get(CellsDbStateKey::MigrationLastFullyMigratedKey)
                .unwrap()
                .is_some()
        );
        assert!(
            db.state
                .get(CellsDbStateKey::CounterSnapshotLatest)
                .unwrap()
                .is_none()
        );
        let interrupted_counters = cell_counters
            .load_snapshot(CellsDbStateKey::MigrationCounterSnapshot)
            .unwrap();
        assert_eq!(interrupted_counters.next_idx.get(), 100_000);
        let expected_last_key = {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&(99_999_u64).to_be_bytes());
            HashBytes(key)
        };
        assert_eq!(
            db.state
                .get(CellsDbStateKey::MigrationLastFullyMigratedKey)
                .expect("failed to load migration boundary")
                .expect("missing migration boundary")
                .as_ref(),
            expected_last_key.as_slice(),
        );
        assert_eq!(provider.get_version(db.raw()).unwrap(), Some([0, 0, 2]));

        cells_v2_to_v3_impl(&db, &mut cell_counters, || false).unwrap();

        let counters = cell_counters
            .load_snapshot(CellsDbStateKey::CounterSnapshotLatest)
            .unwrap();
        assert!(
            db.state
                .get(CellsDbStateKey::MigrationCounterSnapshot)
                .unwrap()
                .is_none()
        );
        assert!(
            db.state
                .get(CellsDbStateKey::MigrationLastFullyMigratedKey)
                .unwrap()
                .is_none()
        );
        assert_eq!(counters.next_idx.get(), (TOTAL - ZEROS) as u64);

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
            let (idx, loaded_payload) =
                decode_indexed_value(value.as_ref()).expect("indexed value is missing idx prefix");
            assert_eq!(loaded_payload, payload.as_slice());
            assert_eq!(counters.get(idx), rc);
            indexed_rows += 1;
        }
        assert_eq!(deleted_rows, ZEROS);
        assert_eq!(indexed_rows, TOTAL - ZEROS);
    }

    #[tokio::test]
    async fn cells_v3_migration_resumes_after_deleted_boundary_key() {
        const TOTAL: usize = 100_002;
        const DELETED_BOUNDARY: usize = 99_999;

        let (ctx, _tmp_dir) = StorageContext::new_temp().await.unwrap();
        let db = CoreStorage::open_cells_db(&ctx).unwrap();

        let mut batch = rocksdb::WriteBatch::default();
        for i in 0..TOTAL {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&(i as u64).to_be_bytes());

            let mut payload = vec![0u8; 32];
            payload[..8].copy_from_slice(&(i as u64).to_le_bytes());

            let rc = if i != DELETED_BOUNDARY { 1u64 } else { 0 };
            let mut value = Vec::with_capacity(size_of::<i64>() + payload.len());
            value.extend_from_slice(&(rc as i64).to_le_bytes());
            if rc > 0 {
                value.extend_from_slice(&payload);
            }

            batch.put_cf(&db.cells.cf(), key.as_slice(), value);
        }
        db.rocksdb()
            .write_opt(batch, db.cells.write_config())
            .unwrap();

        let provider = StateVersionProvider::<crate::storage::tables::State>::new::<CellsTables>();
        provider.set_version(db.raw(), [0, 0, 2]).unwrap();

        let mut cell_counters = CountersStore::open(db.clone(), pool());
        let mut stop_checks = 0usize;
        let err = cells_v2_to_v3_impl(&db, &mut cell_counters, || {
            stop_checks += 1;
            stop_checks == 100_001
        })
        .unwrap_err();
        assert!(matches!(
            err,
            MigrationError::Custom(err)
                if err
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|err| err.kind() == std::io::ErrorKind::Interrupted)
        ));

        let mut expected_last_key = [0u8; 32];
        expected_last_key[..8].copy_from_slice(&(DELETED_BOUNDARY as u64).to_be_bytes());
        assert_eq!(
            db.state
                .get(CellsDbStateKey::MigrationLastFullyMigratedKey)
                .unwrap()
                .unwrap()
                .as_ref(),
            expected_last_key.as_slice(),
        );
        assert!(
            db.cells
                .get(expected_last_key.as_slice())
                .expect("failed to load deleted boundary cell")
                .is_none()
        );

        cells_v2_to_v3_impl(&db, &mut cell_counters, || false)
            .expect("failed to resume cells v3 migration");

        let counters = cell_counters
            .load_snapshot(CellsDbStateKey::CounterSnapshotLatest)
            .expect("missing latest counter snapshot");
        assert_eq!(counters.next_idx.get(), (TOTAL - 1) as u64);

        let mut first_resumed_key = [0u8; 32];
        first_resumed_key[..8].copy_from_slice(&((DELETED_BOUNDARY as u64) + 1).to_be_bytes());
        let value = db
            .cells
            .get(first_resumed_key.as_slice())
            .expect("failed to load first resumed key")
            .expect("first resumed key was skipped");
        let (idx, payload) =
            decode_indexed_value(value.as_ref()).expect("indexed value is missing idx prefix");
        assert_eq!(idx.get(), DELETED_BOUNDARY as u64);
        assert_eq!(
            &payload[..8],
            &((DELETED_BOUNDARY as u64) + 1).to_le_bytes(),
        );
    }
}
