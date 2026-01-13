use tycho_storage::kv::refcount;
use tycho_types::cell::{CellDescriptor, HashBytes};
use tycho_util::sync::CancellationFlag;
use weedb::{MigrationError, rocksdb};

use super::CellsDb;

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
