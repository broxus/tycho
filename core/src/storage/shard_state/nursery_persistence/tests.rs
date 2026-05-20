use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use tycho_storage::StorageContext;
use tycho_storage::fs::Dir;
use tycho_types::cell::HashBytes;
use weedb::rocksdb::WriteBatch;

use super::format::{HASH_LEN, encode_record};
use super::{
    NurseryPersistence, WAL_FILE_NAME, checkpoint_file_name, checkpoint_tmp_file_name,
    is_stale_checkpoint_file,
};
use crate::storage::shard_state::cell_nursery::{CellNursery, NurseryDelta, NurseryEntryRecord};
use crate::storage::shard_state::counters::Idx;
use crate::storage::shard_state::nursery_persistence::format::decode_record;
use crate::storage::shard_state::util::test_hash;
use crate::storage::{CellsDb, CoreStorage};

const DEFAULT_CHECKPOINT_WAL_THRESHOLD: u64 = bytesize::MIB * 100;

fn hash(byte: u8) -> HashBytes {
    HashBytes([byte; HASH_LEN])
}

fn insert(hash: HashBytes, idx: u64, born_round: u32, data: &[u8]) -> NurseryEntryRecord {
    NurseryEntryRecord {
        hash,
        idx: Idx::new(idx),
        born_round,
        data: Bytes::copy_from_slice(data),
    }
}

fn pool() -> Arc<rayon::ThreadPool> {
    Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap(),
    )
}

fn commit(entries: Vec<(HashBytes, Idx, u32, Vec<u8>)>) -> NurseryDelta {
    let mut commit = NurseryDelta::default();
    for (hash, idx, born_round, data) in entries {
        commit.inserts.push(NurseryEntryRecord {
            hash,
            idx,
            born_round,
            data: Bytes::from(data),
        });
    }
    commit
}

fn append_and_apply(
    persistence: &mut NurseryPersistence,
    cells_db: &CellsDb,
    nursery: &CellNursery,
    commit: NurseryDelta,
) -> anyhow::Result<()> {
    let mut batch = WriteBatch::default();
    persistence.append_commit(cells_db, &mut batch, &commit)?;
    cells_db.rocksdb().write(batch)?;
    nursery.apply_commit(commit);
    Ok(())
}

fn wal_len(dir: &Dir) -> anyhow::Result<u64> {
    Ok(dir.file(WAL_FILE_NAME).read(true).open()?.metadata()?.len())
}

#[test]
fn record_roundtrip() {
    let commit = NurseryDelta {
        inserts: vec![
            insert(hash(1), 10, 7, &[1, 2, 3]),
            insert(hash(2), 11, 8, &[4, 5]),
            insert(hash(4), 12, 9, &[]),
        ],
        removes: vec![hash(3)],
    };

    let encoded = encode_record(42, &commit);
    let mut buffer = Vec::new();
    let decoded = decode_record(&mut encoded.bytes().as_ref(), &mut buffer).unwrap();

    let delta = decoded.delta;
    assert_eq!(encoded.id(), 42);
    assert_eq!(delta.inserts.len(), 3);
    assert_eq!(delta.removes, vec![hash(3)]);
    assert_eq!(delta.inserts[0].data.as_ref(), [1, 2, 3]);
    assert_eq!(delta.inserts[1].data.as_ref(), [4, 5]);
    assert_eq!(delta.inserts[2].data.as_ref(), b"");
    assert_eq!(decoded.stats.insert_ops, 3);
    assert_eq!(decoded.stats.remove_ops, 1);
}

#[test]
fn checkpoint_record_roundtrip_rejects_removes() -> anyhow::Result<()> {
    let tempdir = tempfile::tempdir()?;
    let dir = Dir::new(tempdir.path())?;
    let nursery = CellNursery::new(NonZeroUsize::new(1).unwrap(), pool());

    let commit = NurseryDelta {
        inserts: vec![insert(hash(9), 42, 11, &[6, 7, 8])],
        removes: vec![],
    };

    let encoded = encode_record(7, &commit);
    let mut file = dir
        .file(checkpoint_file_name(7))
        .create(true)
        .write(true)
        .truncate(true)
        .open()?;

    file.write_all(encoded.bytes())?;
    drop(file);

    NurseryPersistence::load_checkpoint(&dir, 7, encoded.hash(), &nursery)?;
    assert_eq!(nursery.get(&hash(9)).unwrap().as_ref(), [6, 7, 8]);

    let encoded = encode_record(8, &NurseryDelta {
        inserts: vec![],
        removes: vec![hash(1)],
    });
    let mut file = dir
        .file(checkpoint_file_name(8))
        .create(true)
        .write(true)
        .truncate(true)
        .open()?;
    file.write_all(encoded.bytes())?;
    drop(file);

    assert_eq!(
        NurseryPersistence::load_checkpoint(&dir, 8, encoded.hash(), &nursery)
            .unwrap_err()
            .to_string(),
        "checkpoint payload must not contain removes",
    );
    Ok(())
}

#[test]
fn checkpoint_paths_follow_contract() {
    assert_eq!(
        checkpoint_tmp_file_name(42),
        PathBuf::from("cell_nursery.checkpoint.tmp.42")
    );
    assert_eq!(
        checkpoint_file_name(42),
        PathBuf::from("cell_nursery.checkpoint.42")
    );
}

#[test]
fn record_rejects_hash_mismatch_before_decode() {
    let commit = NurseryDelta {
        inserts: vec![insert(hash(1), 1, 0, &[1, 2, 3])],
        removes: vec![],
    };
    let record = encode_record(1, &commit);
    let mut bytes = record.bytes().to_vec();
    bytes[0] ^= 1;

    let mut buffer = Vec::new();
    assert_eq!(
        decode_record(&mut bytes.as_slice(), &mut buffer)
            .err()
            .expect("decode must reject hash mismatch")
            .to_string(),
        "nursery payload hash mismatch",
    );
}

#[test]
fn replay_decode_copies_row_bytes() {
    let commit = NurseryDelta {
        inserts: vec![insert(hash(1), 1, 0, &[1, 2, 3])],
        removes: vec![],
    };
    let encoded = encode_record(1, &commit);

    let mut buffer = Vec::new();
    let decoded = decode_record(&mut encoded.bytes().as_ref(), &mut buffer).unwrap();

    assert_eq!(decoded.delta.inserts[0].data.as_ref(), [1, 2, 3]);
}

#[test]
fn stale_checkpoint_cleanup_removes_old_checkpoint_files() {
    let current = checkpoint_file_name(10);

    assert!(!is_stale_checkpoint_file(&current, &current));
    assert!(is_stale_checkpoint_file(&checkpoint_file_name(9), &current));
    assert!(is_stale_checkpoint_file(
        &checkpoint_tmp_file_name(10),
        &current,
    ));
    assert!(is_stale_checkpoint_file(
        &checkpoint_tmp_file_name(9),
        &current,
    ));
    assert!(!is_stale_checkpoint_file(
        Path::new("cell_nursery.wal"),
        &current,
    ));
    assert!(!is_stale_checkpoint_file(
        Path::new("cell_nursery.checkpointish.9"),
        &current,
    ));
}

#[tokio::test]
async fn checkpoint_if_needed_roundtrips_after_reopen() -> anyhow::Result<()> {
    let (ctx, _tempdir) = StorageContext::new_temp().await?;
    let cells_db = CoreStorage::open_cells_db(&ctx)?;
    let nursery_dir = ctx.root_dir().path().join("nursery");
    let dir = Dir::new(&nursery_dir)?;
    let (mut persistence, nursery) = NurseryPersistence::open(
        &cells_db,
        &nursery_dir,
        NonZeroUsize::new(1).unwrap(),
        pool(),
        u64::MAX,
    )?;

    append_and_apply(
        &mut persistence,
        &cells_db,
        &nursery,
        commit(vec![
            (test_hash(1), Idx::new(10), 7, vec![1, 2, 3]),
            (test_hash(2), Idx::new(11), 8, vec![4, 5]),
        ]),
    )?;

    let mut remove = NurseryDelta::default();
    remove.removes.push(test_hash(1));
    append_and_apply(&mut persistence, &cells_db, &nursery, remove)?;

    persistence.checkpoint_if_needed(&cells_db, &nursery)?;
    assert!(!dir.file(checkpoint_file_name(2)).exists());
    assert_ne!(wal_len(&dir)?, 0);
    drop(persistence);

    let (mut persistence, nursery) = NurseryPersistence::open(
        &cells_db,
        &nursery_dir,
        NonZeroUsize::new(1).unwrap(),
        pool(),
        1,
    )?;
    persistence.checkpoint_if_needed(&cells_db, &nursery)?;

    assert!(dir.file(checkpoint_file_name(2)).exists());
    assert_eq!(wal_len(&dir)?, 0);
    assert_eq!(persistence.checkpoint_id, 2);
    assert_eq!(persistence.committed_id, 2);
    drop(persistence);

    let (_persistence, nursery) = NurseryPersistence::open(
        &cells_db,
        &nursery_dir,
        NonZeroUsize::new(1).unwrap(),
        pool(),
        DEFAULT_CHECKPOINT_WAL_THRESHOLD,
    )?;
    assert!(nursery.get(&test_hash(1)).is_none());
    assert_eq!(nursery.get(&test_hash(2)).unwrap().as_ref(), [4, 5]);
    Ok(())
}

#[tokio::test]
async fn reopen_truncates_incomplete_uncommitted_wal_record() -> anyhow::Result<()> {
    let (ctx, _tempdir) = StorageContext::new_temp().await?;
    let cells_db = CoreStorage::open_cells_db(&ctx)?;
    let nursery_dir = ctx.root_dir().path().join("nursery");
    let dir = Dir::new(&nursery_dir)?;
    let (mut persistence, nursery) = NurseryPersistence::open(
        &cells_db,
        &nursery_dir,
        NonZeroUsize::new(1).unwrap(),
        pool(),
        DEFAULT_CHECKPOINT_WAL_THRESHOLD,
    )?;

    for i in 1..=5 {
        append_and_apply(
            &mut persistence,
            &cells_db,
            &nursery,
            commit(vec![(
                test_hash(i),
                Idx::new(u64::from(i)),
                i.into(),
                vec![i],
            )]),
        )?;
    }

    let committed_wal_len = wal_len(&dir)?;
    drop(persistence);

    let partial = encode_record(6, &commit(vec![(test_hash(6), Idx::new(6), 6, vec![6, 6])]));
    let mut wal = dir.file(WAL_FILE_NAME).append(true).open()?;
    wal.write_all(&partial.bytes()[..partial.bytes().len() / 2])?;
    wal.flush()?;
    assert!(wal_len(&dir)? > committed_wal_len);

    let (persistence, nursery) = NurseryPersistence::open(
        &cells_db,
        &nursery_dir,
        NonZeroUsize::new(1).unwrap(),
        pool(),
        DEFAULT_CHECKPOINT_WAL_THRESHOLD,
    )?;

    assert_eq!(persistence.committed_id, 5);
    assert_eq!(persistence.wal_size, committed_wal_len);
    assert_eq!(wal_len(&dir)?, committed_wal_len);
    for i in 1..=5 {
        assert_eq!(nursery.get(&test_hash(i)).unwrap().as_ref(), [i]);
    }
    assert!(nursery.get(&test_hash(6)).is_none());
    Ok(())
}
