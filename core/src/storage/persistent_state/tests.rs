use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use bytesize::ByteSize;
use tempfile::TempDir;
use tycho_block_util::block::{ShardPrefix, split_shard_ident};
use tycho_block_util::queue::{
    QueueDiffStuff, QueueKey, QueueStateHeader, RouterAddr, RouterPartitions,
};
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::fs::{Dir, FileBuilder};
use tycho_storage::{StorageConfig, StorageContext};
use tycho_types::boc::Boc;
use tycho_types::cell::{Cell, CellBuilder, CellSlice, HashBytes, Lazy};
use tycho_types::models::{
    BlockId, CurrencyCollection, IntAddr, IntMsgInfo, IntermediateAddr, Message, MsgEnvelope,
    MsgInfo, OutMsg, OutMsgDescr, OutMsgNew, OutMsgQueueUpdates, ShardIdent, StdAddr,
};
use tycho_types::num::Tokens;
use tycho_util::compression::zstd_decompress_simple;
use tycho_util::fs::MappedFile;
use tycho_util::{FastHashMap, FastHashSet};

use crate::storage::config::StatePartsConfig;
use crate::storage::persistent_state::{
    CacheKey, PersistentStateKind, PersistentStateStorage, QueueStateReader, QueueStateWriter,
    ShardStateWriter,
};
use crate::storage::shard_state::StoreStateFromFileResult;
use crate::storage::{CoreStorage, CoreStorageConfig, NewBlockMeta};

#[tokio::test]
async fn persistent_shard_state() -> Result<()> {
    tycho_util::test::init_logger("persistent_shard_state", "debug");

    let (ctx, tmp_dir) = StorageContext::new_temp().await?;
    let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

    assert!(storage.node_state().load_init_mc_block_id().is_none());

    let shard_states = storage.shard_state_storage();
    let persistent_states = storage.persistent_state_storage();

    // Read zerostate
    static ZEROSTATE_BOC: &[u8] = include_bytes!("../../../../core/tests/data/zerostate.boc");
    let zerostate_root = Boc::decode(ZEROSTATE_BOC)?;
    let zerostate_id = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 0,
        root_hash: *zerostate_root.repr_hash(),
        file_hash: Boc::file_hash_blake(ZEROSTATE_BOC),
    };

    let zerostate = ShardStateStuff::from_root(
        &zerostate_id,
        zerostate_root,
        shard_states.min_ref_mc_state().insert_untracked(),
    )?;

    // Write zerostate to db
    let (handle, _) = storage.block_handle_storage().create_or_load_handle(
        &zerostate_id,
        NewBlockMeta::zero_state(zerostate.as_ref().gen_utime, true),
    );

    shard_states
        .store_state(&handle, &zerostate, Default::default())
        .await?;

    // Check seqno
    let min_ref_mc_state = shard_states.min_ref_mc_state();
    // NOTE: Zerostates do not affect the minimal seqno reference.
    assert_eq!(min_ref_mc_state.seqno(), None);

    // Load zerostate from db
    {
        let loaded_state = shard_states.load_state(0, zerostate.block_id()).await?;
        assert_eq!(zerostate.state(), loaded_state.state());
        assert_eq!(zerostate.block_id(), loaded_state.block_id());
        assert_eq!(zerostate.root_cell(), loaded_state.root_cell());
        // NOTE: `loaded_state` must be dropped here since cells are preventing storage from drop
    }

    // Write persistent state to file
    assert!(persistent_states.load_oldest_known_handle().is_none());

    persistent_states
        .store_shard_state(0, &handle, zerostate.ref_mc_state_handle().clone())
        .await?;

    // Check if state exists
    let exist = persistent_states.state_exists(zerostate.block_id(), PersistentStateKind::Shard);
    assert!(exist);

    let read_verify_state = || async {
        let persistent_state_data = persistent_states
            .read_state_chunk(zerostate.block_id(), 0, PersistentStateKind::Shard, None)
            .await
            .unwrap();

        let boc = zstd_decompress_simple(&persistent_state_data)?;

        // Check state
        let cell = Boc::decode(&boc)?;
        assert_eq!(&cell, zerostate.root_cell());
        Ok::<_, anyhow::Error>(())
    };

    let verify_descriptor_cache = |expected_mc_seqno: u32| {
        let cached = persistent_states
            .inner
            .descriptor_cache
            .get(&CacheKey::from((zerostate_id, PersistentStateKind::Shard)))
            .unwrap();
        assert_eq!(cached.mc_seqno, expected_mc_seqno);
    };

    verify_descriptor_cache(0);

    let expected_set = FastHashSet::from_iter([zerostate_id]);

    {
        let index = persistent_states
            .inner
            .descriptor_cache
            .mc_seqno_to_block_ids()
            .lock();
        assert_eq!(index.get(&0), Some(&expected_set));
    }

    // Read persistent state a couple of times to check if it is stateless
    for _ in 0..2 {
        read_verify_state().await?;
    }

    // Reuse persistent state for a different block
    let new_mc_seqno = 123123;
    persistent_states
        .store_shard_state(
            new_mc_seqno,
            &handle,
            zerostate.ref_mc_state_handle().clone(),
        )
        .await?;

    // Check if state exists
    let exist = persistent_states.state_exists(zerostate.block_id(), PersistentStateKind::Shard);
    assert!(exist);
    for _ in 0..2 {
        read_verify_state().await?;
    }

    // Check if state file was reused
    let file_name = PersistentStateKind::Shard.make_file_name(&zerostate_id, None);
    let prev_file = persistent_states
        .inner
        .descriptor_cache
        .mc_states_dir(0)
        .file(&file_name);
    let new_file = persistent_states
        .inner
        .descriptor_cache
        .mc_states_dir(new_mc_seqno)
        .file(file_name);

    let prev_file = std::fs::read(prev_file.path())?;
    let new_file = std::fs::read(new_file.path())?;
    assert_eq!(prev_file, new_file);

    verify_descriptor_cache(new_mc_seqno);

    {
        let index = persistent_states
            .inner
            .descriptor_cache
            .mc_seqno_to_block_ids()
            .lock();
        assert!(!index.contains_key(&0));
        assert_eq!(index.get(&new_mc_seqno), Some(&expected_set));
    }

    // Close the previous storage instance
    drop(storage);

    // And reload it
    let ctx = StorageContext::new(StorageConfig::new_potato(tmp_dir.path())).await?;
    let new_storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

    let new_persistent_states = new_storage.persistent_state_storage();

    {
        let index = new_persistent_states
            .inner
            .descriptor_cache
            .mc_seqno_to_block_ids()
            .lock();
        assert!(!index.contains_key(&0));
        assert_eq!(index.get(&new_mc_seqno), Some(&expected_set));
    }

    let new_cached = new_persistent_states
        .inner
        .descriptor_cache
        .get(&CacheKey::from((zerostate_id, PersistentStateKind::Shard)))
        .unwrap()
        .clone();
    assert_eq!(new_cached.mc_seqno, new_mc_seqno);
    assert_eq!(new_cached.file.as_slice(), new_file);

    Ok(())
}

#[tokio::test]
async fn persistent_queue_state_read_write() -> Result<()> {
    tycho_util::test::init_logger("persistent_queue_state_read_write", "debug");

    struct SimpleBlock {
        queue_diff: QueueDiffStuff,
        out_msgs: OutMsgDescr,
    }

    let (ctx, _temp_dir) = StorageContext::new_temp().await?;
    let storage = CoreStorage::open(ctx, CoreStorageConfig::new_potato()).await?;

    let target_mc_seqno = 10;

    // Prepare blocks with queue diffs
    let shard = ShardIdent::MASTERCHAIN;
    let mut blocks = Vec::new();

    let mut prev_hash = HashBytes::ZERO;
    let mut target_message_count = 0;

    let mut dst_router = RouterPartitions::default();
    let mut src_router = RouterPartitions::default();

    dst_router.insert(
        0.into(),
        BTreeSet::from([RouterAddr::from_int_addr(&IntAddr::Std(StdAddr::from((
            0,
            HashBytes::from([1; 32]),
        ))))
        .unwrap()]),
    );

    src_router.insert(
        1.into(),
        BTreeSet::from([RouterAddr::from_int_addr(&IntAddr::Std(StdAddr::from((
            -1,
            HashBytes::from([3; 32]),
        ))))
        .unwrap()]),
    );

    for seqno in 1..=target_mc_seqno {
        let start_lt = seqno as u64 * 10000;

        let mut messages = Vec::new();
        for i in 0..5000 {
            let message = Message {
                info: MsgInfo::Int(IntMsgInfo {
                    created_lt: start_lt + i,
                    ..Default::default()
                }),
                init: None,
                body: CellSlice::default(),
                layout: None,
            };
            let cell = CellBuilder::build_from(message)?;
            messages.push((
                *cell.repr_hash(),
                CurrencyCollection::ZERO,
                OutMsg::New(OutMsgNew {
                    out_msg_envelope: Lazy::new(&MsgEnvelope {
                        cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                        next_addr: IntermediateAddr::FULL_DEST_SAME_WORKCHAIN,
                        fwd_fee_remaining: Tokens::ZERO,
                        message: Lazy::from_raw(cell)?,
                    })?,
                    transaction: Lazy::from_raw(Cell::default())?,
                }),
            ));
        }
        target_message_count += messages.len();

        messages.sort_unstable_by(|(a, _, _), (b, _, _)| a.cmp(b));
        let out_msgs = OutMsgDescr::try_from_sorted_slice(&messages)?;

        let queue_diff = QueueDiffStuff::builder(shard, seqno, &prev_hash)
            .with_processed_to([(shard, QueueKey::min_for_lt(0))].into())
            .with_messages(
                &QueueKey::max_for_lt(0),
                &QueueKey::max_for_lt(0),
                messages.iter().map(|(hash, _, _)| hash),
            )
            .with_router(src_router.clone(), dst_router.clone())
            .serialize();

        let block_id = BlockId {
            shard,
            seqno,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };

        prev_hash = *queue_diff.hash();

        let queue_diff = queue_diff.build(&block_id).data;

        blocks.push(SimpleBlock {
            queue_diff,
            out_msgs,
        });
    }

    // Write blocks to file
    let persistent_states = storage.persistent_state_storage();
    let states_dir = persistent_states
        .inner
        .descriptor_cache
        .prepare_persistent_states_dir(target_mc_seqno)?;

    let target_header = QueueStateHeader {
        shard_ident: shard,
        seqno: target_mc_seqno,
        queue_diffs: blocks
            .iter()
            .rev()
            .map(|block| block.queue_diff.as_ref().clone())
            .collect(),
    };

    let target_block_id = BlockId {
        shard,
        seqno: target_mc_seqno,
        root_hash: HashBytes::ZERO,
        file_hash: HashBytes::ZERO,
    };
    QueueStateWriter::new(
        &states_dir,
        &target_block_id,
        target_header.clone(),
        blocks
            .iter()
            .rev()
            .map(|block| block.queue_diff.zip(&block.out_msgs))
            .collect(),
    )
    .write(None)?;

    persistent_states
        .inner
        .descriptor_cache
        .cache_queue_state(target_mc_seqno, &target_block_id)?;

    // Check storage state
    let expected_set = FastHashSet::from_iter([target_block_id]);
    {
        let index = persistent_states
            .inner
            .descriptor_cache
            .mc_seqno_to_block_ids()
            .lock();
        assert!(!index.contains_key(&0));
        assert_eq!(index.get(&target_mc_seqno), Some(&expected_set));
    }

    let decompressed = {
        let cached = persistent_states
            .inner
            .descriptor_cache
            .get(&CacheKey::from((
                target_block_id,
                PersistentStateKind::Queue,
            )))
            .unwrap()
            .clone();
        assert_eq!(cached.mc_seqno, target_mc_seqno);

        let written = tokio::fs::read(
            states_dir
                .file(PersistentStateKind::Queue.make_file_name(&target_block_id, None))
                .path(),
        )
        .await?;
        assert_eq!(written, cached.file.as_slice());

        let compressed_size = written.len() as u64;

        let written = zstd_decompress_simple(&cached.file)?;

        let decompressed_size = written.len() as u64;

        assert!(compressed_size <= decompressed_size);

        tracing::info!(
            compressed_size = %ByteSize(compressed_size),
            decompressed_size = %ByteSize(decompressed_size),
        );

        written
    };

    let tail_len = blocks
        .iter()
        .rev()
        .map(|block| block.queue_diff.as_ref().clone())
        .len() as u32;

    // Read queue queue state from file
    let top_update = OutMsgQueueUpdates {
        diff_hash: *blocks.last().unwrap().queue_diff.diff_hash(),
        tail_len,
    };
    let mut reader = QueueStateReader::begin_from_mapped(&decompressed, &top_update)?;
    assert_eq!(reader.state().header, target_header);
    assert!(reader.state().messages.len() > 1);

    for (i, chunk) in reader.state().messages.iter().enumerate() {
        tracing::info!(i, chunk_size = %ByteSize(chunk.len() as u64));
    }

    let mut read_messages = FastHashSet::default();

    let mut next_diff_index = 0;
    while let Some(mut part) = reader.read_next_queue_diff()? {
        next_diff_index += 1;
        assert_eq!(part.queue_diff().router_partitions_dst, dst_router);
        assert_eq!(part.queue_diff().router_partitions_src, src_router);
        while let Some(cell) = part.read_next_message()? {
            let exists = read_messages.insert(*cell.repr_hash());
            assert!(exists, "duplicate message");

            let msg = cell.parse::<Message<'_>>()?;

            matches!(msg.info, MsgInfo::Int(_));

            assert!(msg.init.is_none());
            assert!(msg.body.is_empty());
        }
        assert_eq!(read_messages.len(), next_diff_index * 5000);
    }
    assert_eq!(read_messages.len(), target_message_count);

    reader.finish()?;

    Ok(())
}

#[tokio::test]
async fn test_preload_persistent_states() -> Result<()> {
    tycho_util::test::init_logger("test_preload_persistent_states", "debug");

    let tests_data_path = open_tests_data_path();

    let without_parts_src_dir = tests_data_path.join("persistent_states/38-without-parts");
    let with_parts_src_dir = tests_data_path.join("persistent_states/40-with-parts");

    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
    let storage_dir = ctx.files_dir().create_subdir("states")?;

    let mut config = CoreStorageConfig::new_potato();
    config.state_parts = Some(StatePartsConfig {
        split_depth: 2,
        ..Default::default()
    });

    let storage = CoreStorage::open(ctx, config).await?;
    let persistent_states = storage.persistent_state_storage();

    copy_dir_contents(&without_parts_src_dir, &storage_dir.path().join("38"))?;
    copy_dir_contents(&with_parts_src_dir, &storage_dir.path().join("40"))?;

    persistent_states.preload_states().await?;

    let mc38_block_ids = collect_block_ids(&without_parts_src_dir)?;
    tracing::debug!(?mc38_block_ids);

    let mc40_block_ids = collect_block_ids(&with_parts_src_dir)?;
    tracing::debug!(?mc40_block_ids);

    for block_id in &mc38_block_ids {
        assert!(persistent_states.state_exists(block_id, PersistentStateKind::Shard));

        if !block_id.is_masterchain() {
            let info_without_parts = persistent_states
                .get_state_info(block_id, PersistentStateKind::Shard)
                .expect("state without parts should be preloaded");
            tracing::debug!(?info_without_parts);
            assert!(info_without_parts.parts.is_empty());
        }
    }

    for block_id in &mc40_block_ids {
        assert!(persistent_states.state_exists(block_id, PersistentStateKind::Shard));

        if !block_id.is_masterchain() {
            let info_with_parts = persistent_states
                .get_state_info(block_id, PersistentStateKind::Shard)
                .expect("state with parts should be preloaded");
            tracing::debug!(?info_with_parts);

            let part_prefixes: FastHashSet<_> = info_with_parts
                .parts
                .iter()
                .map(|part| part.prefix)
                .collect();
            assert_eq!(part_prefixes.len(), 3);

            let expected_prefixes: FastHashSet<_> =
                split_shard_ident(0, 2).iter().map(|s| s.prefix()).collect();

            for prefix in part_prefixes {
                assert!(expected_prefixes.contains(&prefix));
            }
        }
    }

    let block_ids_index = persistent_states
        .inner
        .descriptor_cache
        .mc_seqno_to_block_ids()
        .lock();
    assert_eq!(block_ids_index.get(&38), Some(&mc38_block_ids));
    assert_eq!(block_ids_index.get(&40), Some(&mc40_block_ids));

    Ok(())
}

fn open_tests_data_path() -> PathBuf {
    let root_path = env!("CARGO_MANIFEST_DIR");
    std::path::Path::new(root_path).join("tests/data")
}

fn copy_dir_contents(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            continue;
        }
        fs::copy(entry.path(), dst.join(entry.file_name()))?;
    }
    Ok(())
}

fn collect_block_ids(dir: &Path) -> Result<FastHashSet<BlockId>> {
    let mut res = FastHashSet::default();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some(ShardStateWriter::FILE_EXTENSION) {
            continue;
        }
        if let Some((block_id, kind, _)) =
            PersistentStateStorage::parse_persistent_state_file_name(&path)
            && kind == PersistentStateKind::Shard
        {
            res.insert(block_id);
        }
    }
    Ok(res)
}

#[tokio::test]
async fn test_store_shard_state_from_file() -> Result<()> {
    tycho_util::test::init_logger("test_preload_persistent_states", "debug");

    let tests_data_path = open_tests_data_path();

    let without_parts_src_dir = tests_data_path.join("persistent_states/38-without-parts");
    let with_parts_src_dir = tests_data_path.join("persistent_states/40-with-parts");

    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;
    let storage_dir = ctx.files_dir().create_subdir("states")?;

    let mut config = CoreStorageConfig::new_potato();
    config.state_parts = Some(StatePartsConfig {
        split_depth: 2,
        ..Default::default()
    });

    let storage = CoreStorage::open(ctx, config).await?;

    let mc_seqno = 38;
    let (_tmp_dir_without_parts, mc38_downloaded_persistent_states) =
        decompress_persistent_states(&without_parts_src_dir)?;
    store_and_check_persistent_states(&storage, mc_seqno, mc38_downloaded_persistent_states)
        .await?;
    check_persistent_state_files_stored(&without_parts_src_dir, &storage_dir, mc_seqno)?;

    let mc_seqno = 40;
    let (_tmp_dir_with_parts, mc40_downloaded_persistent_states) =
        decompress_persistent_states(&with_parts_src_dir)?;
    store_and_check_persistent_states(&storage, mc_seqno, mc40_downloaded_persistent_states)
        .await?;
    check_persistent_state_files_stored(&with_parts_src_dir, &storage_dir, mc_seqno)?;

    Ok(())
}

#[allow(clippy::type_complexity)]
async fn store_and_check_persistent_states(
    storage: &CoreStorage,
    mc_seqno: u32,
    downloaded_persistent_states: Vec<(BlockId, FileBuilder, u8, Vec<(ShardPrefix, FileBuilder)>)>,
) -> Result<()> {
    let persistent_states = storage.persistent_state_storage();
    for (block_id, main_file_builder, part_split_depth, part_files_builders) in
        downloaded_persistent_states
    {
        // check downloaded
        let parts_prefixes: FastHashSet<_> = part_files_builders
            .iter()
            .map(|(prefix, _)| *prefix)
            .collect();
        PersistentStateStorage::check_parts_info_matches_split_depth(
            parts_prefixes,
            part_split_depth,
        )?;

        // store main file
        let main_file = main_file_builder.clone().read(true).open()?;
        let StoreStateFromFileResult { parts_info, .. } = storage
            .shard_state_storage()
            .store_state_main_from_file(&block_id, main_file, part_split_depth)
            .await?;

        // make a map of parts info
        let mut parts_info_map: FastHashMap<_, _> = parts_info
            .unwrap_or_default()
            .into_iter()
            .map(|part_info| (part_info.prefix, part_info))
            .collect();

        // match with files
        let mut part_files_builders_with_info = vec![];
        for (prefix, file_builder) in part_files_builders {
            if let Some(part_info) = parts_info_map.remove(&prefix) {
                part_files_builders_with_info.push((part_info, file_builder));
            }
        }

        assert!(
            parts_info_map.is_empty(),
            "not all required parts files exist",
        );

        // open part files
        let mut part_files = vec![];
        for (info, part_file_builder) in &part_files_builders_with_info {
            let part_file = part_file_builder.clone().read(true).open()?;
            part_files.push((*info, part_file));
        }

        // store state parts and block
        storage
            .shard_state_storage()
            .store_state_parts_from_files(&block_id, part_files)
            .await?;

        let state = storage
            .shard_state_storage()
            .load_state(mc_seqno, &block_id)
            .await?;
        let (block_handle, _) =
            storage
                .block_handle_storage()
                .create_or_load_handle(&block_id, NewBlockMeta {
                    is_key_block: block_id.is_masterchain(),
                    gen_utime: state.as_ref().gen_utime,
                    ref_by_mc_seqno: mc_seqno,
                });

        // reopen persistent state files
        let main_file = main_file_builder.clone().read(true).open()?;
        let mut part_files = vec![];
        for (info, part_file_builder) in &part_files_builders_with_info {
            let part_file = part_file_builder.clone().read(true).open()?;
            part_files.push((*info, part_file));
        }

        // store persistent state
        persistent_states
            .store_shard_state_file(
                mc_seqno,
                &block_handle,
                main_file,
                part_files,
                part_split_depth,
            )
            .await?;

        assert!(persistent_states.state_exists(&block_id, PersistentStateKind::Shard));

        let persistent_state_info = persistent_states
            .get_state_info(&block_id, PersistentStateKind::Shard)
            .expect("persistent state should be saved");
        tracing::debug!(?persistent_state_info);
        assert_eq!(
            persistent_state_info.parts.is_empty(),
            part_files_builders_with_info.is_empty()
        );
    }

    Ok(())
}

fn check_persistent_state_files_stored(
    src_dir_path: &Path,
    storage_dir: &Dir,
    mc_seqno: u32,
) -> Result<()> {
    let src_dir = tycho_storage::fs::Dir::new(src_dir_path)?;
    let dst_dir = storage_dir.create_subdir(mc_seqno.to_string())?;

    for entry in src_dir.entries()?.flatten() {
        // get file name
        let path = entry.path();
        let Some((_, kind, _)) = PersistentStateStorage::parse_persistent_state_file_name(&path)
        else {
            continue;
        };

        if kind != PersistentStateKind::Shard {
            continue;
        }

        let file_name = entry.file_name();

        // check file exists in destination
        tracing::debug!(
            file_name = ?file_name,
            dst_dir_path = ?dst_dir.path(),
            "check persistent state file exists in destination",
        );
        assert!(dst_dir.file(file_name).exists());
    }

    Ok(())
}

/// Decompress persistent shard state files into a fresh temporary directory,
/// returning file builders that can be used as downloaded persistent states.
#[allow(clippy::type_complexity)]
fn decompress_persistent_states(
    src_dir: &Path,
) -> Result<(
    TempDir,
    Vec<(BlockId, FileBuilder, u8, Vec<(ShardPrefix, FileBuilder)>)>,
)> {
    let temp_dir = tempfile::tempdir()?;
    let download_dir = Dir::new(temp_dir.path())?;

    let dir = tycho_storage::fs::Dir::new(src_dir)?;

    let mut decompressed_states = vec![];

    // first find main files
    let mut main_files_builders = vec![];
    for entry in dir.entries()?.flatten() {
        // parse file name
        let path = entry.path();
        let Some((block_id, kind, shard_prefix)) =
            PersistentStateStorage::parse_persistent_state_file_name(&path)
        else {
            continue;
        };

        // open mapped compressed file
        let file_name = entry.file_name();
        let mut file_builder = dir.file(&file_name);
        let file = file_builder.read(true).open()?;
        let mapped_file = MappedFile::from_existing_file(file)?;

        // decompress file
        let dst_file_builder = download_dir.file(file_name);
        let dst_file = dst_file_builder
            .clone()
            .write(true)
            .create(true)
            .truncate(true)
            .open()?;
        mapped_file.decompress_to_file(&dst_file)?;

        if kind != PersistentStateKind::Shard || shard_prefix.is_some() {
            continue;
        }

        // remember main file
        main_files_builders.push((block_id, dst_file_builder));
    }

    for (block_id, main_file_builder) in main_files_builders {
        let part_split_depth =
            PersistentStateStorage::read_persistent_metadata(&block_id, &main_file_builder)?
                .map(|m| m.part_split_depth)
                .unwrap_or_default();
        let part_files_builders = PersistentStateStorage::read_persistent_shard_part_files(
            &block_id,
            &main_file_builder,
        )?;
        decompressed_states.push((
            block_id,
            main_file_builder,
            part_split_depth,
            part_files_builders,
        ));
    }

    Ok((temp_dir, decompressed_states))
}
