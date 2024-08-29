use anyhow::Result;
use bytesize::ByteSize;
use everscale_types::models::{BlockId, ShardStateUnsplit};
use everscale_types::prelude::*;
use futures_util::future;
use itertools::Itertools;
use tycho_block_util::archive::Archive;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_core::block_strider::{
    BlockSubscriber, BlockSubscriberContext, ShardStateApplier, StateSubscriber,
    StateSubscriberContext,
};
use tycho_storage::{ArchivesGcConfig, NewBlockMeta, Storage, StorageConfig};

mod utils;

#[derive(Default, Debug, Clone, Copy)]
struct DummySubscriber;

impl StateSubscriber for DummySubscriber {
    type HandleStateFut<'a> = future::Ready<Result<()>>;

    fn handle_state(&self, _cx: &StateSubscriberContext) -> Self::HandleStateFut<'_> {
        future::ready(Ok(()))
    }
}

async fn apply_archive<S>(
    archive: Archive,
    storage: &Storage,
    state_applier: &ShardStateApplier<S>,
) -> Result<()>
where
    S: StateSubscriber,
{
    for block_id in archive.mc_block_ids.values() {
        apply_block(block_id, block_id, &archive, storage, state_applier).await?;

        for (id, _) in archive
            .blocks
            .iter()
            .sorted_by_key(|(&block_id, _)| block_id)
        {
            if !id.is_masterchain() {
                let block = archive.get_block_by_id(id)?;
                let block_info = block.block().load_info()?;
                if block_info.min_ref_mc_seqno == block_id.seqno {
                    apply_block(id, block_id, &archive, storage, state_applier).await?;
                }
            }
        }
    }
    Ok(())
}

async fn apply_block<S>(
    block_id: &BlockId,
    mc_block_id: &BlockId,
    archive: &Archive,
    storage: &Storage,
    state_applier: &ShardStateApplier<S>,
) -> Result<()>
where
    S: StateSubscriber,
{
    let (block, proof, diff) = archive.get_entry_by_id(block_id)?;

    let block_info = block.block().load_info()?;

    let cx = BlockSubscriberContext {
        mc_block_id: *mc_block_id,
        is_key_block: block_info.key_block,
        block: block.data,
        archive_data: block.archive_data,
    };

    let prepared = state_applier.prepare_block(&cx).await?;
    state_applier.handle_block(&cx, prepared).await?;

    let handle = storage
        .block_handle_storage()
        .load_handle(block_id)
        .unwrap();

    let handle = storage
        .block_storage()
        .store_block_proof(&proof, handle.into())
        .await?
        .handle;

    storage
        .block_storage()
        .store_queue_diff(&diff, handle.into())
        .await?;

    Ok(())
}

async fn prepare_storage(zerostate: ShardStateStuff) -> Result<(Storage, tempfile::TempDir)> {
    let tmp_dir = tempfile::tempdir()?;
    let storage = Storage::builder()
        .with_config(StorageConfig {
            root_dir: tmp_dir.path().to_owned(),
            rocksdb_lru_capacity: ByteSize::kb(1024),
            cells_cache_size: ByteSize::kb(1024),
            rocksdb_enable_metrics: false,
            archives_gc: Some(ArchivesGcConfig::default()),
            states_gc: None,
            blocks_gc: None,
        })
        .build()?;

    let (handle, _) =
        storage
            .block_handle_storage()
            .create_or_load_handle(zerostate.block_id(), NewBlockMeta {
                is_key_block: zerostate.block_id().is_masterchain(),
                gen_utime: zerostate.state().gen_utime,
                mc_ref_seqno: Some(0),
            });

    storage
        .shard_state_storage()
        .store_state(&handle, &zerostate)
        .await?;

    let tracker = MinRefMcStateTracker::default();

    let global_id = zerostate.state().global_id;
    let gen_utime = zerostate.state().gen_utime;

    for entry in zerostate.shards()?.iter() {
        let (shard_ident, _) = entry?;

        let state = ShardStateUnsplit {
            global_id,
            shard_ident,
            gen_utime,
            min_ref_mc_seqno: u32::MAX,
            ..Default::default()
        };

        let root = CellBuilder::build_from(&state)?;
        let root_hash = *root.repr_hash();
        let file_hash = Boc::file_hash_blake(Boc::encode(&root));

        let block_id = BlockId {
            shard: state.shard_ident,
            seqno: state.seqno,
            root_hash,
            file_hash,
        };

        let state = ShardStateStuff::from_root(&block_id, root, &tracker)?;

        let (handle, _) =
            storage
                .block_handle_storage()
                .create_or_load_handle(state.block_id(), NewBlockMeta {
                    is_key_block: state.block_id().is_masterchain(),
                    gen_utime,
                    mc_ref_seqno: Some(0),
                });

        storage
            .shard_state_storage()
            .store_state(&handle, &state)
            .await?;
    }

    Ok((storage, tmp_dir))
}

#[tokio::test]
async fn archives() -> Result<()> {
    tycho_util::test::init_logger("archives", "debug");

    // Init storage
    let zerostate = utils::get_zerostate("zerostate.boc", false).await?;
    let (storage, _temp_dir) = prepare_storage(zerostate).await?;

    // Init state applier
    let state_tracker = MinRefMcStateTracker::new();
    let state_subscriber = DummySubscriber;

    let state_applier = ShardStateApplier::new(state_tracker, storage.clone(), state_subscriber);

    let (archive, archive_data) = utils::get_archive_with_data("archive.bin", false).await?;
    apply_archive(archive, &storage, &state_applier).await?;

    let (next_archive, _) = utils::get_archive_with_data("next_archive.bin", false).await?;
    apply_archive(next_archive, &storage, &state_applier).await?;

    let (last_archive, _) = utils::get_archive_with_data("last_archive.bin", false).await?;
    apply_archive(last_archive, &storage, &state_applier).await?;

    let archive_id = storage.block_storage().get_archive_id(1).unwrap();

    let archive_size = storage
        .block_storage()
        .get_archive_size(archive_id)?
        .unwrap();

    assert_eq!(archive_size, archive_data.len());

    let archive_chunk_size = storage.block_storage().archive_chunk_size().get() as usize;

    let mut expected_archive_data = vec![];
    for offset in (0..archive_size).step_by(archive_chunk_size) {
        let chunk = storage
            .block_storage()
            .get_archive_chunk(archive_id, offset as u64)
            .await?;
        expected_archive_data.extend(chunk);
    }
    assert_eq!(archive_data, expected_archive_data);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn heavy_archives() -> Result<()> {
    tycho_util::test::init_logger("heavy_archives", "debug");

    // Init storage
    let zerostate = utils::get_zerostate("zerostate.boc", true).await?;
    let (storage, _tmp_dir) = prepare_storage(zerostate).await?;

    // Init state applier
    let state_tracker = MinRefMcStateTracker::new();
    let state_subscriber = DummySubscriber;

    let state_applier = ShardStateApplier::new(state_tracker, storage.clone(), state_subscriber);

    let (archive, archive_data) = utils::get_archive_with_data("archive.bin", true).await?;
    apply_archive(archive, &storage, &state_applier).await?;

    let (next_archive, _) = utils::get_archive_with_data("next_archive.bin", true).await?;
    apply_archive(next_archive, &storage, &state_applier).await?;

    let (last_archive, _) = utils::get_archive_with_data("last_archive.bin", true).await?;
    apply_archive(last_archive, &storage, &state_applier).await?;

    let archive_id = storage.block_storage().get_archive_id(1).unwrap();

    let archive_size = storage
        .block_storage()
        .get_archive_size(archive_id)?
        .unwrap();

    assert_eq!(archive_size, archive_data.len());

    let archive_chunk_size = storage.block_storage().archive_chunk_size().get() as usize;

    let mut expected_archive_data = vec![];
    for offset in (0..archive_size).step_by(archive_chunk_size) {
        let chunk = storage
            .block_storage()
            .get_archive_chunk(archive_id, offset as u64)
            .await?;
        expected_archive_data.extend(chunk);
    }
    assert_eq!(archive_data, expected_archive_data);

    Ok(())
}
