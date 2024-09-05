use anyhow::Result;
use bytesize::ByteSize;
use everscale_types::models::{BlockId, ShardStateUnsplit};
use everscale_types::prelude::*;
use futures_util::future;
use futures_util::future::BoxFuture;
use tycho_block_util::archive::Archive;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_core::block_strider::{
    BlockProvider, BlockProviderExt, BlockStrider, OptionalBlockStuff, PersistentBlockStriderState,
    ProofChecker, ShardStateApplier, StateSubscriber, StateSubscriberContext,
};
use tycho_storage::{ArchivesGcConfig, NewBlockMeta, Storage, StorageConfig};
use tycho_util::compression::zstd_decompress;
use tycho_util::project_root;

mod utils;

#[derive(Default, Debug, Clone, Copy)]
struct DummySubscriber;

impl StateSubscriber for DummySubscriber {
    type HandleStateFut<'a> = future::Ready<Result<()>>;

    fn handle_state(&self, _cx: &StateSubscriberContext) -> Self::HandleStateFut<'_> {
        future::ready(Ok(()))
    }
}

pub struct ArchiveProvider {
    archive: Archive,
    proof_checker: ProofChecker,
}

impl ArchiveProvider {
    async fn get_block_impl(&self, block_id: &BlockId) -> OptionalBlockStuff {
        let (block, proof, diff) = match self.archive.get_entry_by_id(block_id) {
            Ok(entry) => entry,
            Err(e) => return Some(Err(e.into())),
        };

        match self
            .proof_checker
            .check_proof(&block, &proof, &diff, true)
            .await
        {
            Ok(_) => Some(Ok(block.clone())),
            Err(e) => Some(Err(e)),
        }
    }
}

impl BlockProvider for ArchiveProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        let id = match self.archive.mc_block_ids.get(&(prev_block_id.seqno + 1)) {
            Some(id) => id,
            None => return Box::pin(futures_util::future::ready(None)),
        };

        self.get_block(id)
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(self.get_block_impl(block_id))
    }
}

async fn prepare_storage(config: StorageConfig, zerostate: ShardStateStuff) -> Result<Storage> {
    let storage = Storage::builder().with_config(config).build().await?;

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

    Ok(storage)
}

#[tokio::test]
async fn archives() -> Result<()> {
    tycho_util::test::init_logger("archives", "debug");

    // Prepare directory
    let tmp_dir = tempfile::tempdir()?;

    // Init storage
    let config = StorageConfig {
        root_dir: tmp_dir.path().to_owned(),
        rocksdb_lru_capacity: ByteSize::kb(1024),
        cells_cache_size: ByteSize::kb(1024),
        rocksdb_enable_metrics: false,
        archives_gc: Some(ArchivesGcConfig::default()),
        states_gc: None,
        blocks_gc: None,
    };

    let zerostate_data = utils::read_file("zerostate.boc")?;
    let zerostate = utils::parse_zerostate(&zerostate_data)?;
    let zerostate_id = *zerostate.block_id();
    let storage = prepare_storage(config, zerostate).await?;

    // Init state applier
    let state_tracker = MinRefMcStateTracker::new();
    let state_subscriber = DummySubscriber;

    let state_applier = ShardStateApplier::new(state_tracker, storage.clone(), state_subscriber);

    // Archive provider
    let archive_data = utils::read_file("archive.bin")?;
    let archive = utils::parse_archive(&archive_data)?;

    let archive_provider = ArchiveProvider {
        archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Next archive provider
    let next_archive_data = utils::read_file("next_archive.bin")?;
    let next_archive = utils::parse_archive(&next_archive_data)?;

    let next_archive_provider = ArchiveProvider {
        archive: next_archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Last archive provider
    let last_archive_data = utils::read_file("last_archive.bin")?;
    let last_archive = utils::parse_archive(&last_archive_data)?;

    let last_archive_provider = ArchiveProvider {
        archive: last_archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Strider state
    let strider_state = PersistentBlockStriderState::new(zerostate_id, storage.clone());

    // Init block strider
    let block_strider = BlockStrider::builder()
        .with_provider(
            archive_provider
                .chain(next_archive_provider)
                .chain(last_archive_provider),
        )
        .with_state(strider_state)
        .with_block_subscriber(state_applier)
        .build();

    block_strider.run().await?;

    let archive_id = storage.block_storage().get_archive_id(1).unwrap();

    // Check archive size
    let archive_size = storage
        .block_storage()
        .get_archive_size(archive_id)?
        .unwrap();
    assert_eq!(archive_size, archive_data.len());

    // Check archive data
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

    // Prepare directory
    let project_root = project_root()?.join(".scratch");
    let integration_test_path = project_root.join("integration_tests");
    let current_test_path = integration_test_path.join("heavy_archives");
    std::fs::remove_dir_all(&current_test_path).ok();
    std::fs::create_dir_all(&current_test_path)?;

    // Init storage
    let config = StorageConfig {
        root_dir: current_test_path.join("db"),
        rocksdb_lru_capacity: ByteSize::kb(1024 * 1024),
        cells_cache_size: ByteSize::kb(1024 * 1024),
        rocksdb_enable_metrics: false,
        archives_gc: Some(ArchivesGcConfig::default()),
        states_gc: None,
        blocks_gc: None,
    };

    let zerostate_path = integration_test_path.join("zerostate.boc");
    let zerostate_data = std::fs::read(zerostate_path)?;
    let zerostate = utils::parse_zerostate(&zerostate_data)?;
    let zerostate_id = *zerostate.block_id();
    let storage = prepare_storage(config, zerostate).await?;

    // Init state applier
    let state_tracker = MinRefMcStateTracker::new();
    let state_subscriber = DummySubscriber;

    let state_applier = ShardStateApplier::new(state_tracker, storage.clone(), state_subscriber);

    // Archive provider
    let archive_path = integration_test_path.join("archive.bin");
    let archive_data = std::fs::read(archive_path)?;
    let archive = utils::parse_archive(&archive_data)?;

    let archive_provider = ArchiveProvider {
        archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Next archive provider
    let next_archive_path = integration_test_path.join("next_archive.bin");
    let next_archive_data = std::fs::read(next_archive_path)?;
    let next_archive = utils::parse_archive(&next_archive_data)?;

    let next_archive_provider = ArchiveProvider {
        archive: next_archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Last archive provider
    let last_archive_path = integration_test_path.join("last_archive.bin");
    let last_archive_data = std::fs::read(last_archive_path)?;
    let last_archive = utils::parse_archive(&last_archive_data)?;

    let last_archive_provider = ArchiveProvider {
        archive: last_archive,
        proof_checker: ProofChecker::new(storage.clone()),
    };

    // Strider state
    let strider_state = PersistentBlockStriderState::new(zerostate_id, storage.clone());

    // Init block strider
    let block_strider = BlockStrider::builder()
        .with_provider(
            archive_provider
                .chain(next_archive_provider)
                .chain(last_archive_provider),
        )
        .with_state(strider_state)
        .with_block_subscriber(state_applier)
        .build();

    block_strider.run().await?;
    storage.block_storage().wait_for_archive_commit().await?;

    // Check archive data
    let archive_chunk_size = storage.block_storage().archive_chunk_size().get() as usize;

    check_archive(&storage, &archive_data, archive_chunk_size, 1).await?;
    check_archive(&storage, &next_archive_data, archive_chunk_size, 101).await?;

    Ok(())
}

async fn check_archive(
    storage: &Storage,
    original_archive: &[u8],
    archive_chunk_size: usize,
    seqno: u32,
) -> Result<()> {
    tracing::info!("Checking archive {}", seqno);
    let archive_id = storage.block_storage().get_archive_id(seqno).unwrap();

    // Check archive size
    let archive_size = storage
        .block_storage()
        .get_archive_size(archive_id)?
        .unwrap();

    let mut got_archive = vec![];
    for offset in (0..archive_size).step_by(archive_chunk_size) {
        let chunk = storage
            .block_storage()
            .get_archive_chunk(archive_id, offset as u64)
            .await?;
        got_archive.extend(chunk);
    }

    let original_decompressed = decompress(original_archive);
    let got_decompressed = decompress(&got_archive);

    let original_len = original_decompressed.len();
    let got_len = got_decompressed.len();

    assert_eq!(archive_size, original_archive.len(), "Size mismatch");
    assert_eq!(got_archive.len(), archive_size, "Retrieved size mismatch");
    assert_eq!(original_archive, &got_archive, "Content mismatch");
    assert_eq!(
        original_decompressed, got_decompressed,
        "Decompressed mismatch"
    );
    assert_eq!(original_len, got_len, "Decompressed size mismatch");

    Ok(())
}

fn decompress(data: &[u8]) -> Vec<u8> {
    let mut decompressed = Vec::new();
    zstd_decompress(data, &mut decompressed).unwrap();
    decompressed
}
