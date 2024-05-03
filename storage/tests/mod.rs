use std::str::FromStr;

use anyhow::Result;
use bytesize::ByteSize;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, DynCell};
use everscale_types::models::{BlockId, ShardState};
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::{BlockMetaData, Db, DbOptions, Storage};

#[derive(Clone)]
struct ShardStateCombined {
    cell: Cell,
    state: ShardState,
}

impl ShardStateCombined {
    fn from_file(path: impl AsRef<str>) -> Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        let cell = Boc::decode(&bytes)?;
        let state = cell.parse()?;
        Ok(Self { cell, state })
    }

    fn gen_utime(&self) -> Option<u32> {
        match &self.state {
            ShardState::Unsplit(s) => Some(s.gen_utime),
            ShardState::Split(_) => None,
        }
    }

    fn min_ref_mc_seqno(&self) -> Option<u32> {
        match &self.state {
            ShardState::Unsplit(s) => Some(s.min_ref_mc_seqno),
            ShardState::Split(_) => None,
        }
    }
}

fn compare_cells(orig_cell: &DynCell, stored_cell: &DynCell) {
    assert_eq!(orig_cell.repr_hash(), stored_cell.repr_hash());

    let l = orig_cell.descriptor();
    let r = stored_cell.descriptor();

    assert_eq!(l.d1, r.d1);
    assert_eq!(l.d2, r.d2);
    assert_eq!(orig_cell.data(), stored_cell.data());

    for (orig_cell, stored_cell) in std::iter::zip(orig_cell.references(), stored_cell.references())
    {
        compare_cells(orig_cell, stored_cell);
    }
}

#[tokio::test]
async fn persistent_storage_everscale() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let tmp_dir = tempfile::tempdir()?;
    let root_path = tmp_dir.path();

    // Init rocksdb
    let db_options = DbOptions {
        rocksdb_lru_capacity: ByteSize::kb(1024),
        cells_cache_size: ByteSize::kb(1024),
    };
    let db = Db::open(root_path.join("db_storage"), db_options)?;

    // Init storage
    let storage = Storage::new(
        db,
        root_path.join("file_storage"),
        db_options.cells_cache_size.as_u64(),
    )?;
    assert!(storage.node_state().load_init_mc_block_id().is_none());

    // Read zerostate
    let zero_state_raw = ShardStateCombined::from_file("tests/everscale_zerostate.boc")?;

    // Parse block id
    let block_id = BlockId::from_str("-1:8000000000000000:0:58ffca1a178daff705de54216e5433c9bd2e7d850070d334d38997847ab9e845:d270b87b2952b5ba7daa70aaf0a8c361befcf4d8d2db92f9640d5443070838e4")?;

    // Write zerostate to db
    let (handle, _) = storage.block_handle_storage().create_or_load_handle(
        &block_id,
        BlockMetaData::zero_state(zero_state_raw.gen_utime().unwrap()),
    );

    let zerostate = ShardStateStuff::new(
        block_id,
        zero_state_raw.cell.clone(),
        storage.shard_state_storage().min_ref_mc_state(),
    )?;

    storage
        .shard_state_storage()
        .store_state(&handle, zerostate.clone())
        .await?;

    // Check seqno
    let min_ref_mc_state = storage.shard_state_storage().min_ref_mc_state();
    assert_eq!(min_ref_mc_state.seqno(), zero_state_raw.min_ref_mc_seqno());

    // Load zerostate from db
    let loaded_state = storage
        .shard_state_storage()
        .load_state(zerostate.block_id())
        .await?;

    assert_eq!(zerostate.state(), loaded_state.state());
    assert_eq!(zerostate.block_id(), loaded_state.block_id());
    assert_eq!(zerostate.root_cell(), loaded_state.root_cell());

    compare_cells(
        zerostate.root_cell().as_ref(),
        loaded_state.root_cell().as_ref(),
    );

    // Write persistent state to file
    let persistent_state_keeper = storage.runtime_storage().persistent_state_keeper();
    assert!(persistent_state_keeper.current().is_none());

    storage
        .persistent_state_storage()
        .prepare_persistent_states_dir(&zerostate.block_id())?;

    storage
        .persistent_state_storage()
        .save_state(
            &zerostate.block_id(),
            &zerostate.block_id(),
            zero_state_raw.cell.repr_hash(),
        )
        .await?;

    // Check if state exists
    let exist = storage
        .persistent_state_storage()
        .state_exists(&zerostate.block_id(), &zerostate.block_id());
    assert_eq!(exist, true);

    // Read persistent state
    let offset = 0u64;
    let max_size = 1_000_000u64;

    let persistent_state_storage = storage.persistent_state_storage();
    let persistent_state_data = persistent_state_storage
        .read_state_part(
            &zerostate.block_id(),
            &zerostate.block_id(),
            offset,
            max_size,
        )
        .await
        .unwrap();

    // Check state
    let cell = Boc::decode(&persistent_state_data)?;
    assert_eq!(&cell, zerostate.root_cell());

    // Clear files for test
    tmp_dir.close()?;

    Ok(())
}
