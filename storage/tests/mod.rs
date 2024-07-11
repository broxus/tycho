use std::str::FromStr;

use anyhow::Result;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, DynCell};
use everscale_types::models::{BlockId, ShardState};
use tycho_block_util::state::ShardStateStuff;
use tycho_storage::{BlockMetaData, Storage};

#[derive(Clone)]
struct ShardStateCombined {
    cell: Cell,
    state: ShardState,
}

impl ShardStateCombined {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
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
    tycho_util::test::init_logger("persistent_storage_everscale", "debug");

    let (storage, _tmp_dir) = Storage::new_temp()?;
    assert!(storage.node_state().load_init_mc_block_id().is_none());

    // Read zerostate
    let zero_state_raw =
        ShardStateCombined::from_bytes(include_bytes!("../../test/test_state_2_master.boc"))?;

    // Parse block id
    let block_id = BlockId::from_str("-1:8000000000000000:2:4557702252a8fcec88387ab78407e5116e83222b213653911f86e6504cb7aa78:e2bc83d6be6975b9c68f56c5f6d4997d2a33226bfac6a431b47874e3ba18db75")?;

    // Write zerostate to db
    let (handle, _) = storage.block_handle_storage().create_or_load_handle(
        &block_id,
        BlockMetaData::zero_state(zero_state_raw.gen_utime().unwrap(), true),
    );

    let zerostate = ShardStateStuff::from_root(
        &block_id,
        zero_state_raw.cell.clone(),
        storage.shard_state_storage().min_ref_mc_state(),
    )?;

    storage
        .shard_state_storage()
        .store_state(&handle, &zerostate)
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
        .prepare_persistent_states_dir(zerostate.state().seqno)?;

    storage
        .persistent_state_storage()
        .store_state(&handle, zero_state_raw.cell.repr_hash())
        .await?;

    // Check if state exists
    let exist = storage
        .persistent_state_storage()
        .state_exists(zerostate.block_id());
    assert_eq!(exist, true);

    // Read persistent state a couple of times to check if it is stateless
    for _ in 0..2 {
        let limit = bytesize::mb(1u32);
        let offset = 0u64;

        let persistent_state_storage = storage.persistent_state_storage();
        let persistent_state_data = persistent_state_storage
            .read_state_part(zerostate.block_id(), limit as _, offset)
            .await
            .unwrap();

        // Check state
        let cell = Boc::decode(&persistent_state_data)?;
        assert_eq!(&cell, zerostate.root_cell());
    }

    Ok(())
}
