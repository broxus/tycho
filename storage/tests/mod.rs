use anyhow::{anyhow, Result};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytesize::ByteSize;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, DynCell, HashBytes};
use everscale_types::models::{BlockId, ShardIdent, ShardState};
use serde::{Deserialize, Deserializer};
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

#[derive(Deserialize)]
struct GlobalConfigJson {
    validator: ValidatorJson,
}

#[derive(Deserialize)]
struct ValidatorJson {
    zero_state: BlockIdJson,
}

#[derive(Debug, Default)]
pub struct BlockIdJson {
    workchain: i32,
    shard: u64,
    seqno: u32,
    root_hash: HashBytes,
    file_hash: HashBytes,
}

impl<'de> Deserialize<'de> for BlockIdJson {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        struct BlockIdJsonHelper {
            workchain: i32,
            shard: i64,
            seqno: u32,
            root_hash: String,
            file_hash: String,
        }

        let BlockIdJsonHelper {
            workchain,
            shard,
            seqno,
            root_hash,
            file_hash,
        } = BlockIdJsonHelper::deserialize(deserializer)?;

        let shard = shard as u64;

        let mut result = Self {
            workchain,
            shard,
            seqno,
            ..Default::default()
        };

        result.root_hash =
            HashBytes::from_slice(&BASE64_STANDARD.decode(root_hash).map_err(Error::custom)?);

        result.file_hash =
            HashBytes::from_slice(&BASE64_STANDARD.decode(file_hash).map_err(Error::custom)?);

        Ok(result)
    }
}

impl TryFrom<BlockIdJson> for BlockId {
    type Error = anyhow::Error;

    fn try_from(value: BlockIdJson) -> Result<Self, Self::Error> {
        Ok(Self {
            shard: ShardIdent::new(value.workchain, value.shard)
                .ok_or(anyhow!("Invalid ShardIdent"))?,
            seqno: value.seqno,
            root_hash: value.root_hash,
            file_hash: value.file_hash,
        })
    }
}

#[derive(Debug)]
struct GlobalConfig {
    block_id: BlockId,
}

impl GlobalConfig {
    pub fn from_file(path: impl AsRef<str>) -> Result<Self> {
        let data = std::fs::read_to_string(path.as_ref())?;
        Ok(serde_json::from_str::<GlobalConfigJson>(&data)?.try_into()?)
    }
}

impl TryFrom<GlobalConfigJson> for GlobalConfig {
    type Error = anyhow::Error;

    fn try_from(value: GlobalConfigJson) -> Result<Self, Self::Error> {
        Ok(Self {
            block_id: value.validator.zero_state.try_into()?,
        })
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
    assert!(storage.node_state().load_init_mc_block_id().is_err());

    // Read zerostate
    let zero_state_raw = ShardStateCombined::from_file("tests/everscale_zerostate.boc")?;

    // Read global config
    let global_config = GlobalConfig::from_file("tests/global-config.json")?;

    // Write zerostate to db
    let (handle, _) = storage.block_handle_storage().create_or_load_handle(
        &global_config.block_id,
        BlockMetaData::zero_state(zero_state_raw.gen_utime().unwrap()),
    )?;

    let zerostate = ShardStateStuff::new(
        global_config.block_id,
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
