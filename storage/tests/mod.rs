use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Result};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytesize::ByteSize;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::{BlockId, ShardIdent, ShardState};
use serde::{Deserialize, Deserializer};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_storage::{BlockMetaData, Db, DbOptions, Storage};

#[derive(Clone)]
struct ShardStateCombined {
    cell: Cell,
    state: ShardState,
}

impl ShardStateCombined {
    fn from_file(path: impl AsRef<str>) -> Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        let cell = Boc::decode(bytes)?;
        let state = cell.parse()?;
        Ok(Self { cell, state })
    }

    fn short_id(&self) -> ShardShortId {
        match &self.state {
            ShardState::Unsplit(s) => ShardShortId::Unsplit {
                seqno: s.seqno,
                shard_ident: s.shard_ident,
            },
            ShardState::Split(s) => {
                let left = s.left.load().unwrap();
                let right = s.right.load().unwrap();
                ShardShortId::Split {
                    left_seqno: left.seqno,
                    left_shard_ident: left.shard_ident,
                    right_seqno: right.seqno,
                    right_shard_ident: right.shard_ident,
                }
            }
        }
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
            ShardState::Split(s) => None,
        }
    }
}

#[derive(Debug)]
enum ShardShortId {
    Unsplit {
        seqno: u32,
        shard_ident: ShardIdent,
    },
    Split {
        left_seqno: u32,
        left_shard_ident: ShardIdent,
        right_seqno: u32,
        right_shard_ident: ShardIdent,
    },
}

impl ShardShortId {
    pub fn shard_ident(&self) -> ShardIdent {
        match self {
            ShardShortId::Unsplit { shard_ident, .. } => *shard_ident,
            ShardShortId::Split {
                left_shard_ident, ..
            } => *left_shard_ident,
        }
    }

    pub fn seqno(&self) -> u32 {
        match self {
            ShardShortId::Unsplit { seqno, .. } => *seqno,
            ShardShortId::Split { left_seqno, .. } => *left_seqno,
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

#[tokio::test]
async fn storage_init() {
    tracing_subscriber::fmt::try_init().ok();
    tracing::info!("connect_new_node_to_bootstrap");

    let root_path = Path::new("tmp");
    let db_options = DbOptions {
        rocksdb_lru_capacity: ByteSize::kb(1024),
        cells_cache_size: ByteSize::kb(1024),
    };
    let db = Db::open(root_path.join("db_storage"), db_options).unwrap();

    let storage = Storage::new(
        db,
        root_path.join("file_storage"),
        db_options.cells_cache_size.as_u64(),
    )
    .unwrap();
    assert!(storage.node_state().load_init_mc_block_id().is_err());

    // Read zerostate
    let zero_state = ShardStateCombined::from_file("tests/everscale_zerostate.boc").unwrap();

    // Read global config
    let global_config = GlobalConfig::from_file("tests/global-config.json").unwrap();

    // Write zerostate to db
    let (handle, _) = storage
        .block_handle_storage()
        .create_or_load_handle(
            &global_config.block_id,
            BlockMetaData::zero_state(zero_state.gen_utime().unwrap()),
        )
        .unwrap();

    let state = ShardStateStuff::new(
        global_config.block_id,
        zero_state.cell.clone(),
        storage.shard_state_storage().min_ref_mc_state(),
    )
    .unwrap();

    storage
        .shard_state_storage()
        .store_state(&handle, &state)
        .await
        .unwrap();

    let min_ref_mc_state = storage.shard_state_storage().min_ref_mc_state();
    assert_eq!(min_ref_mc_state.seqno(), zero_state.min_ref_mc_seqno());

    // Write persistent state
    let persistent_state_keeper = storage.runtime_storage().persistent_state_keeper();
    assert!(persistent_state_keeper.current().is_none());

    storage
        .persistent_state_storage()
        .prepare_persistent_states_dir(&state.block_id())
        .unwrap();

    storage
        .persistent_state_storage()
        .save_state(
            &state.block_id(),
            &state.block_id(),
            zero_state.cell.repr_hash(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;

    //println!("{:?}", zero_state.state);
    //println!("{:?}", global_config);

    //std::fs::remove_dir_all(root_path).unwrap()
}
