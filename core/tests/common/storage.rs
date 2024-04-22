use std::sync::Arc;

use anyhow::Result;
use bytesize::ByteSize;
use everscale_types::boc::Boc;
use everscale_types::cell::Cell;
use everscale_types::models::ShardState;
use tempfile::TempDir;
use tycho_storage::{Db, DbOptions, Storage};

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

pub(crate) async fn init_storage() -> Result<(Arc<Storage>, TempDir)> {
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

    Ok((storage, tmp_dir))
}
