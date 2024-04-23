use std::sync::Arc;

use anyhow::Result;
use bytesize::ByteSize;
use tempfile::TempDir;
use tycho_storage::{Db, DbOptions, Storage};

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
