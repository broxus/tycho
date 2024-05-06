use std::sync::Arc;

use anyhow::{Context, Result};

pub use self::config::*;
pub use self::db::*;
pub use self::models::*;
pub use self::store::*;

mod config;
mod db;
mod models;
mod store;

mod util {
    pub use stored_value::*;

    mod stored_value;
}

const DB_SUBDIR: &str = "rocksdb";
const FILES_SUBDIR: &str = "files";

#[derive(Clone)]
#[repr(transparent)]
pub struct Storage {
    inner: Arc<Inner>,
}

impl Storage {
    pub fn new(config: StorageConfig) -> Result<Self> {
        let root = FileDb::new(&config.root_dir)?;

        let files_db = root.create_subdir(FILES_SUBDIR)?;
        let kv_db = Db::open(config.root_dir.join(DB_SUBDIR), config.db_config)
            .context("failed to open a rocksdb")?;

        let block_handle_storage = Arc::new(BlockHandleStorage::new(kv_db.clone()));
        let block_connection_storage = Arc::new(BlockConnectionStorage::new(kv_db.clone()));
        let runtime_storage = Arc::new(RuntimeStorage::new(block_handle_storage.clone()));
        let block_storage = Arc::new(BlockStorage::new(
            kv_db.clone(),
            block_handle_storage.clone(),
            block_connection_storage.clone(),
        )?);
        let shard_state_storage = ShardStateStorage::new(
            kv_db.clone(),
            &files_db,
            block_handle_storage.clone(),
            block_storage.clone(),
            config.cells_cache_size.as_u64(),
        )?;
        let persistent_state_storage =
            PersistentStateStorage::new(kv_db.clone(), &files_db, block_handle_storage.clone())?;
        let node_state_storage = NodeStateStorage::new(kv_db);

        Ok(Self {
            inner: Arc::new(Inner {
                root,
                block_handle_storage,
                block_storage,
                shard_state_storage,
                persistent_state_storage,
                block_connection_storage,
                node_state_storage,
                runtime_storage,
            }),
        })
    }

    /// Creates a new temporary storage with potato config.
    ///
    /// NOTE: Temp dir must live longer than the storage,
    /// otherwise compaction filter will not work.
    #[cfg(any(test, feature = "test"))]
    pub fn new_temp() -> Result<(Self, tempfile::TempDir)> {
        let tmp_dir = tempfile::tempdir()?;
        let storage = Storage::new(StorageConfig::new_potato(tmp_dir.path()))?;
        Ok((storage, tmp_dir))
    }

    pub fn root(&self) -> &FileDb {
        &self.inner.root
    }

    pub fn runtime_storage(&self) -> &RuntimeStorage {
        &self.inner.runtime_storage
    }

    pub fn persistent_state_storage(&self) -> &PersistentStateStorage {
        &self.inner.persistent_state_storage
    }

    pub fn block_handle_storage(&self) -> &BlockHandleStorage {
        &self.inner.block_handle_storage
    }

    pub fn block_storage(&self) -> &BlockStorage {
        &self.inner.block_storage
    }

    pub fn block_connection_storage(&self) -> &BlockConnectionStorage {
        &self.inner.block_connection_storage
    }

    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.inner.shard_state_storage
    }

    pub fn node_state(&self) -> &NodeStateStorage {
        &self.inner.node_state_storage
    }
}

struct Inner {
    root: FileDb,
    runtime_storage: Arc<RuntimeStorage>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: ShardStateStorage,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
}
