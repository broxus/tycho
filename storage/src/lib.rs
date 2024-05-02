use std::path::PathBuf;
use std::sync::Arc;

pub use self::db::*;
pub use self::models::*;
pub use self::store::*;

mod db;
mod models;
mod store;

mod util {
    pub use stored_value::*;

    mod stored_value;
}

#[derive(Clone)]
#[repr(transparent)]
pub struct Storage {
    inner: Arc<Inner>,
}

impl Storage {
    pub fn new(
        db: Arc<Db>,
        file_db_path: PathBuf,
        max_cell_cache_size_bytes: u64,
    ) -> anyhow::Result<Self> {
        let files_dir = FileDb::new(file_db_path)?;

        let block_handle_storage = Arc::new(BlockHandleStorage::new(db.clone()));
        let block_connection_storage = Arc::new(BlockConnectionStorage::new(db.clone()));
        let runtime_storage = Arc::new(RuntimeStorage::new(block_handle_storage.clone()));
        let block_storage = Arc::new(BlockStorage::new(
            db.clone(),
            block_handle_storage.clone(),
            block_connection_storage.clone(),
        )?);
        let shard_state_storage = ShardStateStorage::new(
            db.clone(),
            &files_dir,
            block_handle_storage.clone(),
            block_storage.clone(),
            max_cell_cache_size_bytes,
        )?;
        let persistent_state_storage =
            PersistentStateStorage::new(db.clone(), &files_dir, block_handle_storage.clone())?;
        let node_state_storage = NodeStateStorage::new(db);

        Ok(Self {
            inner: Arc::new(Inner {
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
        use bytesize::ByteSize;

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

        Ok((storage, tmp_dir))
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

    pub fn shard_state_storage(&self) -> &Arc<ShardStateStorage> {
        &self.inner.shard_state_storage
    }

    pub fn node_state(&self) -> &NodeStateStorage {
        &self.inner.node_state_storage
    }
}

struct Inner {
    runtime_storage: Arc<RuntimeStorage>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: Arc<ShardStateStorage>,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
}
