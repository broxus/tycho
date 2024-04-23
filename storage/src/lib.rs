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

pub struct Storage {
    runtime_storage: Arc<RuntimeStorage>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: ShardStateStorage,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
}

impl Storage {
    pub fn new(
        db: Arc<Db>,
        file_db_path: PathBuf,
        max_cell_cache_size_bytes: u64,
    ) -> anyhow::Result<Arc<Self>> {
        let files_dir = FileDb::new(file_db_path);

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

        Ok(Arc::new(Self {
            block_handle_storage,
            block_storage,
            shard_state_storage,
            persistent_state_storage,
            block_connection_storage,
            node_state_storage,
            runtime_storage,
        }))
    }

    #[inline]
    pub fn runtime_storage(&self) -> &RuntimeStorage {
        &self.runtime_storage
    }

    #[inline]
    pub fn persistent_state_storage(&self) -> &PersistentStateStorage {
        &self.persistent_state_storage
    }

    #[inline]
    pub fn block_handle_storage(&self) -> &BlockHandleStorage {
        &self.block_handle_storage
    }

    #[inline]
    pub fn block_storage(&self) -> &BlockStorage {
        &self.block_storage
    }

    #[inline]
    pub fn block_connection_storage(&self) -> &BlockConnectionStorage {
        &self.block_connection_storage
    }

    #[inline]
    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.shard_state_storage
    }

    #[inline]
    pub fn node_state(&self) -> &NodeStateStorage {
        &self.node_state_storage
    }
}
