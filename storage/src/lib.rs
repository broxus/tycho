use std::path::PathBuf;
use std::sync::Arc;

pub use self::db::*;
pub use self::models::*;
pub use self::store::*;

mod db;
mod models;
mod store;
mod utils;

pub struct Storage {
    file_db_path: PathBuf,

    runtime_storage: Arc<RuntimeStorage>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: ShardStateStorage,
    block_connection_storage: BlockConnectionStorage,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
}

impl Storage {
    pub fn new(
        db: Arc<Db>,
        file_db_path: PathBuf,
        max_cell_cache_size_bytes: u64,
    ) -> anyhow::Result<Arc<Self>> {
        let block_handle_storage = Arc::new(BlockHandleStorage::new(db.clone()));
        let runtime_storage = Arc::new(RuntimeStorage::new(block_handle_storage.clone()));
        let block_storage = Arc::new(BlockStorage::new(db.clone(), block_handle_storage.clone())?);
        let shard_state_storage = ShardStateStorage::new(
            db.clone(),
            block_handle_storage.clone(),
            block_storage.clone(),
            file_db_path.clone(),
            max_cell_cache_size_bytes,
        )?;
        let persistent_state_storage = PersistentStateStorage::new(
            file_db_path.clone(),
            db.clone(),
            block_handle_storage.clone(),
        )?;
        let node_state_storage = NodeStateStorage::new(db.clone());
        let block_connection_storage = BlockConnectionStorage::new(db);

        Ok(Arc::new(Self {
            file_db_path,

            block_handle_storage,
            block_storage,
            shard_state_storage,
            persistent_state_storage,
            block_connection_storage,
            node_state_storage,
            runtime_storage,
        }))
    }

    #[inline(always)]
    pub fn runtime_storage(&self) -> &RuntimeStorage {
        &self.runtime_storage
    }

    #[inline(always)]
    pub fn persistent_state_storage(&self) -> &PersistentStateStorage {
        &self.persistent_state_storage
    }

    #[inline(always)]
    pub fn block_handle_storage(&self) -> &BlockHandleStorage {
        &self.block_handle_storage
    }

    #[inline(always)]
    pub fn block_connection_storage(&self) -> &BlockConnectionStorage {
        &self.block_connection_storage
    }

    #[inline(always)]
    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.shard_state_storage
    }

    #[inline(always)]
    pub fn node_state(&self) -> &NodeStateStorage {
        &self.node_state_storage
    }
}
