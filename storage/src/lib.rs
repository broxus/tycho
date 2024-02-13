use std::path::PathBuf;
use std::sync::Arc;

pub use self::block_connection_storage::*;
pub use self::block_handle_storage::*;
pub use self::models::*;
pub use self::runtime_storage::*;

use self::block_storage::*;
use self::shard_state_storage::*;

mod block_connection_storage;
mod block_handle_storage;
mod block_storage;
mod db;
mod models;
mod node_state_storage;
mod runtime_storage;
mod shard_state_storage;

pub struct Storage {
    file_db_path: PathBuf,

    runtime_storage: Arc<RuntimeStorage>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: ShardStateStorage,
    block_connection_storage: BlockConnectionStorage,
    //node_state_storage: NodeStateStorage,
    //persistent_state_storage: PersistentStateStorage,
}

impl Storage {
    pub fn block_handle_storage(&self) -> &BlockHandleStorage {
        &self.block_handle_storage
    }

    pub fn block_connection_storage(&self) -> &BlockConnectionStorage {
        &self.block_connection_storage
    }

    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.shard_state_storage
    }
}
