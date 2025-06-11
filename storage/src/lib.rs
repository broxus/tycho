use std::sync::Arc;

use anyhow::Result;
pub use tycho_storage_traits::{StoredValue, StoredValueBuffer};

pub use self::config::*;
pub use self::context::StorageContext;
pub use self::db::*;
pub use self::store::*;

mod config;
mod context;
mod db;
mod store;

pub mod util {
    pub use self::instance_id::*;
    pub use self::slot_subscriptions::*;
    pub use self::stored_value::*;

    pub mod instance_id;
    mod slot_subscriptions;
    mod stored_value;
}

const BASE_DB_SUBDIR: &str = "base";

// TODO: Move into `tycho_core`.
// TODO: Rename to `CoreStorage`.
#[derive(Clone)]
#[repr(transparent)]
pub struct Storage {
    inner: Arc<Inner>,
}

impl Storage {
    pub async fn open(ctx: StorageContext) -> Result<Self> {
        let config = ctx.config().clone();

        let base_db: BaseDb = ctx.open_preconfigured(BASE_DB_SUBDIR)?;
        base_db.normalize_version()?;
        base_db.apply_migrations().await?;

        let blocks_storage_config = BlockStorageConfig {
            archive_chunk_size: config.archive_chunk_size,
            blocks_cache: config.blocks_cache,
            split_block_tasks: config.split_block_tasks,
        };
        let block_handle_storage = Arc::new(BlockHandleStorage::new(base_db.clone()));
        let block_connection_storage = Arc::new(BlockConnectionStorage::new(base_db.clone()));
        let block_storage = Arc::new(BlockStorage::new(
            base_db.clone(),
            blocks_storage_config,
            block_handle_storage.clone(),
            block_connection_storage.clone(),
            config.archive_chunk_size,
        ));
        let shard_state_storage = ShardStateStorage::new(
            base_db.clone(),
            block_handle_storage.clone(),
            block_storage.clone(),
            ctx.temp_files().clone(),
            config.cells_cache_size,
        )?;
        let persistent_state_storage = PersistentStateStorage::new(
            base_db.clone(),
            ctx.files_dir(),
            block_handle_storage.clone(),
            block_storage.clone(),
            shard_state_storage.clone(),
        )?;

        persistent_state_storage.preload().await?;

        let node_state_storage = NodeStateStorage::new(base_db.clone());

        block_storage.finish_block_data().await?;
        block_storage.preload_archive_ids().await?;

        Ok(Storage {
            inner: Arc::new(Inner {
                ctx,
                base_db,
                config,
                block_handle_storage,
                block_storage,
                shard_state_storage,
                persistent_state_storage,
                block_connection_storage,
                node_state_storage,
            }),
        })
    }

    /// Creates a new temporary storage with potato config.
    ///
    /// NOTE: Temp dir must live longer than the storage,
    /// otherwise compaction filter will not work.
    #[cfg(any(test, feature = "test"))]
    pub async fn open_temp() -> Result<(Self, tempfile::TempDir)> {
        let (ctx, tmp_dir) = StorageContext::new_temp().await?;
        let storage = Self::open(ctx).await?;
        Ok((storage, tmp_dir))
    }

    pub fn context(&self) -> &StorageContext {
        &self.inner.ctx
    }

    pub fn root(&self) -> &FileDb {
        self.inner.ctx.root_dir()
    }

    pub fn base_db(&self) -> &BaseDb {
        &self.inner.base_db
    }

    pub fn config(&self) -> &StorageConfig {
        &self.inner.config
    }

    pub fn persistent_state_storage(&self) -> &PersistentStateStorage {
        &self.inner.persistent_state_storage
    }

    // TODO: Remove.
    pub fn temp_file_storage(&self) -> &TempFileStorage {
        self.inner.ctx.temp_files()
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
    ctx: StorageContext,
    base_db: BaseDb,
    config: StorageConfig,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: Arc<ShardStateStorage>,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
}
