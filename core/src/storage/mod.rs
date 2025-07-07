use std::sync::Arc;

use anyhow::Result;
use tycho_storage::StorageContext;
use tycho_storage::kv::ApplyMigrations;

pub use self::block::{
    ArchiveId, BlockGcStats, BlockStorage, BlockStorageConfig, MaybeExistingHandle,
    PackageEntryKey, PartialBlockId, StoreBlockResult,
};
pub use self::block_connection::{BlockConnection, BlockConnectionStorage};
pub use self::block_handle::{
    BlockFlags, BlockHandle, BlockHandleStorage, BlockMeta, HandleCreationStatus,
    KeyBlocksDirection, LoadedBlockMeta, NewBlockMeta, WeakBlockHandle,
};
pub use self::config::{
    ArchivesGcConfig, BlocksCacheConfig, BlocksGcConfig, BlocksGcType, CoreStorageConfig,
    StatesGcConfig,
};
pub use self::db::{CoreDb, CoreDbExt, CoreTables};
pub use self::node_state::{NodeStateStorage, NodeSyncState};
pub use self::persistent_state::{
    BriefBocHeader, PersistentStateInfo, PersistentStateKind, PersistentStateStorage,
    QueueDiffReader, QueueStateReader, QueueStateWriter, ShardStateReader, ShardStateWriter,
};
pub use self::shard_state::{
    ShardStateStorage, ShardStateStorageError, ShardStateStorageMetrics, StoreStateHint,
};

pub mod tables;

mod block;
mod block_connection;
mod block_handle;
mod config;
mod db;
mod node_state;
mod persistent_state;
mod shard_state;

mod util {
    pub use self::slot_subscriptions::*;
    pub use self::stored_value::*;

    mod slot_subscriptions;
    mod stored_value;
}

const BASE_DB_SUBDIR: &str = "base";

#[derive(Clone)]
#[repr(transparent)]
pub struct CoreStorage {
    inner: Arc<Inner>,
}

impl CoreStorage {
    pub async fn open(ctx: StorageContext, config: CoreStorageConfig) -> Result<Self> {
        let db: CoreDb = ctx.open_preconfigured(BASE_DB_SUBDIR)?;
        db.normalize_version()?;
        db.apply_migrations().await?;

        let blocks_storage_config = BlockStorageConfig {
            archive_chunk_size: config.archive_chunk_size,
            blocks_cache: config.blocks_cache,
            blobs_root: ctx.root_dir().path().join("blobs"),
        };
        let block_handle_storage = Arc::new(BlockHandleStorage::new(db.clone()));
        let block_connection_storage = Arc::new(BlockConnectionStorage::new(db.clone()));
        let block_storage = Arc::new(
            BlockStorage::new(
                db.clone(),
                blocks_storage_config,
                block_handle_storage.clone(),
                block_connection_storage.clone(),
                config.archive_chunk_size,
            )
            .await?,
        );
        let shard_state_storage = ShardStateStorage::new(
            db.clone(),
            block_handle_storage.clone(),
            block_storage.clone(),
            ctx.temp_files().clone(),
            config.cells_cache_size,
        )?;
        let persistent_state_storage = PersistentStateStorage::new(
            db.clone(),
            ctx.files_dir(),
            block_handle_storage.clone(),
            block_storage.clone(),
            shard_state_storage.clone(),
        )?;

        persistent_state_storage.preload().await?;

        let node_state_storage = NodeStateStorage::new(db.clone());

        Ok(Self {
            inner: Arc::new(Inner {
                ctx,
                db,
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

    pub fn context(&self) -> &StorageContext {
        &self.inner.ctx
    }

    pub fn db(&self) -> &CoreDb {
        &self.inner.db
    }

    pub fn config(&self) -> &CoreStorageConfig {
        &self.inner.config
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
    ctx: StorageContext,
    db: CoreDb,
    config: CoreStorageConfig,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: Arc<ShardStateStorage>,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
}
