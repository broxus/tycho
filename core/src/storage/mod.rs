use std::sync::Arc;

use anyhow::Result;
use tycho_block_util::block::BlockStuff;
use tycho_storage::StorageContext;
use tycho_storage::kv::ApplyMigrations;

pub use self::block::{
    ArchiveId, BlockGcStats, BlockStorage, BlockStorageConfig, MaybeExistingHandle, OpenStats,
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
pub use self::db::{CellsDb, CoreDb, CoreDbExt, CoreTables};
use self::gc::CoreStorageGc;
pub use self::gc::ManualGcTrigger;
pub use self::node_state::{NodeStateStorage, NodeSyncState};
pub use self::persistent_state::{
    BriefBocHeader, PersistentState, PersistentStateInfo, PersistentStateKind,
    PersistentStateStorage, QueueDiffReader, QueueStateReader, QueueStateWriter, ShardStateReader,
    ShardStateWriter,
};
pub use self::shard_state::{
    ShardStateStorage, ShardStateStorageError, ShardStateStorageMetrics, StoreStateHint,
    split_shard_accounts,
};

pub mod tables;

pub(crate) mod block;
mod block_connection;
mod block_handle;
mod config;
mod db;
mod gc;
mod node_state;
mod persistent_state;
mod shard_state;
mod util;

pub const CORE_DB_SUBDIR: &str = "core";
pub const CELLS_DB_SUBDIR: &str = "cells";

#[derive(Clone)]
#[repr(transparent)]
pub struct CoreStorage {
    inner: Arc<Inner>,
}

impl CoreStorage {
    pub async fn open(ctx: StorageContext, config: CoreStorageConfig) -> Result<Self> {
        let db: CoreDb = ctx.open_preconfigured(CORE_DB_SUBDIR)?;
        db.normalize_version()?;
        db.apply_migrations().await?;

        let cells_db: CellsDb = ctx.open_preconfigured(CELLS_DB_SUBDIR)?;
        cells_db.normalize_version()?;
        cells_db.apply_migrations().await?;

        let node_state_storage = Arc::new(NodeStateStorage::new(db.clone()));

        let blocks_storage_config = BlockStorageConfig {
            blocks_cache: config.blocks_cache,
            blobs_root: ctx.root_dir().path().join("blobs"),
            blob_db_config: config.blob_db.clone(),
        };
        let block_handle_storage = Arc::new(BlockHandleStorage::new(db.clone()));
        let block_connection_storage = Arc::new(BlockConnectionStorage::new(db.clone()));
        let block_storage = BlockStorage::new(
            db.clone(),
            blocks_storage_config,
            block_handle_storage.clone(),
            block_connection_storage.clone(),
        )
        .await?;
        let block_storage = Arc::new(block_storage);
        let shard_state_storage = ShardStateStorage::new(
            cells_db.clone(),
            block_handle_storage.clone(),
            block_storage.clone(),
            ctx.temp_files().clone(),
            config.cells_cache_size,
            config.drop_interval,
            config.store_shard_state_step,
        )?;
        let persistent_state_storage = PersistentStateStorage::new(
            cells_db.clone(),
            ctx.files_dir(),
            node_state_storage.clone(),
            block_handle_storage.clone(),
            block_storage.clone(),
            shard_state_storage.clone(),
        )?;

        persistent_state_storage.preload().await?;

        let gc = CoreStorageGc::new(
            &node_state_storage,
            block_handle_storage.clone(),
            block_storage.clone(),
            shard_state_storage.clone(),
            persistent_state_storage.clone(),
            &config,
        );

        Ok(Self {
            inner: Arc::new(Inner {
                ctx,
                db,
                config,
                gc,
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

    pub fn open_stats(&self) -> &OpenStats {
        self.inner.block_storage.open_stats()
    }

    pub fn trigger_archives_gc(&self, trigger: ManualGcTrigger) {
        self.inner.gc.trigger_archives_gc(trigger);
    }

    pub fn trigger_blocks_gc(&self, trigger: ManualGcTrigger) {
        self.inner.gc.trigger_blocks_gc(trigger);
    }

    pub fn trigger_states_gc(&self, trigger: ManualGcTrigger) {
        self.inner.gc.trigger_states_gc(trigger);
    }

    pub(crate) fn update_gc_state(&self, is_key_block: bool, block: &BlockStuff) {
        self.inner.gc.handle_block(is_key_block, block);
    }
}

struct Inner {
    ctx: StorageContext,
    db: CoreDb,
    config: CoreStorageConfig,
    gc: CoreStorageGc,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: Arc<ShardStateStorage>,
    node_state_storage: Arc<NodeStateStorage>,
    persistent_state_storage: PersistentStateStorage,
}
