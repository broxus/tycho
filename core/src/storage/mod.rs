use std::sync::{Arc, Once};

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
use self::db::apply_cells_migrations;
pub use self::db::{CellsDb, CoreDb, CoreDbExt};
pub use self::gc::ManualGcTrigger;
pub use self::node_state::{NodeStateStorage, NodeSyncState};
pub use self::persistent_state::{
    BriefBocHeader, PersistentState, PersistentStateInfo, PersistentStateKind, PersistentStateMeta,
    PersistentStateStorage, QueueDiffReader, QueueStateReader, QueueStateWriter, ShardStateReader,
    ShardStateWriter, validate_persistent_state_split_metadata,
};
pub use self::shard_state::{
    BlockInfoForApply, InitiatedStoreState, LoadStateHint, ShardStateStorage,
    ShardStateStorageMetrics, StateNotFound, StoreStateHint, split_shard_accounts,
};

pub mod shard_state;
pub mod tables;

pub(crate) mod block;
mod block_connection;
mod block_handle;
mod config;
mod db;
mod gc;
mod node_state;
mod persistent_state;
mod util;

pub const CORE_DB_SUBDIR: &str = "core";
pub const CELLS_DB_SUBDIR: &str = "cells";
pub const CELL_NURSERY_SUBDIR: &str = "cell-nursery";

#[derive(Clone)]
#[repr(transparent)]
pub struct CoreStorage {
    inner: Arc<Inner>,
}

impl CoreStorage {
    pub fn open_cells_db(ctx: &StorageContext) -> Result<CellsDb> {
        // CRoaring owns its allocator hooks globally. Install the Rust allocator
        // before command parsing can create any roaring bitmaps.
        // SAFETY: this is the first code in `main`, so no CRoaring objects exist yet.
        static CROARING_INIT: Once = Once::new();
        CROARING_INIT.call_once(|| unsafe {
            croaring::configure_rust_alloc();
        });

        ctx.open(CELLS_DB_SUBDIR, |opts| {
            const MIB: u64 = 1024 * 1024;
            const GIB: u64 = 1024 * MIB;

            ctx.apply_default_options(opts);

            // we have our own cache and don't want `kcompactd` goes brrr scenario
            opts.set_use_direct_reads(true);
            opts.set_use_direct_io_for_flush_and_compaction(true);

            opts.set_max_background_jobs(8);
            opts.set_max_subcompactions(4);

            opts.set_bytes_per_sync(MIB);

            // single writer optimizations
            opts.set_enable_write_thread_adaptive_yield(false);
            opts.set_enable_pipelined_write(false);
            opts.set_unordered_write(false);

            opts.set_auto_tuned_ratelimiter(
                GIB as i64, // 1GB/s base rate
                100_000,    // 100ms refill
                10,         // fairness
            );

            opts.set_stats_dump_period_sec(60);
            opts.set_report_bg_io_stats(true);

            opts.set_avoid_unnecessary_blocking_io(true); // schedule unnecessary IO in background;
        })
    }

    pub async fn open(ctx: StorageContext, config: CoreStorageConfig) -> Result<Self> {
        anyhow::ensure!(
            config.persistent_state_split_depth
                <= CoreStorageConfig::MAX_PERSISTENT_STATE_SPLIT_DEPTH,
            "persistent_state_split_depth exceeds maximum: value={}, max={}",
            config.persistent_state_split_depth,
            CoreStorageConfig::MAX_PERSISTENT_STATE_SPLIT_DEPTH
        );

        let db: CoreDb = ctx.open_preconfigured(CORE_DB_SUBDIR)?;
        db.normalize_version()?;
        db.apply_migrations().await?;

        let cells_db = CoreStorage::open_cells_db(&ctx)?;
        let cell_storage_worker_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(config.cell_storage_threads.get())
                .stack_size(8 * 1024 * 1024)
                .thread_name(|_| "cellstor".to_owned())
                .build()?,
        );
        let mut cell_counters = shard_state::db_state::CountersStore::open(
            cells_db.clone(),
            cell_storage_worker_pool.clone(),
        );
        cells_db.normalize_version()?;

        // WARNING: An interrupted raw import poisons local storage.
        cell_counters.check_no_interrupted_raw_import()?;

        let mut cell_counters = apply_cells_migrations(cells_db.clone(), cell_counters).await?;
        cell_counters.load_latest_or_empty()?;

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

        let shard_state_storage = ShardStateStorage::new(shard_state::ShardStateStorageInit {
            cells_db: cells_db.clone(),
            cell_nursery_dir: ctx.root_dir().path().join(CELL_NURSERY_SUBDIR),
            block_handle_storage: block_handle_storage.clone(),
            block_storage: block_storage.clone(),
            block_connections: block_connection_storage.clone(),
            temp_file_storage: ctx.temp_files().clone(),
            cell_counters,
            cell_storage_worker_pool,
            config: &config,
        })?;
        let persistent_state_storage = PersistentStateStorage::new(
            cells_db.clone(),
            ctx.files_dir(),
            node_state_storage.clone(),
            block_handle_storage.clone(),
            block_storage.clone(),
            shard_state_storage.clone(),
            config.persistent_state_split_depth,
        )?;

        persistent_state_storage.preload().await?;

        let gc = gc::CoreStorageGc::new(
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

    pub fn shard_state_storage(&self) -> &Arc<ShardStateStorage> {
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
    gc: gc::CoreStorageGc,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: Arc<ShardStateStorage>,
    node_state_storage: Arc<NodeStateStorage>,
    persistent_state_storage: PersistentStateStorage,
}
