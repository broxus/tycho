use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tycho_util::metrics::spawn_metrics_loop;
use weedb::rocksdb;

pub use self::archive_config::*;
pub use self::config::*;
pub use self::db::*;
pub use self::models::*;
pub use self::store::*;

mod archive_config;
mod config;
mod db;
mod models;
mod store;

mod util {
    pub use self::owned_iterator::*;
    pub use self::slot_subscriptions::*;
    pub use self::stored_value::*;

    pub mod owned_iterator;
    mod slot_subscriptions;
    mod stored_value;
}

pub use store::internal_queue::model::InternalMessageKey;
// TODO move to weedb
pub use util::owned_iterator;

const BASE_DB_SUBDIR: &str = "base";
const RPC_DB_SUBDIR: &str = "rpc";
const FILES_SUBDIR: &str = "files";

pub struct StorageBuilder {
    config: StorageConfig,
    archive_config: Option<ArchiveConfig>,
    init_rpc_storage: bool,
}

impl StorageBuilder {
    pub fn build(self) -> Result<Storage> {
        let root = FileDb::new(&self.config.root_dir)?;

        let file_db = root.create_subdir(FILES_SUBDIR)?;

        let caches = weedb::Caches::with_capacity(self.config.rocksdb_lru_capacity.as_u64() as _);

        let mut threads = std::thread::available_parallelism()?.get();
        let mut fdlimit = match fdlimit::raise_fd_limit() {
            // New fd limit
            Ok(fdlimit::Outcome::LimitRaised { to, .. }) => to,
            // Current soft limit
            _ => {
                rlimit::getrlimit(rlimit::Resource::NOFILE)
                    .unwrap_or((256, 0))
                    .0
            }
        };

        let update_options = |opts: &mut rocksdb::Options, threads: usize, fdlimit: u64| {
            opts.set_paranoid_checks(false);

            // bigger base level size - less compactions
            // parallel compactions finishes faster - less write stalls

            opts.set_max_subcompactions(threads as u32 / 2);

            // io
            opts.set_max_open_files(fdlimit as i32);

            // logging
            opts.set_log_level(rocksdb::LogLevel::Info);
            opts.set_keep_log_file_num(2);
            opts.set_recycle_log_file_num(2);

            // cf
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);

            // cpu
            opts.set_max_background_jobs(std::cmp::max((threads as i32) / 2, 2));
            opts.increase_parallelism(threads as i32);

            opts.set_allow_concurrent_memtable_write(false);
            opts.set_enable_write_thread_adaptive_yield(true);

            // debug
            // NOTE: could slower everything a bit in some cloud environments.
            //       See: https://github.com/facebook/rocksdb/issues/3889
            //
            // opts.enable_statistics();
            // opts.set_stats_dump_period_sec(600);
        };

        let rpc_db = if self.init_rpc_storage {
            // Half the resources for the RPC storage
            // TODO: Is it ok to use exactly half?
            threads = std::cmp::max(2, threads / 2);
            fdlimit = std::cmp::max(256, fdlimit / 2);

            tracing::debug!(threads, fdlimit, subdir = RPC_DB_SUBDIR);
            RpcDb::builder_prepared(self.config.root_dir.join(RPC_DB_SUBDIR), caches.clone())
                .with_metrics_enabled(self.config.rocksdb_enable_metrics)
                .with_options(|opts, _| update_options(opts, threads, fdlimit))
                .build()
                .map(Some)?
        } else {
            None
        };

        tracing::debug!(threads, fdlimit, subdir = BASE_DB_SUBDIR, "opening RocksDB");
        let base_db = BaseDb::builder_prepared(self.config.root_dir.join(BASE_DB_SUBDIR), caches)
            .with_metrics_enabled(self.config.rocksdb_enable_metrics)
            .with_options(|opts, _| update_options(opts, threads, fdlimit))
            .build()?;

        let block_handle_storage = Arc::new(BlockHandleStorage::new(base_db.clone()));
        let block_connection_storage = Arc::new(BlockConnectionStorage::new(base_db.clone()));
        let runtime_storage = Arc::new(RuntimeStorage::new(block_handle_storage.clone()));
        let block_storage = Arc::new(BlockStorage::new(
            base_db.clone(),
            block_handle_storage.clone(),
            block_connection_storage.clone(),
        ));
        let shard_state_storage = ShardStateStorage::new(
            base_db.clone(),
            &file_db,
            block_handle_storage.clone(),
            block_storage.clone(),
            self.config.cells_cache_size.as_u64(),
        )?;
        let persistent_state_storage =
            PersistentStateStorage::new(base_db.clone(), &file_db, block_handle_storage.clone())?;
        let node_state_storage = NodeStateStorage::new(base_db.clone());

        let rpc_state = rpc_db.map(RpcStorage::new);

        let internal_queue_storage = InternalQueueStorage::new(base_db.clone());

        // TODO: preload archive ids

        let inner = Arc::new(Inner {
            root,
            base_db,
            block_handle_storage,
            block_storage,
            shard_state_storage,
            persistent_state_storage,
            block_connection_storage,
            node_state_storage,
            runtime_storage,
            rpc_state,
            internal_queue_storage,
            archive_config: self.archive_config,
        });

        spawn_metrics_loop(&inner, Duration::from_secs(5), |this| async move {
            this.base_db.refresh_metrics();
            if let Some(rpc_state) = this.rpc_state.as_ref() {
                rpc_state.db().refresh_metrics();
            }
        });

        Ok(Storage { inner })
    }

    pub fn with_config(mut self, config: StorageConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_archive_config(mut self, achive_config: Option<ArchiveConfig>) -> Self {
        self.archive_config = achive_config;
        self
    }

    pub fn with_rpc_storage(mut self, init_rpc_storage: bool) -> Self {
        self.init_rpc_storage = init_rpc_storage;
        self
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct Storage {
    inner: Arc<Inner>,
}

impl Storage {
    pub fn builder() -> StorageBuilder {
        StorageBuilder {
            config: StorageConfig::default(),
            archive_config: None,
            init_rpc_storage: false,
        }
    }

    /// Creates a new temporary storage with potato config.
    ///
    /// NOTE: Temp dir must live longer than the storage,
    /// otherwise compaction filter will not work.
    #[cfg(any(test, feature = "test"))]
    pub fn new_temp() -> Result<(Self, tempfile::TempDir)> {
        let tmp_dir = tempfile::tempdir()?;
        let storage = Storage::builder()
            .with_config(StorageConfig::new_potato(tmp_dir.path()))
            .build()?;
        Ok((storage, tmp_dir))
    }

    pub fn root(&self) -> &FileDb {
        &self.inner.root
    }

    pub fn base_db(&self) -> &BaseDb {
        &self.inner.base_db
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

    pub fn rpc_storage(&self) -> Option<&RpcStorage> {
        self.inner.rpc_state.as_ref()
    }

    pub fn archive_config(&self) -> Option<&ArchiveConfig> {
        self.inner.archive_config.as_ref()
    }

    pub fn internal_queue_storage(&self) -> &InternalQueueStorage {
        &self.inner.internal_queue_storage
    }
}

struct Inner {
    root: FileDb,
    base_db: BaseDb,

    runtime_storage: Arc<RuntimeStorage>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: ShardStateStorage,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
    rpc_state: Option<RpcStorage>,
    internal_queue_storage: InternalQueueStorage,

    archive_config: Option<ArchiveConfig>,
}
