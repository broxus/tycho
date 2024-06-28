use std::ops::Add;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::Notify;
use tycho_util::metrics::{spawn_metrics_loop, HistogramGuard};
use tycho_util::time::now_sec;
use weedb::rocksdb;

pub use self::config::*;
pub use self::db::*;
pub use self::models::*;
pub use self::store::*;

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

use tycho_util::time::duration_between_unix_and_instant;
// TODO move to weedb
pub use util::owned_iterator;

const BASE_DB_SUBDIR: &str = "base";
const RPC_DB_SUBDIR: &str = "rpc";
const FILES_SUBDIR: &str = "files";

pub struct StorageBuilder {
    config: StorageConfig,
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

        let gc_enabled_for_sync = AtomicBool::new(
            self.config
                .blocks_gc_config
                .map(|x| x.enable_for_sync)
                .unwrap_or(false),
        );

        let inner = Arc::new(Inner {
            root,
            base_db,
            config: self.config,
            block_handle_storage,
            block_storage,
            shard_state_storage,
            persistent_state_storage,
            block_connection_storage,
            node_state_storage,
            runtime_storage,
            rpc_state,
            internal_queue_storage,
            blocks_gc_state: BlockGcState {
                enabled_for_sync: gc_enabled_for_sync,
            },
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

    pub fn config(&self) -> &StorageConfig {
        &self.inner.config
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

    pub fn internal_queue_storage(&self) -> &InternalQueueStorage {
        &self.inner.internal_queue_storage
    }

    pub fn gc_enable_for_sync(&self) -> &AtomicBool {
        &self.inner.blocks_gc_state.enabled_for_sync
    }
}

pub async fn prepare_blocks_gc(storage: Storage) -> Result<()> {
    let blocks_gc_config = match &storage.inner.config.blocks_gc_config {
        Some(state) => state,
        None => return Ok(()),
    };

    storage
        .inner
        .blocks_gc_state
        .enabled_for_sync
        .store(true, Ordering::Release);

    let Some(key_block) = storage.block_handle_storage().find_last_key_block() else {
        return Ok(());
    };

    let Some(block) = storage.node_state().load_shards_client_mc_block_id() else {
        return Ok(());
    };
    // Blocks GC will be called later when the shards client will reach the key block
    if block.seqno < key_block.id().seqno {
        return Ok(());
    }

    storage
        .block_storage()
        .remove_outdated_blocks(
            key_block.id(),
            blocks_gc_config.max_blocks_per_batch,
            blocks_gc_config.kind,
        )
        .await
}

pub fn start_states_gc(storage: Storage) {
    let options = match storage.inner.config.states_gc_options {
        Some(options) => options,
        None => return,
    };

    // Compute gc timestamp aligned to `interval_sec` with an offset `offset_sec`
    let mut gc_at = now_sec() as u64;
    gc_at = (gc_at - gc_at % options.interval_sec) + options.offset_sec;

    tokio::spawn(async move {
        'gc: loop {
            // Shift gc timestamp one iteration further
            gc_at += options.interval_sec;

            // Check if there is some time left before the GC
            if let Some(interval) = gc_at.checked_sub(now_sec() as u64) {
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }

            let start = Instant::now();
            let mut shards_gc_lock = storage
                .runtime_storage()
                .persistent_state_keeper()
                .shards_gc_lock()
                .subscribe();
            metrics::histogram!("tycho_storage_shard_states_gc_lock_time").record(start.elapsed());

            let block_id = loop {
                // Load the latest block id
                let block_id = match storage.node_state().load_shards_client_mc_block_id() {
                    Some(block_id) => block_id,
                    None => {
                        tracing::error!(target: "storage", "Failed to load last shards client block. Block not found");
                        continue 'gc;
                    }
                };

                if *shards_gc_lock.borrow_and_update() {
                    if shards_gc_lock.changed().await.is_err() {
                        tracing::warn!(target: "storage", "Stopping shard states GC");
                        return;
                    }
                    continue;
                }

                break block_id;
            };

            // subscriber.on_before_states_gc(&block_id).await;

            let _histogram = HistogramGuard::begin("tycho_storage_remove_outdated_states_time");
            let shard_state_storage = storage.shard_state_storage();
            let _ = match shard_state_storage
                .remove_outdated_states(block_id.seqno)
                .await
            {
                Ok(top_blocks) => Some(top_blocks),
                Err(e) => {
                    tracing::error!(target: "storage", "Failed to GC state: {e:?}");
                    None
                }
            };

            // subscriber.on_after_states_gc(&block_id, &top_blocks).await;
        }
    });
}

pub fn start_archives_gc(storage: Storage) -> Result<()> {
    let options = match &storage.inner.config.archives {
        Some(options) => options,
        None => return Ok(()),
    };

    struct LowerBound {
        archive_id: AtomicU32,
        changed: Notify,
    }

    #[allow(unused_mut)]
    let mut lower_bound = None::<Arc<LowerBound>>;

    match options.gc_interval {
        ArchivesGcInterval::Manual => Ok(()),
        ArchivesGcInterval::PersistentStates { offset } => {
            // let engine = self.clone();
            tokio::spawn(async move {
                let persistent_state_keeper = storage.runtime_storage().persistent_state_keeper();

                loop {
                    tokio::pin!(let new_state_found = persistent_state_keeper.new_state_found(););

                    let (until_id, untile_time) = match persistent_state_keeper.current() {
                        Some(state) => {
                            let untile_time =
                                (state.meta().gen_utime() as u64).add(offset.as_secs());
                            (state.id().seqno, untile_time)
                        }
                        None => {
                            new_state_found.await;
                            continue;
                        }
                    };

                    tokio::select!(
                        _ = tokio::time::sleep(duration_between_unix_and_instant(untile_time, Instant::now())) => {},
                        _ = &mut new_state_found => continue,
                    );

                    if let Some(lower_bound) = &lower_bound {
                        loop {
                            tokio::pin!(let lower_bound_changed = lower_bound.changed.notified(););

                            let lower_bound = lower_bound.archive_id.load(Ordering::Acquire);
                            if until_id < lower_bound {
                                break;
                            }

                            tracing::info!(
                                until_id,
                                lower_bound,
                                "waiting for the archives barrier"
                            );
                            lower_bound_changed.await;
                        }
                    }

                    if let Err(e) = storage
                        .block_storage()
                        .remove_outdated_archives(until_id)
                        .await
                    {
                        tracing::error!("failed to remove outdated archives: {e:?}");
                    }

                    new_state_found.await;
                }
            });

            Ok(())
        }
    }
}

struct Inner {
    root: FileDb,
    base_db: BaseDb,
    config: StorageConfig,

    blocks_gc_state: BlockGcState,

    runtime_storage: Arc<RuntimeStorage>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_connection_storage: Arc<BlockConnectionStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: ShardStateStorage,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
    rpc_state: Option<RpcStorage>,
    internal_queue_storage: InternalQueueStorage,
}

struct BlockGcState {
    enabled_for_sync: AtomicBool,
}
