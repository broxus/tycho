use std::collections::hash_map;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tycho_util::metrics::spawn_metrics_loop;
use tycho_util::FastHashMap;
use weedb::{rocksdb, WeakWeeDbRaw};

use crate::config::StorageConfig;
use crate::db::{FileDb, WeeDbExt, WithMigrations};
use crate::store::TempFileStorage;

const FILES_SUBDIR: &str = "files";

#[derive(Clone)]
pub struct StorageContext {
    inner: Arc<StorageContextInner>,
}

impl StorageContext {
    /// Creates a new temporary storage context with potato config.
    ///
    /// NOTE: Temp dir must live longer than the storage,
    /// otherwise compaction filter will not work.
    #[cfg(any(test, feature = "test"))]
    pub async fn new_temp() -> Result<(Self, tempfile::TempDir)> {
        let tmp_dir = tempfile::tempdir()?;
        let config = StorageConfig::new_potato(tmp_dir.path());
        let ctx = StorageContext::new(config).await?;
        Ok((ctx, tmp_dir))
    }

    pub async fn new(config: StorageConfig) -> Result<Self> {
        let root_dir = FileDb::new(&config.root_dir)?;
        let files_dir = root_dir.create_subdir(FILES_SUBDIR)?;

        let temp_files =
            TempFileStorage::new(&files_dir).context("failed to create temp files storage")?;
        temp_files.remove_outdated_files().await?;

        let threads = std::thread::available_parallelism()?.get();
        let fdlimit = match fdlimit::raise_fd_limit() {
            // New fd limit
            Ok(fdlimit::Outcome::LimitRaised { to, .. }) => to,
            // Current soft limit
            _ => match rlimit::getrlimit(rlimit::Resource::NOFILE) {
                Ok((limit, _)) => limit,
                Err(_) => 256,
            },
        };

        let rocksdb_caches =
            weedb::Caches::with_capacity(config.rocksdb_lru_capacity.as_u64() as _);

        let mut rocksdb_env =
            rocksdb::Env::new().context("failed to create a new RocksDB environemnt")?;
        let thread_pool_size = std::cmp::max(threads as i32 / 2, 2);
        rocksdb_env.set_background_threads(thread_pool_size);
        rocksdb_env.set_low_priority_background_threads(thread_pool_size);
        rocksdb_env.set_high_priority_background_threads(thread_pool_size);

        let rocksdb_instances: Arc<ArcSwap<KnownInstances>> = Default::default();
        let rocksdb_metrics_handle = if config.rocksdb_enable_metrics {
            Some(spawn_metrics_loop(
                &rocksdb_instances,
                Duration::from_secs(5),
                async move |this| {
                    let this = this.load_full();
                    for item in this.values() {
                        if let Some(db) = item.weak.upgrade() {
                            db.refresh_metrics();
                        };
                    }
                },
            ))
        } else {
            None
        };

        tracing::info!(
            threads,
            fdlimit,
            thread_pool_size,
            root_dir = %config.root_dir.display(),
            "storage context created",
        );

        Ok(Self {
            inner: Arc::new(StorageContextInner {
                config,
                root_dir,
                files_dir,
                temp_files,
                threads,
                fdlimit,
                rocksdb_caches,
                rocksdb_env,
                rocksdb_instances_lock: Default::default(),
                rocksdb_instances,
                rocksdb_metrics_handle,
            }),
        })
    }

    pub fn config(&self) -> &StorageConfig {
        &self.inner.config
    }

    pub fn root_dir(&self) -> &FileDb {
        &self.inner.root_dir
    }

    pub fn files_dir(&self) -> &FileDb {
        &self.inner.files_dir
    }

    pub fn temp_files(&self) -> &TempFileStorage {
        &self.inner.temp_files
    }

    pub fn threads(&self) -> usize {
        self.inner.threads
    }

    pub fn fdlimit(&self) -> u64 {
        self.inner.fdlimit
    }

    pub fn rocksdb_caches(&self) -> &weedb::Caches {
        &self.inner.rocksdb_caches
    }

    pub fn rocksdb_env(&self) -> &rocksdb::Env {
        &self.inner.rocksdb_env
    }

    pub fn trigger_rocksdb_compaction(&self, name: &str) -> bool {
        let Some(known) = self.inner.rocksdb_instances.load().get(name).cloned() else {
            return false;
        };
        known.compation_events.notify_waiters();
        true
    }

    pub fn add_rocksdb_instance(&self, name: &str, db: &weedb::WeeDbRaw) -> bool {
        let _guard = self.inner.rocksdb_instances_lock.lock().unwrap();

        let mut items = Arc::unwrap_or_clone(self.inner.rocksdb_instances.load_full());

        // Remove all identical items just in case.
        let db = weedb::WeeDbRaw::downgrade(db);
        items.retain(|_, item| !weedb::WeakWeeDbRaw::ptr_eq(&db, &item.weak));

        // Insert a new item.
        match items.entry(name.to_owned()) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Arc::new(KnownInstance::new(name, db)));
                true
            }
            hash_map::Entry::Occupied(mut entry) => {
                if !weedb::WeakWeeDbRaw::ptr_eq(&db, &entry.get().weak) {
                    *entry.get_mut() = Arc::new(KnownInstance::new(name, db));
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn open_preconfigured<P, T>(&self, subdir: P) -> Result<weedb::WeeDb<T>>
    where
        P: AsRef<Path>,
        T: WithMigrations<Context = weedb::Caches> + 'static,
    {
        let subdir = subdir.as_ref();
        tracing::debug!(subdir = %subdir.display(), "opening RocksDB instance");

        let this = self.inner.as_ref();

        let db_dir = this.root_dir.create_subdir(subdir)?;
        let db = weedb::WeeDb::<T>::builder_prepared(db_dir.path(), this.rocksdb_caches.clone())
            .with_metrics_enabled(this.config.rocksdb_enable_metrics)
            .with_options(|opts, _| self.apply_default_options(opts))
            .build()?;

        if let Some(name) = db.db_name() {
            self.add_rocksdb_instance(name, db.raw());
        }

        Ok(db)
    }

    pub fn apply_default_options(&self, opts: &mut rocksdb::Options) {
        let this = self.inner.as_ref();

        opts.set_paranoid_checks(false);

        // parallel compactions finishes faster - less write stalls
        opts.set_max_subcompactions(this.threads as u32 / 2);

        // io
        opts.set_max_open_files(this.fdlimit as i32);

        // logging
        opts.set_log_level(rocksdb::LogLevel::Info);
        opts.set_keep_log_file_num(2);
        opts.set_recycle_log_file_num(2);

        // cf
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // cpu
        // https://github.com/facebook/rocksdb/blob/0560544e86c1f97f8d1da348f2647aadaefbd095/options/options.cc#L680-L685
        // docs are lying as always
        // so fuck this deprecation warning
        #[allow(deprecated)]
        opts.set_max_background_flushes(this.threads as i32 / 2);
        #[allow(deprecated)]
        opts.set_max_background_compactions(this.threads as i32 / 2);

        opts.set_env(&this.rocksdb_env);

        opts.set_allow_concurrent_memtable_write(false);

        // debug
        // NOTE: could slower everything a bit in some cloud environments.
        //       See: https://github.com/facebook/rocksdb/issues/3889
        //
        // opts.enable_statistics();
        // opts.set_stats_dump_period_sec(600);
    }
}

struct StorageContextInner {
    config: StorageConfig,
    root_dir: FileDb,
    files_dir: FileDb,
    temp_files: TempFileStorage,
    threads: usize,
    fdlimit: u64,
    rocksdb_caches: weedb::Caches,
    rocksdb_env: rocksdb::Env,
    rocksdb_instances_lock: Mutex<()>,
    rocksdb_instances: Arc<ArcSwap<KnownInstances>>,
    rocksdb_metrics_handle: Option<AbortHandle>,
}

impl Drop for StorageContextInner {
    fn drop(&mut self) {
        if let Some(handle) = &self.rocksdb_metrics_handle {
            handle.abort();
        }
    }
}

type KnownInstances = FastHashMap<String, Arc<KnownInstance>>;

struct KnownInstance {
    weak: WeakWeeDbRaw,
    compation_events: Arc<Notify>,
    task_handle: AbortHandle,
}

impl KnownInstance {
    fn new(name: &str, weak: WeakWeeDbRaw) -> Self {
        let name = name.to_owned();
        let compation_events = Arc::new(Notify::new());
        let task_handle = tokio::task::spawn({
            let weak = weak.clone();
            let compation_events = compation_events.clone();
            async move {
                tracing::debug!(name, "compaction trigger listener started");
                scopeguard::defer! {
                    tracing::debug!(name, "compaction trigger listener stopped");
                }

                let mut notified = compation_events.notified();
                loop {
                    notified.await;
                    notified = compation_events.notified();
                    let Some(db) = weak.upgrade() else {
                        break;
                    };

                    db.compact();
                }
            }
        })
        .abort_handle();

        Self {
            weak,
            compation_events,
            task_handle,
        }
    }
}

impl Drop for KnownInstance {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}
