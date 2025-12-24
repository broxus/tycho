use std::collections::hash_map;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use bytesize::ByteSize;
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tycho_util::FastHashMap;
use tycho_util::metrics::{FsUsageBuilder, FsUsageMonitor, spawn_metrics_loop};
use weedb::{WeakWeeDbRaw, rocksdb};

use crate::config::StorageConfig;
use crate::fs::{Dir, TempFileStorage};
use crate::kv::{NamedTables, TableContext, WeeDbExt};

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
        let root_dir = Dir::new(&config.root_dir)?;
        let files_dir = root_dir.create_subdir(FILES_SUBDIR)?;

        let temp_files =
            TempFileStorage::new(&files_dir).context("failed to create temp files storage")?;
        temp_files.remove_outdated_files().await?;

        let mut fs_usage = FsUsageBuilder::new()
            .add_path(files_dir.path())
            .add_path(temp_files.dir().path())
            .build();

        fs_usage.spawn_metrics_loop(Duration::from_secs(60))?;

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
                fs_usage,
                threads,
                fdlimit,
                rocksdb_table_context: Default::default(),
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

    pub fn root_dir(&self) -> &Dir {
        &self.inner.root_dir
    }

    pub fn files_dir(&self) -> &Dir {
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

    pub fn fs_usage(&self) -> &FsUsageMonitor {
        &self.inner.fs_usage
    }

    pub fn rocksdb_table_context(&self) -> &TableContext {
        &self.inner.rocksdb_table_context
    }

    pub fn rocksdb_env(&self) -> &rocksdb::Env {
        &self.inner.rocksdb_env
    }

    pub fn trigger_rocksdb_compaction(&self, name: &str) -> bool {
        let Some(known) = self.inner.rocksdb_instances.load().get(name).cloned() else {
            return false;
        };
        known.compaction_events.notify_waiters();
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

    pub fn validate_options<T>(&self) -> Result<()>
    where
        T: NamedTables<Context = TableContext> + 'static,
    {
        let this = self.inner.as_ref();
        // TODO: Make configuration fallible in `weedb`?
        weedb::WeeDbRaw::builder("nonexisting", this.rocksdb_table_context.clone())
            .with_options(|opts, _| self.apply_default_options(opts))
            .with_tables::<T>();
        Ok(())
    }

    pub fn open_preconfigured<P, T>(&self, subdir: P) -> Result<weedb::WeeDb<T>>
    where
        P: AsRef<Path>,
        T: NamedTables<Context = TableContext> + 'static,
    {
        self.open_preconfigured_part(subdir, None)
    }

    pub fn open_preconfigured_part<P, T>(
        &self,
        dir_path: P,
        part_id: Option<u64>,
    ) -> Result<weedb::WeeDb<T>>
    where
        P: AsRef<Path>,
        T: NamedTables<Context = TableContext> + 'static,
    {
        let dir_path = dir_path.as_ref();
        tracing::debug!(dir_path = %dir_path.display(), "opening RocksDB instance");

        let this = self.inner.as_ref();

        let db_dir = if dir_path.is_relative() {
            this.root_dir.create_subdir(dir_path)?
        } else {
            Dir::new(dir_path)?
        };
        this.fs_usage.add_path(db_dir.path());

        let db =
            weedb::WeeDb::<T>::builder_prepared(db_dir.path(), this.rocksdb_table_context.clone())
                .with_metrics_enabled(this.config.rocksdb_enable_metrics)
                .with_options(|opts, _| self.apply_default_options(opts))
                .build()?;

        if let Some(name) = db.db_name() {
            let name_w_part = part_id.map(|id| format!("{}-{:016x}", name, id));
            let name = name_w_part.unwrap_or_else(|| name.to_owned());
            self.add_rocksdb_instance(&name, db.raw());
        }

        tracing::debug!(current_rocksdb_buffer_usage = ?self.rocksdb_table_context().buffer_usage());

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
        opts.set_max_log_file_size(ByteSize::gib(1).as_u64() as usize);

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
    root_dir: Dir,
    files_dir: Dir,
    temp_files: TempFileStorage,
    fs_usage: FsUsageMonitor,
    threads: usize,
    fdlimit: u64,
    rocksdb_table_context: TableContext,
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
    compaction_events: Arc<Notify>,
    task_handle: AbortHandle,
}

impl KnownInstance {
    fn new(name: &str, weak: WeakWeeDbRaw) -> Self {
        let name = name.to_owned();
        let compaction_events = Arc::new(Notify::new());
        let task_handle = tokio::task::spawn({
            let weak = weak.clone();
            let compaction_events = compaction_events.clone();
            async move {
                tracing::debug!(name, "compaction trigger listener started");
                scopeguard::defer! {
                    tracing::debug!(name, "compaction trigger listener stopped");
                }

                let mut notified = compaction_events.notified();
                loop {
                    notified.await;
                    notified = compaction_events.notified();
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
            compaction_events,
            task_handle,
        }
    }
}

impl Drop for KnownInstance {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}
