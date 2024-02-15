use std::path::PathBuf;
use std::sync::Arc;
use std::thread::available_parallelism;

use anyhow::{Context, Result};
use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use weedb::{Caches, WeeDb};

pub use weedb::Stats as RocksdbStats;
pub use weedb::{rocksdb, BoundedCfHandle, ColumnFamily, Table};
pub mod refcount;
pub mod tables;

mod config;
mod migrations;

pub struct Db {
    pub archives: Table<tables::Archives>,
    pub block_handles: Table<tables::BlockHandles>,
    pub key_blocks: Table<tables::KeyBlocks>,
    pub package_entries: Table<tables::PackageEntries>,
    pub shard_states: Table<tables::ShardStates>,
    pub cells: Table<tables::Cells>,
    pub node_states: Table<tables::NodeStates>,
    pub prev1: Table<tables::Prev1>,
    pub prev2: Table<tables::Prev2>,
    pub next1: Table<tables::Next1>,
    pub next2: Table<tables::Next2>,

    compaction_lock: tokio::sync::RwLock<()>,
    inner: WeeDb,
}

impl Db {
    pub fn open(path: PathBuf, options: DbOptions) -> Result<Arc<Self>> {
        tracing::info!(
            rocksdb_lru_capacity = %options.rocksdb_lru_capacity,
            cells_cache_size = %options.cells_cache_size,
            "opening DB"
        );

        let limit = match fdlimit::raise_fd_limit() {
            // New fd limit
            Ok(fdlimit::Outcome::LimitRaised { to, .. }) => to,
            // Current soft limit
            _ => {
                rlimit::getrlimit(rlimit::Resource::NOFILE)
                    .unwrap_or((256, 0))
                    .0
            }
        };

        let caches_capacity =
            std::cmp::max(options.rocksdb_lru_capacity, ByteSize::mib(256)).as_u64() as usize;

        let caches = Caches::with_capacity(caches_capacity);
        let threads = available_parallelism()?.get();

        let inner = WeeDb::builder(path, caches)
            .options(|opts, _| {
                opts.set_paranoid_checks(false);

                // bigger base level size - less compactions
                // parallel compactions finishes faster - less write stalls

                opts.set_max_subcompactions(threads as u32 / 2);

                // io
                opts.set_max_open_files(limit as i32);

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
            })
            .with_table::<tables::Archives>()
            .with_table::<tables::BlockHandles>()
            .with_table::<tables::KeyBlocks>()
            .with_table::<tables::ShardStates>()
            .with_table::<tables::Cells>()
            .with_table::<tables::NodeStates>()
            .with_table::<tables::Prev1>()
            .with_table::<tables::Prev2>()
            .with_table::<tables::Next1>()
            .with_table::<tables::Next2>()
            .with_table::<tables::PackageEntries>()
            .build()
            .context("Failed building db")?;

        migrations::apply(&inner).context("Failed to apply migrations")?;

        Ok(Arc::new(Self {
            archives: inner.instantiate_table(),
            block_handles: inner.instantiate_table(),
            key_blocks: inner.instantiate_table(),
            package_entries: inner.instantiate_table(),
            shard_states: inner.instantiate_table(),
            cells: inner.instantiate_table(),
            node_states: inner.instantiate_table(),
            prev1: inner.instantiate_table(),
            prev2: inner.instantiate_table(),
            next1: inner.instantiate_table(),
            next2: inner.instantiate_table(),
            compaction_lock: tokio::sync::RwLock::default(),
            inner,
        }))
    }

    #[inline]
    pub fn raw(&self) -> &Arc<rocksdb::DB> {
        self.inner.raw()
    }

    pub fn get_memory_usage_stats(&self) -> Result<RocksdbStats> {
        self.inner.get_memory_usage_stats().map_err(From::from)
    }

    pub async fn delay_compaction(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.compaction_lock.read().await
    }

    pub async fn trigger_compaction(&self) {
        use std::time::Instant;

        let _compaction_guard = self.compaction_lock.write().await;

        let tables = [
            (self.block_handles.cf(), "block handles"),
            (self.package_entries.cf(), "package entries"),
            (self.archives.cf(), "archives"),
            (self.shard_states.cf(), "shard states"),
            (self.cells.cf(), "cells"),
        ];

        for (cf, title) in tables {
            tracing::info!("{title} compaction started");

            let instant = Instant::now();

            let bound = Option::<[u8; 0]>::None;
            self.raw().compact_range_cf(&cf, bound, bound);

            tracing::info!(
                elapsed = %humantime::format_duration(instant.elapsed()),
                "{title} compaction finished"
            );
        }
    }

    pub fn get_disk_usage(&self) -> Result<Vec<DiskUsageInfo>> {
        use std::thread;

        fn get_table_stats<T: ColumnFamily>(db: &WeeDb) -> (ByteSize, ByteSize) {
            let cf = db.instantiate_table::<T>();
            let res: (usize, usize) = cf
                .iterator(rocksdb::IteratorMode::Start)
                .filter_map(|x| {
                    let x = match x {
                        Ok(x) => x,
                        Err(e) => {
                            tracing::error!("Error while iterating: {}", e);
                            return None;
                        }
                    };
                    Some((x.0.len(), x.1.len()))
                })
                .fold((0, 0), |acc, x| (acc.0 + x.0, acc.1 + x.1));

            (ByteSize(res.0 as u64), ByteSize(res.1 as u64))
        }

        macro_rules! stats {
             ($spawner:expr, $( $x:ident => $table:ty ),* ) => {{
                    $(
                   let $x = $spawner.spawn(|| get_table_stats::<$table>(&self.inner));
                    )*
                 stats!($($x),*)
             }
             };
            ( $( $x:ident),* ) => {
                {
                    let mut temp_vec = Vec::new();
                    $(
                        temp_vec.push({
                            let $x = $x.join().map_err(|_|anyhow::anyhow!("Join error"))?;
                            DiskUsageInfo {
                                cf_name: stringify!($x).to_string(),
                                keys_total: $x.0,
                                values_total: $x.1,
                            }
                        });
                    )*
                    return Ok(temp_vec)
                }
            };
        }

        let stats = thread::scope(|s| -> Result<Vec<DiskUsageInfo>> {
            stats!(s,
                archives => tables::Archives,
                block_handles => tables::BlockHandles,
                key_blocks => tables::KeyBlocks,
                package_entries => tables::PackageEntries,
                shard_states => tables::ShardStates,
                cells => tables::Cells,
                node_states => tables::NodeStates,
                prev1 => tables::Prev1,
                prev2 => tables::Prev2,
                next1 => tables::Next1,
                next2 => tables::Next2
            )
        })?;

        Ok(stats)
    }
}

#[derive(Debug, Clone)]
pub struct DiskUsageInfo {
    pub cf_name: String,
    pub keys_total: ByteSize,
    pub values_total: ByteSize,
}

impl Drop for Db {
    fn drop(&mut self) {
        self.raw().cancel_all_background_work(true);
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DbOptions {
    pub rocksdb_lru_capacity: ByteSize,
    pub cells_cache_size: ByteSize,
}

impl Default for DbOptions {
    fn default() -> Self {
        // Fetch the currently available memory in bytes
        let available = {
            let mut sys = sysinfo::System::new();
            sys.refresh_memory();
            sys.available_memory()
        };

        // Estimated memory usage of components other than cache:
        // - 2 GiBs for write buffers(4 if we are out of luck and all memtables are being flushed at the same time)
        // - 2 GiBs for indexer logic
        // - 10 bits per cell for bloom filter. Realistic case is 100M cells, so 0.25 GiBs
        // - 1/3 of all available memory is reserved for kernel buffers
        const WRITE_BUFFERS: ByteSize = ByteSize::gib(2);
        const INDEXER_LOGIC: ByteSize = ByteSize::gib(2);
        const BLOOM_FILTER: ByteSize = ByteSize::mib(256);
        let estimated_memory_usage = WRITE_BUFFERS + INDEXER_LOGIC + BLOOM_FILTER + available / 3;

        // Reduce the available memory by the fixed offset
        let available = available
            .checked_sub(estimated_memory_usage.as_u64())
            .unwrap_or_else(|| {
                tracing::error!(
                    "Not enough memory for cache, using 1/4 of all available memory. \
                    Tweak `db_options` in config to improve performance."
                );
                available / 4
            });

        // We will use 3/4 of available memory for the cells cache (at most 4 GB).
        let cells_cache_size = std::cmp::min(ByteSize(available * 4 / 3), ByteSize::gib(4));

        // The reset of the memory is used for LRU cache (at least 128 MB)
        let rocksdb_lru_capacity = std::cmp::max(
            ByteSize(available.saturating_sub(cells_cache_size.as_u64())),
            ByteSize::mib(128),
        );

        Self {
            rocksdb_lru_capacity,
            cells_cache_size,
        }
    }
}
