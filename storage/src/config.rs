use std::path::{Path, PathBuf};

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

use crate::db::DbConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// Path to the root directory of the storage.
    ///
    /// Default: `./db`.
    pub root_dir: PathBuf,

    /// Runtime cells cache size.
    ///
    /// Default: calculated based on the available memory.
    pub cells_cache_size: ByteSize,

    /// RocksDB configuration.
    pub db_config: DbConfig,
}

impl StorageConfig {
    /// Creates a new storage config with very low cache sizes.
    pub fn new_potato(path: &Path) -> Self {
        Self {
            root_dir: path.to_owned(),
            cells_cache_size: ByteSize::kb(1024),
            db_config: DbConfig {
                rocksdb_lru_capacity: ByteSize::kb(1024),
            },
        }
    }
}

impl Default for StorageConfig {
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
            root_dir: PathBuf::from("./db"),
            cells_cache_size,
            db_config: DbConfig {
                rocksdb_lru_capacity,
            },
        }
    }
}
