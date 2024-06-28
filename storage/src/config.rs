use std::path::{Path, PathBuf};
use std::time::Duration;

use bytesize::ByteSize;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

use crate::BlocksGcKind;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// Path to the root directory of the storage.
    ///
    /// Default: `./db`.
    pub root_dir: PathBuf,

    /// Whether to enable `RocksDB` metrics.
    ///
    /// Default: `true`.
    pub rocksdb_enable_metrics: bool,

    /// `RocksDB` LRU cache capacity.
    ///
    /// Default: calculated based on the available memory.
    pub rocksdb_lru_capacity: ByteSize,

    /// Runtime cells cache size.
    ///
    /// Default: calculated based on the available memory.
    pub cells_cache_size: ByteSize,

    /// Archives storage config.
    ///
    /// Archives are disabled if this field is `None`.
    pub archives: Option<ArchivesConfig>,

    /// States GC config.
    ///
    /// States GC is disabled if this field is `None`.
    pub states_gc_options: Option<StateGcOptions>,

    /// Blocks GC config.
    ///
    /// Blocks GC is disabled if this field is `None`.
    pub blocks_gc_config: Option<BlocksGcOptions>,
}

impl StorageConfig {
    /// Creates a new storage config with very low cache sizes.
    pub fn new_potato(path: &Path) -> Self {
        Self {
            root_dir: path.to_owned(),
            rocksdb_lru_capacity: ByteSize::kb(1024),
            cells_cache_size: ByteSize::kb(1024),
            rocksdb_enable_metrics: false,
            archives: Some(ArchivesConfig::default()),
            states_gc_options: Some(StateGcOptions::default()),
            blocks_gc_config: Some(BlocksGcOptions::default()),
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
            rocksdb_lru_capacity,
            rocksdb_enable_metrics: true,
            archives: Some(ArchivesConfig::default()),
            states_gc_options: Some(StateGcOptions::default()),
            blocks_gc_config: Some(BlocksGcOptions::default()),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivesConfig {
    pub gc_interval: ArchivesGcInterval,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "type", rename_all = "snake_case")]
pub enum ArchivesGcInterval {
    /// Do not perform archives GC
    Manual,
    /// Archives GC triggers on each persistent state
    PersistentStates {
        /// Remove archives after this interval after the new persistent state
        #[serde(with = "serde_helpers::humantime")]
        offset: Duration,
    },
}

impl Default for ArchivesGcInterval {
    fn default() -> Self {
        Self::PersistentStates {
            offset: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StateGcOptions {
    /// Default: rand[0,900)
    pub offset_sec: u64,
    /// Default: 900
    pub interval_sec: u64,
}

impl Default for StateGcOptions {
    fn default() -> Self {
        Self {
            offset_sec: rand::thread_rng().gen_range(0..60),
            interval_sec: 60,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlocksGcOptions {
    /// Blocks GC type
    /// - `before_previous_key_block` - on each new key block delete all blocks before the previous one
    /// - `before_previous_persistent_state` - on each new key block delete all blocks before the
    ///   previous key block with persistent state
    pub kind: BlocksGcKind,

    /// Whether to enable blocks GC during sync. Default: true
    pub enable_for_sync: bool,

    /// Max `WriteBatch` entries before apply
    pub max_blocks_per_batch: Option<usize>,
}

impl Default for BlocksGcOptions {
    fn default() -> Self {
        Self {
            kind: BlocksGcKind::BeforePreviousPersistentState,
            enable_for_sync: true,
            max_blocks_per_batch: Some(100_000),
        }
    }
}
