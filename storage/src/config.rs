use std::path::{Path, PathBuf};
use std::time::Duration;

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

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

    /// Archive chunk size.
    ///
    /// Default: 1 MB.
    pub archive_chunk_size: ByteSize,

    /// Archives storage config.
    ///
    /// Archives are disabled if this field is `None`.
    pub archives_gc: Option<ArchivesGcConfig>,

    /// States GC config.
    ///
    /// States GC is disabled if this field is `None`.
    pub states_gc: Option<StatesGcConfig>,

    /// Blocks GC config.
    ///
    /// Blocks GC is disabled if this field is `None`.
    pub blocks_gc: Option<BlocksGcConfig>,

    /// Blocks cache config.
    pub blocks_cache: BlocksCacheConfig,
}

impl StorageConfig {
    /// Creates a new storage config with very low cache sizes.
    pub fn new_potato(path: &Path) -> Self {
        Self {
            root_dir: path.to_owned(),
            rocksdb_lru_capacity: ByteSize::kb(1024),
            cells_cache_size: ByteSize::kb(1024),
            archive_chunk_size: ByteSize::kb(1024),
            rocksdb_enable_metrics: false,
            archives_gc: None,
            states_gc: None,
            blocks_gc: None,
            blocks_cache: BlocksCacheConfig::default(),
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
            archive_chunk_size: ByteSize::kb(1024),
            archives_gc: Some(ArchivesGcConfig::default()),
            states_gc: Some(StatesGcConfig::default()),
            blocks_gc: Some(BlocksGcConfig::default()),
            blocks_cache: BlocksCacheConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ArchivesGcConfig {
    /// Remove archives after this interval after the new persistent state
    #[serde(with = "serde_helpers::humantime")]
    pub persistent_state_offset: Duration,
}

impl Default for ArchivesGcConfig {
    fn default() -> Self {
        Self {
            persistent_state_offset: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StatesGcConfig {
    /// Wether to add random offset to the first interval.
    ///
    /// Default: true.
    pub random_offset: bool,
    /// Default: 900
    #[serde(with = "serde_helpers::humantime")]
    pub interval: Duration,
}

impl Default for StatesGcConfig {
    fn default() -> Self {
        Self {
            random_offset: true,
            interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BlocksGcConfig {
    /// Blocks GC type
    /// - `before_previous_key_block` - on each new key block delete all blocks before the previous one
    /// - `before_previous_persistent_state` - on each new key block delete all blocks before the
    ///   previous key block with persistent state
    #[serde(flatten)]
    pub ty: BlocksGcType,

    /// Whether to enable blocks GC during sync. Default: true
    pub enable_for_sync: bool,

    /// Max `WriteBatch` entries before apply
    pub max_blocks_per_batch: Option<usize>,
}

impl Default for BlocksGcConfig {
    fn default() -> Self {
        Self {
            ty: BlocksGcType::BeforeSafeDistance {
                safe_distance: 1000,
                min_interval: Duration::from_secs(60),
            },
            enable_for_sync: true,
            max_blocks_per_batch: Some(100_000),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BlocksGcType {
    /// Remove all blocks before the specified safe distance (of mc blocks).
    BeforeSafeDistance {
        /// Number of masterchain blocks to keep.
        safe_distance: u32,
        /// Minimum interval between GC runs.
        ///
        /// Should be about 1 minute.
        #[serde(with = "serde_helpers::humantime")]
        min_interval: Duration,
    },
    /// Remove all blocks before the previous key block.
    BeforePreviousKeyBlock,
    /// Remove all blocks before the previous persistent state.
    BeforePreviousPersistentState,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlocksCacheConfig {
    /// Entry TTL.
    ///
    /// Default: `5 min`.
    #[serde(with = "serde_helpers::humantime")]
    pub ttl: Duration,

    /// Cache capacity in bytes.
    ///
    /// Default: `500 MB`.
    pub size: ByteSize,
}

impl Default for BlocksCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            size: ByteSize::mb(500),
        }
    }
}
