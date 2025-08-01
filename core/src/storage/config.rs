use std::time::Duration;

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use tycho_util::config::PartialConfig;
use tycho_util::serde_helpers;

#[derive(Debug, Clone, Serialize, Deserialize, PartialConfig)]
#[serde(deny_unknown_fields, default)]
pub struct CoreStorageConfig {
    /// Runtime cells cache size.
    ///
    /// Default: 256 MB.
    #[important]
    pub cells_cache_size: ByteSize,

    /// Archive chunk size.
    ///
    /// Default: 1 MB.
    pub archive_chunk_size: ByteSize,

    /// Number of concurrent running split block tasks.
    ///
    /// Default: 100.
    pub split_block_tasks: usize,

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

impl CoreStorageConfig {
    #[cfg(any(test, feature = "test"))]
    pub fn new_potato() -> Self {
        Self {
            cells_cache_size: ByteSize::kb(1024),
            ..Default::default()
        }
    }
}

impl Default for CoreStorageConfig {
    fn default() -> Self {
        Self {
            cells_cache_size: ByteSize::mb(256),
            split_block_tasks: 100,
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
