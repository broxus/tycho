use std::path::{Path, PathBuf};

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use tycho_util::config::PartialConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialConfig)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// Path to the root directory of the storage.
    ///
    /// Default: `./db`.
    #[important]
    pub root_dir: PathBuf,

    /// Whether to enable `RocksDB` metrics.
    ///
    /// Default: `true`.
    pub rocksdb_enable_metrics: bool,

    /// `RocksDB` LRU cache capacity.
    ///
    /// Default: calculated based on the available memory.
    #[important]
    pub rocksdb_lru_capacity: ByteSize,
}

impl StorageConfig {
    /// Creates a new storage config with very low cache sizes.
    pub fn new_potato(path: &Path) -> Self {
        Self {
            root_dir: path.to_owned(),
            rocksdb_enable_metrics: false,
            rocksdb_lru_capacity: ByteSize::mib(1),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("./db"),
            rocksdb_enable_metrics: true,
            rocksdb_lru_capacity: ByteSize::gib(2),
        }
    }
}
