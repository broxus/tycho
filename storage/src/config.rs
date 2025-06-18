use std::path::{Path, PathBuf};

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

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
