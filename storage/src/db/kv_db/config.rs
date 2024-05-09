use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DbConfig {
    pub rocksdb_lru_capacity: ByteSize,
    pub cells_cache_size: ByteSize,
}
