use std::path::PathBuf;
use std::time::Duration;

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use tycho_util::config::PartialConfig;
use tycho_util::{FastHashMap, serde_helpers};

#[derive(Debug, Clone, Serialize, Deserialize, PartialConfig)]
#[serde(deny_unknown_fields, default)]
pub struct CoreStorageConfig {
    /// Runtime cells cache size.
    ///
    /// Default: 256 MB.
    #[important]
    pub cells_cache_size: ByteSize,

    /// Minimal epoch interval when the state can be reused.
    ///
    /// Default: 3.
    pub drop_interval: u32,

    /// Whether to pack blocks into archives.
    ///
    /// Default: `true`.
    pub store_archives: bool,

    /// Archives storage config.
    ///
    /// Archives are disabled if this field is `None`.
    pub archives_gc: Option<ArchivesGcConfig>,

    /// States GC config.
    ///
    /// States GC is disabled if this field is `None`.
    pub states_gc: Option<StatesGcConfig>,

    /// State partitions config.
    ///
    /// State partitioning is disabled if this field is `None`.
    pub state_parts: Option<StatePartitionsConfig>,

    /// Blocks GC config.
    ///
    /// Blocks GC is disabled if this field is `None`.
    pub blocks_gc: Option<BlocksGcConfig>,

    /// Blocks cache config.
    pub blocks_cache: BlocksCacheConfig,

    /// Blob DB config.
    pub blob_db: BlobDbConfig,
}

impl CoreStorageConfig {
    #[cfg(any(test, feature = "test"))]
    pub fn new_potato() -> Self {
        Self {
            cells_cache_size: ByteSize::kb(1024),
            blob_db: BlobDbConfig {
                pre_create_cas_tree: false,
            },
            archives_gc: None,
            states_gc: None,
            blocks_gc: None,
            ..Default::default()
        }
    }

    pub fn without_gc(mut self) -> Self {
        self.archives_gc = None;
        self.states_gc = None;
        self.blocks_gc = None;
        self
    }
}

impl Default for CoreStorageConfig {
    fn default() -> Self {
        Self {
            cells_cache_size: ByteSize::mb(256),
            drop_interval: 3,
            store_archives: true,
            archives_gc: Some(ArchivesGcConfig::default()),
            states_gc: Some(StatesGcConfig::default()),
            state_parts: None,
            blocks_gc: Some(BlocksGcConfig::default()),
            blocks_cache: BlocksCacheConfig::default(),
            blob_db: BlobDbConfig::default(),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlobDbConfig {
    pub pre_create_cas_tree: bool,
}

impl Default for BlobDbConfig {
    fn default() -> Self {
        Self {
            pre_create_cas_tree: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct StatePartitionsConfig {
    /// State partitions split depth
    ///
    /// Default: 0 -> 2^0 = 1 partition, no split.
    pub split_depth: u8,

    /// Map of state partitions directories.
    ///
    /// Default: empty, relative paths will be generated.
    #[serde(with = "serde_shard_part_dirs_map")]
    pub part_dirs: FastHashMap<u64, PathBuf>,
}

mod serde_shard_part_dirs_map {
    use std::path::PathBuf;

    use serde::de::Deserializer;
    use serde::ser::{SerializeMap, Serializer};
    use tycho_block_util::block::DisplayShardPrefix;
    use tycho_util::FastHashMap;

    use super::*;

    pub fn serialize<S>(value: &FastHashMap<u64, PathBuf>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[repr(transparent)]
        struct WrappedKey<'a>(&'a u64);
        impl Serialize for WrappedKey<'_> {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&DisplayShardPrefix(self.0).to_string())
            }
        }

        let mut ser = serializer.serialize_map(Some(value.len()))?;
        for (prefix, path) in value {
            ser.serialize_entry(&WrappedKey(prefix), &path)?;
        }
        ser.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<FastHashMap<u64, PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(PartialEq, Eq, Hash)]
        #[repr(transparent)]
        struct WrappedKey(u64);
        impl<'de> Deserialize<'de> for WrappedKey {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let tmp = String::deserialize(deserializer)?;
                if tmp.len() != 16 || !tmp.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err(serde::de::Error::custom(format!(
                        "invalid shard prefix key format '{tmp}', expected 16 hex digits (e.g. a000000000000000)"
                    )));
                }
                let prefix = u64::from_str_radix(&tmp, 16).map_err(serde::de::Error::custom)?;
                Ok(Self(prefix))
            }
        }

        <FastHashMap<WrappedKey, PathBuf>>::deserialize(deserializer)
            .map(|map| map.into_iter().map(|(k, v)| (k.0, v)).collect())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    pub fn test_state_partitions_config() {
        let test: &str = r#"{
            "split_depth": 2,
            "part_dirs": {
                "2000000000000000": "/dev1/node/data/cells-part-1",
                "6000000000000000": "/dev2/node/data/cells-part-2",
                "a000000000000000": "/dev3/node/data/cells-part-3",
                "e000000000000000": "/dev4/node/data/cells-part-4"
            }
        }"#;

        let mut value = StatePartitionsConfig::default();
        value
            .part_dirs
            .insert(234567890, "/dev1/node/data/cells-part".into());
        println!("test: {:?}", test);

        let parsed: StatePartitionsConfig = serde_json::from_str(test).unwrap();
        println!("parsed: {:?}", parsed);

        assert_eq!(parsed.split_depth, 2);
        let path1 = parsed.part_dirs.get(&2305843009213693952).unwrap();
        let expected = PathBuf::from_str("/dev1/node/data/cells-part-1").unwrap();
        assert_eq!(path1, &expected);

        let serialized = serde_json::to_string(&parsed).unwrap();
        println!("test serialized: {:?}", serialized);

        let mut value = StatePartitionsConfig::default();
        value
            .part_dirs
            .insert(234567890, "/dev99/node/data/cells-part".into());

        let test = serde_json::to_string(&value).unwrap();
        println!("test: {:?}", test);

        let parsed: StatePartitionsConfig = serde_json::from_str(&test).unwrap();
        println!("parsed: {:?}", parsed);

        assert_eq!(parsed, value);
    }
}
