use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveConfig {
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
        offset_sec: u64,
    },
}

impl Default for ArchivesGcInterval {
    fn default() -> Self {
        Self::PersistentStates { offset_sec: 300 }
    }
}