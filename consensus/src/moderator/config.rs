use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::FastHashMap;

use crate::models::UnixTime;
use crate::storage::EventKind;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BanConfig(FastHashMap<EventKind, BanConfigValue>);
impl BanConfig {
    pub fn get(&self, kind: EventKind) -> BanConfigValue {
        self.0.get(&kind).copied().unwrap_or_default()
    }
    /// returns at least default duration because config may be shallow
    pub fn max_duration(&self) -> BanConfigDuration {
        (self.0.values())
            .map(|v| v.duration.max(v.toleration.duration))
            .fold(BanConfigDuration::default(), |a, b| a.max(b))
    }
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BanConfigValue {
    /// time to wait before unban
    #[serde(default)]
    pub duration: BanConfigDuration,
    /// to ban later than at first attempt
    #[serde(default)]
    pub toleration: BanToleration,
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BanToleration {
    /// sliding window to count event occurrences
    #[serde(default)]
    pub duration: BanConfigDuration,
    /// acceptable event occurrences in given window
    #[serde(default)]
    pub count: u16,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BanConfigDuration(#[serde(with = "tycho_util::serde_helpers::humantime")] Duration);

impl BanConfigDuration {
    pub const FAST: Self = Self(Duration::from_secs(30 * 60));
    pub const SLOW: Self = Self(Duration::from_secs(24 * 60 * 60));

    pub fn to_time(self) -> UnixTime {
        UnixTime::from_millis(self.0.as_millis() as u64)
    }
}

impl Default for BanConfigDuration {
    fn default() -> Self {
        Self::SLOW
    }
}

impl Default for BanConfig {
    fn default() -> BanConfig {
        Self(FastHashMap::from_iter([
            (EventKind::UnknownQuery, BanConfigValue {
                duration: BanConfigDuration::FAST,
                toleration: BanToleration {
                    duration: BanConfigDuration::FAST,
                    count: 5,
                },
            }),
            (EventKind::BroadcastLimitReached, BanConfigValue {
                duration: BanConfigDuration::FAST,
                toleration: BanToleration::default(),
            }),
            (EventKind::SigRequestLimitReached, BanConfigValue {
                duration: BanConfigDuration::FAST,
                toleration: BanToleration::default(),
            }),
            (EventKind::UploadLimitReached, BanConfigValue {
                duration: BanConfigDuration::FAST,
                toleration: BanToleration::default(),
            }),
            (EventKind::Equivocated, BanConfigValue {
                duration: BanConfigDuration::FAST,
                toleration: BanToleration {
                    duration: BanConfigDuration(Duration::from_secs(3 * 60 * 60)),
                    count: 3,
                },
            }),
            (EventKind::EvidenceNoInclusion, BanConfigValue {
                duration: BanConfigDuration::SLOW,
                toleration: BanToleration {
                    duration: BanConfigDuration(Duration::from_secs(60 * 60)),
                    count: 5,
                },
            }),
        ]))
    }
}
