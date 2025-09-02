use std::num::NonZeroU16;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::FastHashMap;

use crate::models::UnixTime;
use crate::storage::EventTag;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BanConfig(FastHashMap<EventTag, BanConfigValue>);
impl BanConfig {
    /// [`Self::get`] and [`Self::max_known_duration`] are in par by default ban duration
    pub fn get(&self, tag: EventTag) -> BanConfigValue {
        self.0.get(&tag).copied().unwrap_or(BanConfigValue {
            duration: BanConfigDuration::DAY,
            toleration: None,
        })
    }
    /// [`Self::get`] and [`Self::max_known_duration`] are in par by default ban duration
    pub fn max_known_duration(&self) -> BanConfigDuration {
        (self.0.values())
            .map(|v| match v.toleration {
                None => v.duration,
                Some(t) => v.duration.max(t.duration),
            })
            .fold(BanConfigDuration::DAY, Ord::max)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BanConfigValue {
    /// time to wait before unban
    pub duration: BanConfigDuration,
    /// to ban later than at first attempt
    #[serde(default)]
    pub toleration: Option<BanToleration>,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BanToleration {
    /// sliding window to count event occurrences
    pub duration: BanConfigDuration,
    /// acceptable event occurrences in given window
    pub count: NonZeroU16,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BanConfigDuration(#[serde(with = "tycho_util::serde_helpers::humantime")] Duration);

impl BanConfigDuration {
    pub const DAY: Self = Self(Duration::from_secs(24 * 60 * 60));
    pub fn to_time(self) -> UnixTime {
        UnixTime::from_millis(self.0.as_millis() as u64)
    }
}

impl Default for BanConfig {
    fn default() -> BanConfig {
        let five_min = BanConfigDuration(Duration::from_secs(5 * 60));
        let half_hour = BanConfigDuration(Duration::from_secs(30 * 60));

        Self(FastHashMap::from_iter([
            (EventTag::UnknownQuery, BanConfigValue {
                duration: five_min,
                toleration: None,
            }),
            (EventTag::BadRequest, BanConfigValue {
                duration: five_min,
                toleration: None,
            }),
            (EventTag::BadResponse, BanConfigValue {
                duration: five_min,
                toleration: None,
            }),
            (EventTag::Equivocated, BanConfigValue {
                duration: half_hour,
                toleration: Some(BanToleration {
                    duration: BanConfigDuration::DAY,
                    count: 3.try_into().unwrap(),
                }),
            }),
            (EventTag::EvidenceNoInclusion, BanConfigValue {
                duration: BanConfigDuration::DAY,
                toleration: Some(BanToleration {
                    duration: BanConfigDuration::DAY,
                    count: 5.try_into().unwrap(),
                }),
            }),
        ]))
    }
}
