use std::num::NonZeroU16;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::FastHashMap;

use crate::models::UnixTime;
use crate::moderator::EventTag;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModeratorConfig {
    /// Storage for ban-related data
    pub journal: JournalConfig,

    /// Ban durations and tolerations for mempool events
    pub bans: BanConfig,
}

impl ModeratorConfig {
    #[cfg(any(test, feature = "test"))]
    pub fn test_default() -> Self {
        let mut this: ModeratorConfig = serde_json::from_str("{}").expect("parse default");
        this.bans = BanConfig::filled_with(BanConfigValue {
            duration: BanConfigDuration(Duration::from_mins(5)),
            toleration: None,
        })
        .expect("test ban config is not valid");
        this.validate().expect("test moderator config is not valid");
        this
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.bans.validate()?;
        self.journal.ttl.validate()?;
        let max_duration = self.bans.max_duration();
        anyhow::ensure!(
            self.journal.ttl >= max_duration,
            "mempool journal ttl {:?} should not be less than \
             max duration of a ban with its toleration: {max_duration:?}",
            self.journal.ttl
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JournalConfig {
    /// Time to keep mempool events before deletion. Should be greater than any duration
    /// in ban config, otherwise bans cannot be reproduced after node restart.
    pub ttl: BanConfigDuration,

    #[serde(with = "tycho_util::serde_helpers::humantime")]
    pub clean_interval: Duration,

    #[serde(with = "tycho_util::serde_helpers::humantime")]
    pub batch_interval: Duration,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            ttl: BanConfigDuration(Duration::from_hours(30 * 24)),
            clean_interval: Duration::from_hours(1),
            batch_interval: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BanConfig(FastHashMap<EventTag, BanConfigValue>);
impl BanConfig {
    #[cfg(any(test, feature = "test"))]
    pub fn filled_with(value: BanConfigValue) -> anyhow::Result<Self> {
        let this = Self(FastHashMap::from_iter(
            EventTag::VALUES.map(|tag| (tag, value)),
        ));
        this.validate()?;
        Ok(this)
    }
    pub fn get(&self, tag: EventTag) -> &BanConfigValue {
        (self.0.get(&tag)).expect("ban config must be validated for exhaustiveness")
    }
    pub fn max_duration(&self) -> BanConfigDuration {
        (self.0.values())
            .map(|ban| match ban.toleration {
                None => ban.duration,
                Some(t) => BanConfigDuration(ban.duration.0 + t.duration.0),
            })
            .max()
            .expect("ban config must be validated to contain values")
    }
    pub fn validate(&self) -> anyhow::Result<()> {
        let missed = EventTag::VALUES
            .iter()
            .filter(|v| !self.0.contains_key(v))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            missed.is_empty(),
            "mempool ban config doesn't contain all entries: {missed:?}"
        );
        for value in self.0.values() {
            value.duration.validate()?;
            if let Some(toleration) = value.toleration {
                toleration.duration.validate()?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BanConfigValue {
    /// time to wait before unban
    pub duration: BanConfigDuration,
    /// to ban later than at first attempt; `None` to ban immediately
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

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BanConfigDuration(#[serde(with = "tycho_util::serde_helpers::humantime")] Duration);

impl std::fmt::Debug for BanConfigDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", humantime::format_duration(self.0))
    }
}

impl TryFrom<Duration> for BanConfigDuration {
    type Error = anyhow::Error;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        Self(value).validate().map(|()| Self(value))
    }
}

impl BanConfigDuration {
    pub fn to_time(self) -> UnixTime {
        UnixTime::from_millis(self.0.as_millis() as u64)
    }

    /// This type is intended for user input, so keep the value sane:
    /// someone may have mistaken "m" (minutes) for "M" (months) and so on.
    /// Also `impl fmt::Display for Rfc3339Timestamp` in `humantime` crate panics
    /// if the TS is after the end of year 9999 (the source of limitation is RFC itself).
    pub fn validate(self) -> anyhow::Result<()> {
        // border values are intentionally excluded as an easy misuse
        anyhow::ensure!(
            self.0 > Duration::from_mins(1),
            "duration must be greater than 1 minute, got {self:?}"
        );
        anyhow::ensure!(
            self.0 < Duration::from_hours(((4 * 365) + 1) * 24),
            "duration must be less than 4 years, got {self:?}"
        );
        Ok(())
    }
}

impl Default for BanConfig {
    fn default() -> BanConfig {
        let half_hour = BanConfigDuration(Duration::from_mins(30));
        let day = BanConfigDuration(Duration::from_hours(24));

        let five_min_immediate = BanConfigValue {
            duration: BanConfigDuration(Duration::from_mins(5)),
            toleration: None,
        };
        let day_immediate = BanConfigValue {
            duration: day,
            toleration: None,
        };

        Self(FastHashMap::from_iter([
            // someone maybe runs an old and incompatible version of the node
            (EventTag::BadQuery, five_min_immediate),
            // serious
            (EventTag::QueryLimitReached, day_immediate),
            (EventTag::PointIntegrityError, day_immediate),
            (EventTag::ReplacedPoint, day_immediate),
            (EventTag::IllFormedPoint, day_immediate),
            (EventTag::InvalidPoint, day_immediate),
            // arguable
            (EventTag::ForkedPoint, BanConfigValue {
                duration: half_hour,
                toleration: Some(BanToleration {
                    duration: day,
                    count: 3.try_into().unwrap(),
                }),
            }),
            (EventTag::EvidenceNoInclusion, BanConfigValue {
                duration: day,
                toleration: Some(BanToleration {
                    duration: day,
                    count: 5.try_into().unwrap(),
                }),
            }),
        ]))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn validate_default() -> anyhow::Result<()> {
        JournalConfig::default().ttl.validate()?;
        ModeratorConfig::default().validate()?;
        ModeratorConfig::test_default().validate()?;
        Ok(())
    }
}
