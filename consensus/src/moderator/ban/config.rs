use std::num::NonZeroU16;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::FastHashMap;

use crate::models::UnixTime;
use crate::moderator::stored::EventTag;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BanConfig(FastHashMap<EventTag, BanConfigValue>);
impl BanConfig {
    pub fn validate(&self, event_journal_ttl: JournalTtl) -> anyhow::Result<()> {
        let missed = EventTag::VALUES
            .iter()
            .filter(|v| !self.0.contains_key(v))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            missed.is_empty(),
            "mempool ban config doesn't contain all entries: {missed:?}"
        );
        let min_duration = (self.0.values())
            .flat_map(|ban| std::iter::once(ban.duration).chain(ban.toleration.map(|t| t.duration)))
            .min()
            .expect("ban config must be validated to contain values");
        // because ban-unban-resolve is not fast
        anyhow::ensure!(
            min_duration.0 >= Duration::from_secs(60),
            "mempool ban config duration should not be less than 1 minute, got: {min_duration:?}"
        );
        let max_duration = self.max_duration();
        anyhow::ensure!(
            event_journal_ttl.0 >= max_duration,
            "mempool event journal ttl {event_journal_ttl:?} \
             should not be less than max duration of a ban with its toleration: {max_duration:?}"
        );
        Ok(())
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

impl BanConfigDuration {
    pub fn to_time(self) -> UnixTime {
        UnixTime::from_millis(self.0.as_millis() as u64)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JournalTtl(pub BanConfigDuration);
impl Default for JournalTtl {
    fn default() -> Self {
        let month = Duration::from_secs(30 * 24 * 60 * 60);
        Self(BanConfigDuration(month))
    }
}

impl Default for BanConfig {
    fn default() -> BanConfig {
        let half_hour = BanConfigDuration(Duration::from_secs(30 * 60));
        let day = BanConfigDuration(Duration::from_secs(24 * 60 * 60));

        let five_min_immediate = BanConfigValue {
            duration: BanConfigDuration(Duration::from_secs(5 * 60)),
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
            (EventTag::IllFormed, day_immediate),
            (EventTag::Invalid, day_immediate),
            // arguable
            (EventTag::Equivocated, BanConfigValue {
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

#[cfg(any(test, feature = "test"))]
impl BanConfig {
    pub fn test_default() -> Self {
        let hour_immediate = BanConfigValue {
            duration: BanConfigDuration(Duration::from_secs(5 * 60)),
            toleration: None,
        };
        Self(FastHashMap::from_iter(
            EventTag::VALUES.map(|tag| (tag, hour_immediate)),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn validate_default() -> anyhow::Result<()> {
        BanConfig::default().validate(JournalTtl::default())
    }
}
