use serde::Serialize;
use tycho_util::{FastHashMap, FastHashSet};

use crate::models::UnixTime;
use crate::moderator::{BanConfig, EventTag, RecordKey};

#[derive(Default, Serialize)]
pub struct PeerEvents {
    latest_ban_until: Option<UnixTime>,
    events: FastHashMap<EventTag, EventHistory>,
}

#[derive(Default, Serialize)]
struct EventHistory {
    last_event_time: Option<UnixTime>,
    tolerated: FastHashSet<RecordKey>,
}

impl PeerEvents {
    /// Maintains only latest toleration window: will discard events in windows that have gone away
    /// since newest known (passed) event.
    /// It may return several successive values (for overlapping ranges) if events were fed in order.
    /// It may return at most one (latest) value if order of events was reversed.
    /// It will not return a value if [`PeerEvents::latest_ban_until`] is already up-to-date.
    /// Note: will not return a value retrospectively, i.e. if some ban could have been applied
    ///   from a window that is not the latest one.
    pub fn new_ban_until(
        &mut self,
        key: RecordKey,
        tag: EventTag,
        ban_config: &BanConfig,
    ) -> Option<UnixTime> {
        let conf = ban_config.get(tag);

        let current = self.events.entry(tag).or_default();

        let last_event_time = current
            .last_event_time
            .unwrap_or(key.created)
            .max(key.created);
        current.last_event_time = Some(last_event_time);

        match conf.toleration {
            None => {}
            Some(toleration) => {
                assert!(
                    current.tolerated.len() <= toleration.count.get() as usize,
                    "tolerated length must not exceed config value at the start"
                );

                let is_up_to_date = {
                    let oldest_bound = last_event_time - toleration.duration.to_time();
                    move |k: &RecordKey| oldest_bound <= k.created
                };

                if !is_up_to_date(&key) {
                    return None; // passed out-of-order, ignore
                }

                current.tolerated.insert(key);

                let mut min_key = key;

                current.tolerated.retain(|k| {
                    let keep = is_up_to_date(k);
                    if keep {
                        min_key = min_key.min(*k);
                    }
                    keep
                });

                match (current.tolerated.len()).cmp(&(toleration.count.get() as usize)) {
                    std::cmp::Ordering::Greater => {
                        let was_removed = current.tolerated.remove(&min_key);
                        assert!(was_removed, "min key must have been in map");
                        assert_eq!(
                            current.tolerated.len(),
                            toleration.count.get() as usize,
                            "tolerated length must be equal to config value after removal"
                        );
                    }
                    std::cmp::Ordering::Equal => {}
                    std::cmp::Ordering::Less => return None, // tolerated
                }
            }
        };

        let new_until = last_event_time + conf.duration.to_time();

        // Return new time only if it is strictly greater than previous
        self.latest_ban_until = Some(match self.latest_ban_until {
            Some(old) if old >= new_until => return None,
            None | Some(_) => new_until,
        });

        Some(new_until)
    }

    /// Returns `true` if new unban time is GEQ than previous
    pub fn reset_ban_until(&mut self, unban_at: UnixTime) -> bool {
        self.latest_ban_until = Some(match self.latest_ban_until {
            Some(old) if old > unban_at => return false,
            None | Some(_) => unban_at,
        });
        true
    }

    /// returns `false` if a newer ban exists and cannot be removed
    pub fn set_no_ban(&mut self, unban_at: UnixTime) -> bool {
        let no_newer_ban = self.latest_ban_until.is_none_or(|old| old <= unban_at);
        if no_newer_ban {
            self.latest_ban_until = None; // ok to replace None with None
        }
        no_newer_ban
    }
}
