use serde::Serialize;
use tycho_util::{FastHashMap, FastHashSet};

use crate::models::UnixTime;
use crate::moderator::{BanConfig, EventTag, RecordKey};

#[derive(Default, Serialize)]
pub struct PeerEvents {
    events: FastHashMap<EventTag, EventHistory>,
}

#[derive(Default, Serialize)]
struct EventHistory {
    last_event_time: Option<UnixTime>,
    tolerated: FastHashSet<RecordKey>,
}

impl PeerEvents {
    /// Maintains only latest toleration window: will discard events in windows that have gone away
    /// since latest known (passed) event.
    /// * returns `Some` when the current event is the first one beyond the tolerated count
    #[must_use]
    pub fn maybe_ban_until(
        &mut self,
        key: &RecordKey,
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

                if !is_up_to_date(key) {
                    return None; // passed out-of-order, ignore
                }

                current.tolerated.insert(*key);

                let mut min_key = *key;

                current.tolerated.retain(|k| {
                    let keep = is_up_to_date(k);
                    if keep {
                        min_key = min_key.min(*k);
                    }
                    keep
                });

                if current.tolerated.len() > toleration.count.get() as usize {
                    let was_removed = current.tolerated.remove(&min_key);
                    assert!(was_removed, "min key must have been in map");
                    assert_eq!(
                        current.tolerated.len(),
                        toleration.count.get() as usize,
                        "tolerated length must be equal to config value after removal"
                    );
                } else {
                    return None;
                }
            }
        };

        Some(last_event_time + conf.duration.to_time())
    }
}
