use tycho_util::{FastHashMap, FastHashSet};

use crate::engine::NodeConfig;
use crate::models::UnixTime;
use crate::moderator::RecordKey;
use crate::moderator::stored::EventTag;

#[derive(Default)]
pub struct ByPeer {
    latest_ban_until: Option<UnixTime>,
    by_tag: FastHashMap<EventTag, ByTag>,
}

#[derive(Default)]
struct ByTag {
    last_event_time: Option<UnixTime>,
    tolerated: FastHashSet<RecordKey>,
}

impl ByPeer {
    /// Maintains only latest toleration window: will discard events in windows that have gone away
    /// since newest known (passed) event.
    /// It may return several successive values (for overlapping ranges) if events were fed in order.
    /// It may return at most one (latest) value if order of events was reversed.
    /// It will not return a value if [`ByPeer::latest_ban_until`] is already up-to-date.
    /// Note: will not return a value retrospectively, i.e. if some ban could have been applied
    ///   from a window that is not the latest one.
    pub fn new_ban_until(&mut self, key: RecordKey, tag: EventTag) -> Option<UnixTime> {
        let conf = NodeConfig::get().bans.get(tag);

        let current = self.by_tag.entry(tag).or_default();

        let last_event_time = *current.last_event_time.iter().fold(&key.created, Ord::max);
        current.last_event_time = Some(last_event_time);

        match conf.toleration {
            None => {}
            Some(toleration) => {
                let is_up_to_date = {
                    let oldest_bound = last_event_time - toleration.duration.to_time();
                    move |k: &RecordKey| oldest_bound <= k.created
                };

                if !is_up_to_date(&key) {
                    return None; // passed out-of-order most likely on init, ignore
                }

                current.tolerated.insert(key);

                current.tolerated.retain(is_up_to_date);

                if current.tolerated.len() < toleration.count.get() as usize {
                    return None; // tolerated
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
    pub fn remove_ban(&mut self) {
        self.latest_ban_until = None;
    }
}
