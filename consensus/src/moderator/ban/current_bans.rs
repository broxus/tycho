use std::collections::hash_map;

use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::moderator::ban::CurrentBan;

#[derive(Default)]
pub struct CurrentBans(FastHashMap<PeerId, CurrentBan>);

impl CurrentBans {
    /// * returns `Some(is_first)` if stored a new value that ends later than previous
    ///   * `true` if the peer was not previously banned
    ///   * `false` if the peer already has a ban entry
    /// * returns `None` if the peer has a ban entry with a further ending
    #[must_use]
    pub fn upsert(&mut self, peer_id: &PeerId, q_ban: CurrentBan) -> Option<bool> {
        match self.0.entry(*peer_id) {
            hash_map::Entry::Occupied(mut occupied) => {
                // cannot reset ban to an EQ value: otherwise we may place a new item
                // into the channel to the delay queue with a duplicated `until` value
                // that will fire the unban once, and a new item the second time (or panic)
                if q_ban.until > occupied.get().until {
                    occupied.insert(q_ban);
                    Some(false)
                } else {
                    None
                }
            }
            hash_map::Entry::Vacant(vacant) => {
                vacant.insert(q_ban);
                Some(true)
            }
        }
    }

    /// * returns `true` if value was found and removed
    #[must_use]
    pub fn remove(&mut self, peer_id: &PeerId) -> bool {
        self.0.remove(peer_id).is_some()
    }

    pub fn inner(&self) -> &FastHashMap<PeerId, CurrentBan> {
        &self.0
    }
}
