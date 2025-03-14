use std::mem;
use std::sync::Arc;

use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::intercom::dto::PeerState;
use crate::models::Round;

#[derive(Debug)]
pub struct PeerScheduleStateful {
    // whole validator sets except local id for peer resolver
    validator_set: [FastHashMap<PeerId, PeerState>; 3],
    // working subset
    active_subset: [Arc<FastHashMap<PeerId, PeerState>>; 3],
    prev_epoch_start: Round,
    pub(super) cur_epoch_start: Round,
    pub(super) next_epoch_start: Option<Round>,
    empty: Arc<FastHashMap<PeerId, PeerState>>,
    // resolved peers from current or next subset
    broadcast_receivers: FastHashSet<PeerId>,
    all_resolved: FastHashSet<PeerId>,
}

impl Default for PeerScheduleStateful {
    fn default() -> Self {
        Self {
            validator_set: Default::default(),
            active_subset: Default::default(),
            prev_epoch_start: Round::BOTTOM,
            cur_epoch_start: Round::BOTTOM,
            next_epoch_start: None,
            empty: Default::default(),
            broadcast_receivers: Default::default(),
            all_resolved: Default::default(),
        }
    }
}

impl PeerScheduleStateful {
    /// local peer id is always kept as not resolved, so always excluded from result
    pub fn broadcast_receivers(&self) -> &FastHashSet<PeerId> {
        &self.broadcast_receivers
    }

    /// local peer id is always kept as not resolved
    pub fn peers_state_for(&self, round: Round) -> &'_ Arc<FastHashMap<PeerId, PeerState>> {
        let result = if self.next_epoch_start.is_some_and(|r| round >= r) {
            &self.active_subset[2]
        } else if round >= self.cur_epoch_start {
            &self.active_subset[1]
        } else if round >= self.prev_epoch_start {
            &self.active_subset[0]
        } else {
            &self.empty
        };
        if result.is_empty() {
            tracing::error!(
                "empty peer set for {round:?}; epoch starts: prev={} curr={} next={:?}",
                self.prev_epoch_start.0,
                self.cur_epoch_start.0,
                self.next_epoch_start.map(|r| r.0)
            );
        }
        result
    }

    /// local peer id is always kept as not resolved
    pub fn peer_state(&self, peer_id: &PeerId) -> PeerState {
        self.all_resolved
            .get(peer_id)
            .map_or(PeerState::Unknown, |_| PeerState::Resolved)
    }

    /// Returns [true] if should notify other tasks
    /// Always keeps local id as [`PeerState::Unknown`]
    pub(super) fn set_state(&mut self, peer_id: &PeerId, state: PeerState) -> bool {
        let mut is_applied = false;
        let mut is_broadcast_receiver = false;
        for whole_set in &mut self.validator_set {
            if let Some(peer_state) = whole_set.get_mut(peer_id) {
                if *peer_state != state {
                    *peer_state = state;
                }
            }
        }
        for i in 0..self.active_subset.len() {
            if self.active_subset[i]
                .get(peer_id)
                .is_some_and(|old| *old != state)
            {
                Arc::make_mut(&mut self.active_subset[i])
                    .entry(*peer_id)
                    .and_modify(|old| *old = state);
                is_applied = true;
                is_broadcast_receiver |= i != 0;
            }
        }
        if is_applied {
            if is_broadcast_receiver {
                match state {
                    PeerState::Unknown => _ = self.broadcast_receivers.remove(peer_id),
                    PeerState::Resolved => _ = self.broadcast_receivers.insert(*peer_id),
                }
                meter_bcast_receivers(self.broadcast_receivers.len());
            }
            match state {
                PeerState::Unknown => _ = self.all_resolved.remove(peer_id),
                PeerState::Resolved => _ = self.all_resolved.insert(*peer_id),
            }
            meter_all_resolved(self.all_resolved.len());
        }
        is_applied
    }

    /// returns outdated peers that should not be resolved anymore
    #[must_use]
    pub(super) fn set_next_validator_set(
        &mut self,
        peers: &[PeerId],
        all_resolved: FastHashSet<PeerId>,
    ) -> Vec<PeerId> {
        let validator_set = peers
            .iter()
            .map(|peer_id| {
                let state = if all_resolved.contains(peer_id) {
                    PeerState::Resolved
                } else {
                    PeerState::Unknown
                };
                (*peer_id, state)
            })
            .collect::<FastHashMap<_, _>>();
        let to_forget = self.validator_set[2]
            .iter()
            .filter(|(peer, _)| {
                !(self.validator_set[0].contains_key(peer)
                    || self.validator_set[1].contains_key(peer)
                    || validator_set.contains_key(peer))
            })
            .map(|(peer_id, _)| *peer_id)
            .collect();
        self.validator_set[2] = validator_set;
        self.all_resolved = all_resolved;
        meter_all_resolved(self.all_resolved.len());
        for peer in &to_forget {
            self.broadcast_receivers.remove(peer);
        }
        meter_bcast_receivers(self.broadcast_receivers.len());
        to_forget
    }

    pub(super) fn set_next_subset(&mut self, peers: &[PeerId]) {
        let peers_state = peers
            .iter()
            .map(|peer_id| {
                let state = if self.all_resolved.contains(peer_id) {
                    PeerState::Resolved
                } else {
                    PeerState::Unknown
                };
                (*peer_id, state)
            })
            .collect::<FastHashMap<_, _>>();
        self.active_subset[2] = Arc::new(peers_state);
        self.broadcast_receivers = self.active_subset[1]
            .iter()
            .chain(self.active_subset[2].iter())
            .filter(|(_, state)| **state == PeerState::Resolved)
            .map(|(peer_id, _)| *peer_id)
            .collect();
        meter_bcast_receivers(self.broadcast_receivers.len());
    }

    /// on epoch change
    pub(super) fn rotate(&mut self) {
        assert!(
            self.validator_set[0].is_empty() && self.active_subset[0].is_empty(),
            "previous peer set was not cleaned {self:?}"
        );

        // make next from previous
        let next = self
            .next_epoch_start
            .ok_or_else(|| format!("{self:?}"))
            .expect("attempt to change epoch, but next epoch start is not set");

        assert!(
            next > self.cur_epoch_start,
            "next start is not in future {self:?}"
        );

        self.prev_epoch_start = self.cur_epoch_start;
        self.cur_epoch_start = next;
        self.next_epoch_start = None; // makes next epoch peers inaccessible for reads

        self.validator_set.rotate_left(1);
        self.active_subset.rotate_left(1);
    }

    /// on epoch change
    #[must_use]
    pub(super) fn forget_previous(&mut self) -> Vec<PeerId> {
        let mut to_forget = Vec::new();
        for (peer_id, state) in mem::take(&mut self.validator_set[0]).iter() {
            if !self.validator_set[1].contains_key(peer_id)
                && !self.validator_set[2].contains_key(peer_id)
            {
                to_forget.push(*peer_id);
                if *state == PeerState::Resolved {
                    self.all_resolved.remove(peer_id);
                    self.broadcast_receivers.remove(peer_id);
                }
            }
        }
        meter_all_resolved(self.all_resolved.len());
        meter_bcast_receivers(self.broadcast_receivers.len());
        self.active_subset[0] = Default::default();
        to_forget
    }
}

fn meter_all_resolved(len: usize) {
    metrics::gauge!("tycho_mempool_peers_resolved").set(len as u32);
}
fn meter_bcast_receivers(len: usize) {
    metrics::gauge!("tycho_mempool_bcast_receivers").set(len as u32);
}
