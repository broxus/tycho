use std::mem;
use std::sync::Arc;

use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::intercom::peer_schedule::PeerState;
use crate::intercom::peer_schedule::epoch_starts::EpochStarts;
use crate::models::Round;

#[derive(Default, Debug)]
pub struct PeerScheduleStateful {
    // whole validator sets except local id for peer resolver
    validator_set: [FastHashMap<PeerId, PeerState>; 4],
    // working subset
    active_subset: [Arc<FastHashMap<PeerId, PeerState>>; 4],
    pub(super) epoch_starts: EpochStarts,
    empty: Arc<FastHashMap<PeerId, PeerState>>,
    // resolved peers from current or next subset
    broadcast_receivers: FastHashSet<PeerId>,
    all_resolved: FastHashSet<PeerId>,
    pub(super) to_forget: Vec<PeerId>,
}

impl PeerScheduleStateful {
    /// local peer id is always kept as not resolved, so always excluded from result;
    /// a check disregard state: [`super::stateless::PeerScheduleStateless::is_in_active_v_subset`]
    pub fn broadcast_receivers(&self) -> &FastHashSet<PeerId> {
        &self.broadcast_receivers
    }

    pub(super) fn is_in_any_vset(&self, peer_id: &PeerId) -> bool {
        (self.validator_set.iter()).any(|vset| vset.contains_key(peer_id))
    }

    /// local peer id is always kept as not resolved
    pub fn peers_state_for(&self, round: Round) -> &Arc<FastHashMap<PeerId, PeerState>> {
        let result =
            (self.epoch_starts.arr_idx(round)).map_or(&self.empty, |i| &self.active_subset[i]);
        if result.is_empty() {
            tracing::error!("empty peer set for {round:?}; {:?}", self.epoch_starts);
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
            if let Some(peer_state) = whole_set.get_mut(peer_id)
                && *peer_state != state
            {
                *peer_state = state;
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
                is_broadcast_receiver |= i >= 2;
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
        let to_forget = self.validator_set[3]
            .iter()
            .filter(|(peer, _)| {
                !(self.validator_set[0].contains_key(peer)
                    || self.validator_set[1].contains_key(peer)
                    || self.validator_set[2].contains_key(peer)
                    || validator_set.contains_key(peer))
            })
            .map(|(peer_id, _)| *peer_id)
            .collect::<Vec<_>>();
        self.validator_set[3] = validator_set;
        self.all_resolved = all_resolved;
        meter_all_resolved(self.all_resolved.len());
        for peer in &to_forget {
            self.broadcast_receivers.remove(peer);
        }
        meter_bcast_receivers(self.broadcast_receivers.len());
        self.to_forget.extend(to_forget.iter().copied());
        to_forget
    }

    pub(super) fn set_next_subset(&mut self, peers: &[(PeerId, u16)]) {
        let peers_state = peers
            .iter()
            .map(|(peer_id, _)| {
                let state = if self.all_resolved.contains(peer_id) {
                    PeerState::Resolved
                } else {
                    PeerState::Unknown
                };
                (*peer_id, state)
            })
            .collect::<FastHashMap<_, _>>();
        self.active_subset[3] = Arc::new(peers_state);
        self.broadcast_receivers = self.active_subset[2]
            .iter()
            .chain(self.active_subset[3].iter())
            .filter(|(_, state)| **state == PeerState::Resolved)
            .map(|(peer_id, _)| *peer_id)
            .collect();
        meter_bcast_receivers(self.broadcast_receivers.len());
    }

    /// on epoch change
    pub(super) fn rotate(&mut self) {
        assert!(
            self.validator_set[0].is_empty() && self.active_subset[0].is_empty(),
            "oldest peer set was not cleaned {self:?}"
        );

        self.epoch_starts.rotate();

        self.validator_set.rotate_left(1);
        self.active_subset.rotate_left(1);
    }

    /// on epoch change
    #[must_use]
    pub(super) fn forget_oldest(&mut self) -> Vec<PeerId> {
        let mut to_forget = Vec::new();
        for (peer_id, state) in mem::take(&mut self.validator_set[0]).iter() {
            if !self.validator_set[1].contains_key(peer_id)
                && !self.validator_set[2].contains_key(peer_id)
                && !self.validator_set[3].contains_key(peer_id)
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
        self.to_forget.extend(to_forget.iter().copied());
        to_forget
    }
}

fn meter_all_resolved(len: usize) {
    metrics::gauge!("tycho_mempool_peers_resolved").set(len as u32);
}
fn meter_bcast_receivers(len: usize) {
    metrics::gauge!("tycho_mempool_bcast_receivers").set(len as u32);
}
