use std::mem;
use std::sync::Arc;

use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::intercom::dto::PeerState;
use crate::models::Round;

#[derive(Debug)]
pub struct PeerScheduleStateful {
    // order to select leader by coin flip
    peers_state: [Arc<FastHashMap<PeerId, PeerState>>; 3],
    prev_epoch_start: Round,
    pub(super) cur_epoch_start: Round,
    pub(super) next_epoch_start: Option<Round>,
    empty: Arc<FastHashMap<PeerId, PeerState>>,
    // resolved peers from current or next epoch
    broadcast_receivers: FastHashSet<PeerId>,
    all_resolved: FastHashSet<PeerId>,
}

impl Default for PeerScheduleStateful {
    fn default() -> Self {
        Self {
            peers_state: Default::default(),
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
        let result = if self.next_epoch_start.map_or(false, |r| round >= r) {
            &self.peers_state[2]
        } else if round >= self.cur_epoch_start {
            &self.peers_state[1]
        } else if round >= self.prev_epoch_start {
            &self.peers_state[0]
        } else {
            &self.empty
        };
        if result.is_empty() {
            tracing::error!("empty peer set for {round:?}: {self:?}");
        }
        result
    }

    /// local peer id is always kept as not resolved
    pub fn peer_state(&self, peer_id: &PeerId) -> PeerState {
        self.all_resolved
            .get(peer_id)
            .map_or(PeerState::Unknown, |_| PeerState::Resolved)
    }

    /// Returns [true] if update was successfully applied.
    /// Always keeps local id as [`PeerState::Unknown`]
    pub(super) fn set_state(&mut self, peer_id: &PeerId, state: PeerState) -> bool {
        let mut is_applied = false;
        let mut is_broadcast_receiver = false;
        for i in 0..self.peers_state.len() {
            if self.peers_state[i]
                .get(peer_id)
                .map_or(false, |old| *old != state)
            {
                Arc::make_mut(&mut self.peers_state[i])
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
            }
            match state {
                PeerState::Unknown => _ = self.all_resolved.remove(peer_id),
                PeerState::Resolved => _ = self.all_resolved.insert(*peer_id),
            }
        }
        is_applied
    }

    pub(super) fn set_next_peers(&mut self, peers: &[PeerId], all_resolved: FastHashSet<PeerId>) {
        let peers_state = peers
            .iter()
            .map(|peer_id| {
                (
                    *peer_id,
                    if all_resolved.contains(peer_id) {
                        PeerState::Resolved
                    } else {
                        PeerState::Unknown
                    },
                )
            })
            .collect::<FastHashMap<_, _>>();
        self.peers_state[2] = Arc::new(peers_state);
        self.all_resolved = all_resolved;
        self.broadcast_receivers = self.peers_state[1]
            .iter()
            .chain(self.peers_state[2].iter())
            .filter(|(_, state)| **state == PeerState::Resolved)
            .map(|(peer_id, _)| *peer_id)
            .collect();
    }

    /// on epoch change
    pub(super) fn rotate(&mut self) {
        assert!(
            self.peers_state[0].is_empty(),
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

        self.peers_state.rotate_left(1);
    }

    /// on epoch change
    #[must_use]
    pub(super) fn forget_previous(&mut self) -> Vec<PeerId> {
        let mut to_forget = Vec::new();
        for (peer_id, state) in mem::take(&mut self.peers_state[0]).iter() {
            if !self.peers_state[1].contains_key(peer_id)
                && !self.peers_state[2].contains_key(peer_id)
            {
                to_forget.push(*peer_id);
                if *state == PeerState::Resolved {
                    self.all_resolved.remove(peer_id);
                    self.broadcast_receivers.remove(peer_id);
                }
            }
        }
        to_forget
    }
}
