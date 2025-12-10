use std::cmp::{Ordering, Reverse};

use rand::RngCore;
use tycho_network::PeerId;

use crate::intercom::dependency::value_ordered_map::ValueOrderedMap;
use crate::intercom::peer_schedule::PeerState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerStatus {
    pub state: OrderedPeerState,
    /// has uncompleted request just now
    pub is_in_flight: bool,
    pub failed_queries: usize,
    /// `false` for peers that depend on current point, i.e. included it directly;
    /// those have more priority than independent ones which may not have the point
    pub is_independent: bool,
    // once assign random order position and hide from usage
    rand: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedPeerState(pub PeerState);
impl PartialOrd for OrderedPeerState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedPeerState {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0, other.0) {
            (PeerState::Resolved, PeerState::Unknown) => Ordering::Less,
            (PeerState::Unknown, PeerState::Resolved) => Ordering::Greater,
            (PeerState::Resolved, PeerState::Resolved)
            | (PeerState::Unknown, PeerState::Unknown) => Ordering::Equal,
        }
    }
}

impl PeerStatus {
    fn new(state: PeerState) -> Self {
        Self {
            state: OrderedPeerState(state),
            is_in_flight: false,
            failed_queries: 0,
            is_independent: true, // `false` comes from channel to start immediate download
            rand: rand::rng().next_u32(),
        }
    }
    fn is_ready_to_query(&self) -> bool {
        self.state.0 == PeerState::Resolved && !self.is_in_flight
    }
}

// keeps order unique
pub struct PeerQueue(ValueOrderedMap<PeerId, Reverse<PeerStatus>>);
impl PeerQueue {
    pub fn new<'a>(states: impl Iterator<Item = (&'a PeerId, &'a PeerState)>) -> Self {
        let mapped = states.map(|(peer_id, state)| (*peer_id, Reverse(PeerStatus::new(*state))));
        Self(mapped.collect())
    }

    pub fn take_to_flight(&mut self) -> Option<PeerId> {
        let (&peer_id, &Reverse(mut status)) = self.0.max()?;
        if status.is_ready_to_query() {
            status.is_in_flight = true;
            self.0.upsert(peer_id, Reverse(status), PartialEq::ne).ok();
            Some(peer_id)
        } else {
            None
        }
    }

    /// returns `false` in case peer was not found
    #[must_use = "all peers must be known"]
    pub fn update<F>(&mut self, peer_id: &PeerId, f: F) -> bool
    where
        F: FnOnce(&mut PeerStatus),
    {
        let Some(&Reverse(mut status)) = self.0.inner().get(peer_id) else {
            return false;
        };
        f(&mut status);
        self.0.upsert(*peer_id, Reverse(status), PartialEq::ne).ok();
        true
    }

    pub fn remove(&mut self, peer_id: &PeerId) -> Option<PeerStatus> {
        self.0.remove(peer_id).map(|Reverse(status)| status)
    }
}
