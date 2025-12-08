use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use rand::RngCore;
use tycho_network::PeerId;
use tycho_util::FastHashMap;

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
pub struct PeerQueue {
    statuses: FastHashMap<PeerId, PeerStatus>,
    min_heap: BinaryHeap<(Reverse<PeerStatus>, PeerId)>,
}
impl PeerQueue {
    pub fn new<'a>(states: impl Iterator<Item = (&'a PeerId, &'a PeerState)>) -> Self {
        let statuses = states
            .map(|(peer_id, state)| (*peer_id, PeerStatus::new(*state)))
            .collect::<FastHashMap<_, _>>();
        let mut orders = Vec::with_capacity(statuses.len());
        for (peer_id, status) in &statuses {
            orders.push((Reverse(*status), *peer_id));
        }
        Self {
            statuses,
            min_heap: BinaryHeap::from(orders),
        }
    }

    pub fn take_to_flight(&mut self) -> Option<PeerId> {
        let (Reverse(status), peer_id) = &mut *self.min_heap.peek_mut()?;
        if !status.is_ready_to_query() {
            return None;
        }
        status.is_in_flight = true;
        let peer_id = *peer_id;
        (self.statuses.get_mut(&peer_id))
            .expect("must be in both collections")
            .is_in_flight = true;
        Some(peer_id)
    }

    /// returns `false` in case peer was not found
    #[must_use = "all peers must be known"]
    pub fn update<F>(&mut self, peer_id: &PeerId, f: F) -> bool
    where
        F: FnOnce(&mut PeerStatus),
    {
        let Some(status) = self.statuses.get_mut(peer_id) else {
            return false;
        };
        let old = *status;
        f(status);
        let new = *status;
        if old == new {
            return true;
        }
        // TODO impl over IndexMap has the same O(N + logN), but may be a bit more efficient
        self.min_heap.retain(|(_, p_id)| p_id != peer_id);
        self.min_heap.push((Reverse(new), *peer_id));
        true
    }

    pub fn remove(&mut self, peer_id: &PeerId) -> Option<PeerStatus> {
        let old = self.statuses.remove(peer_id)?;
        self.min_heap.retain(|(_, p_id)| p_id != peer_id);
        Some(old)
    }
}
