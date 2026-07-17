use std::cmp::{Ordering, Reverse};

use rand::RngCore;
use tycho_network::PeerId;

use crate::intercom::peer_schedule::PeerState;
use crate::utils::ValueOrderedMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerStatus {
    pub state: OrderedPeerState,
    /// has uncompleted request just now
    pub is_in_flight: bool,
    last_attempt: Option<u32>,
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
            last_attempt: None,
            failed_queries: 0,
            is_independent: true, // `false` comes from channel to start immediate download
            rand: rand::rng().next_u32(),
        }
    }
    fn is_ready_to_query(&self, attempt: u32) -> bool {
        self.state.0 == PeerState::Resolved
            && !self.is_in_flight
            && self.last_attempt.is_none_or(|a| a < attempt)
    }
}

// keeps order unique
pub struct PeerQueue(ValueOrderedMap<PeerId, Reverse<PeerStatus>>);
impl PeerQueue {
    pub fn new<'a>(states: impl Iterator<Item = (&'a PeerId, &'a PeerState)>) -> Self {
        let mapped = states.map(|(peer_id, state)| (*peer_id, Reverse(PeerStatus::new(*state))));
        Self(mapped.collect())
    }

    pub fn take_to_flight(&mut self, attempt: u32) -> Option<PeerId> {
        let (&peer_id, &Reverse(mut status)) = self.0.max()?;
        if status.is_ready_to_query(attempt) {
            status.is_in_flight = true;
            status.last_attempt = Some(attempt);
            self.0.upsert(peer_id, Reverse(status), PartialEq::ne);
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
        let Some(&Reverse(mut status)) = self.0.get(peer_id) else {
            return false;
        };
        f(&mut status);
        self.0.upsert(*peer_id, Reverse(status), PartialEq::ne);
        true
    }

    pub fn remove(&mut self, peer_id: &PeerId) -> Option<PeerStatus> {
        self.0.remove(peer_id).map(|Reverse(status)| status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: u8) -> PeerId {
        PeerId([id; _])
    }

    fn queue(states: &[(u8, PeerState)]) -> PeerQueue {
        let iter = (states.iter()).map(|&(id, state)| {
            let status = PeerStatus {
                state: OrderedPeerState(state),
                is_in_flight: false,
                last_attempt: None,
                failed_queries: 0,
                is_independent: true,
                rand: u32::from(id),
            };
            (peer(id), Reverse(status))
        });
        PeerQueue(iter.collect())
    }

    fn retry(queue: &mut PeerQueue, peer_id: &PeerId) {
        assert!(queue.update(peer_id, |status| {
            status.is_in_flight = false;
            status.failed_queries += 1;
        }));
    }

    #[test]
    fn resolved_dependers_are_queried_before_independent_peers() {
        let mut queue = queue(&[
            (1, PeerState::Resolved),
            (2, PeerState::Resolved),
            (3, PeerState::Unknown),
        ]);
        let depender = peer(2);

        assert!(queue.update(&depender, |status| status.is_independent = false));

        assert_eq!(queue.take_to_flight(0), Some(depender));
        assert_eq!(queue.take_to_flight(0), Some(peer(1)));
        assert_eq!(queue.take_to_flight(0), None);
    }

    #[test]
    fn retries_rotate_peers_by_last_attempt() {
        let mut queue = queue(&[(1, PeerState::Resolved), (2, PeerState::Resolved)]);
        let first = peer(1);
        let second = peer(2);

        assert_eq!(queue.take_to_flight(0), Some(first));
        retry(&mut queue, &first);
        assert_eq!(queue.take_to_flight(0), Some(second));
        retry(&mut queue, &second);
        assert_eq!(queue.take_to_flight(0), None);

        assert_eq!(queue.take_to_flight(1), Some(first));
        retry(&mut queue, &first);
        assert_eq!(queue.take_to_flight(1), Some(second));
    }

    #[test]
    fn resolved_state_update_makes_peer_queryable() {
        let mut queue = queue(&[(1, PeerState::Unknown)]);
        let peer_id = peer(1);

        assert_eq!(queue.take_to_flight(0), None);
        assert!(queue.update(&peer_id, |status| {
            status.state = OrderedPeerState(PeerState::Resolved);
        }));
        assert_eq!(queue.take_to_flight(0), Some(peer_id));
    }
}
