use std::array;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use std::sync::Arc;

use ahash::RandomState;
use parking_lot::Mutex;
use tokio::sync::broadcast::error::RecvError;

use tycho_network::{PeerId, PrivateOverlay, PrivateOverlayEntriesEvent};
use tycho_util::FastHashMap;

use crate::engine::node_count::NodeCount;
use crate::models::point::Round;

/*
    As validators are elected for wall-clock time range,
    the round of validator set switch is not known beforehand
    and will be determined by the time in anchor vertices:
    it must reach some predefined time range,
    when new set is supposed to be online and begin to request points,
    and a (relatively high) predefined number of support rounds must follow
    for the anchor chain to be committed by majority and for new nodes to gather data.
    The switch will occur for validator sets as a whole, at a single leaderless round.
*/
#[derive(Clone)]
pub struct PeerSchedule {
    // FIXME determine if our local_id is in next epoch
    inner: Arc<Mutex<PeerScheduleInner>>,
    overlay: PrivateOverlay,
    pub local_id: PeerId, // FIXME move into schedule when it starts to change with new epoch
}

impl PeerSchedule {
    pub fn new(
        current_epoch_start: Round,
        current_peers: &Vec<PeerId>,
        overlay: &PrivateOverlay,
        local_id: &PeerId,
    ) -> Self {
        let mut current_peers = current_peers.clone();
        current_peers.retain(|p| p != local_id);
        let this = Self {
            inner: Arc::new(Mutex::new(PeerScheduleInner::new(
                current_epoch_start,
                &current_peers,
            ))),
            overlay: overlay.clone(),
            local_id: local_id.clone(),
        };
        tokio::spawn(this.clone().listen());
        this
    }

    // To sign a point or to query for points, we need to know the intersection of:
    // * which nodes are in the validator set during the round of interest
    // * which nodes are able to connect at the moment
    /// TODO replace bool with AtomicBool? use Arc<FastDashMap>? to return map with auto refresh
    pub async fn wait_for_peers(&self, round: Round, node_count: NodeCount) {
        let mut rx = self.overlay.read_entries().subscribe();
        let mut peers = (*self.peers_for(round)).clone();
        let mut count = peers.iter().filter(|(_, &is_resolved)| is_resolved).count();
        while count < node_count.into() {
            match rx.recv().await {
                Ok(PrivateOverlayEntriesEvent::Resolved(peer_id)) if peer_id != self.local_id => {
                    if let Some(resolved) = peers.get_mut(&peer_id) {
                        if !*resolved {
                            count += 1;
                        }
                        *resolved = true;
                    }
                }
                Ok(PrivateOverlayEntriesEvent::Removed(peer_id)) if peer_id != self.local_id => {
                    if let Some(resolved) = peers.get_mut(&peer_id) {
                        if *resolved {
                            count -= 1;
                        }
                        *resolved = false;
                    }
                }
                _ => {}
            }
        }
    }

    pub fn peers_for(&self, round: Round) -> Arc<BTreeMap<PeerId, bool>> {
        let mut inner = self.inner.lock();
        inner.peers_for_index_plus_one(inner.index_plus_one(round))
    }

    pub fn peers_for_array<const N: usize>(
        &self,
        rounds: [Round; N],
    ) -> [Arc<BTreeMap<PeerId, bool>>; N] {
        let mut inner = self.inner.lock();
        array::from_fn(|i| inner.peers_for_index_plus_one(inner.index_plus_one(rounds[i])))
    }

    /// does not return empty maps
    pub fn peers_for_range(&self, rounds: Range<Round>) -> Vec<Arc<BTreeMap<PeerId, bool>>> {
        if rounds.end <= rounds.start {
            return vec![];
        }
        let mut inner = self.inner.lock();
        let mut first = inner.index_plus_one(rounds.start);
        let last = inner.index_plus_one(rounds.end.prev());
        if 0 == first && first < last {
            first += 1; // exclude inner.empty
        }
        (first..=last)
            .into_iter()
            .map(|i| inner.peers_for_index_plus_one(i))
            .filter(|m| !m.is_empty())
            .collect()
    }

    /// on epoch change
    pub fn rotate(&self) {
        // make next from previous
        let mut inner = self.inner.lock();
        let Some(next) = inner.next_epoch_start else {
            let msg = "Fatal: attempt to change epoch, but next epoch start is not set";
            tracing::error!("{msg}");
            panic!("{msg}");
        };
        inner.prev_epoch_start = inner.cur_epoch_start;
        inner.cur_epoch_start = next;
        inner.next_epoch_start = None;

        if !inner.peers_resolved[0].is_empty() {
            Arc::make_mut(&mut inner.peers_resolved[0]).clear();
        }
        inner.peers_resolved.rotate_left(1);
    }

    /// after successful sync to current epoch
    /// and validating all points from previous peer set
    /// free some memory and ignore overlay updates
    pub fn forget_previous(&self) {
        let mut inner = self.inner.lock();
        if !inner.peers_resolved[0].is_empty() {
            Arc::make_mut(&mut inner.peers_resolved[0]).clear();
        }
    }

    pub fn set_next_start(&self, round: Round) {
        let mut inner = self.inner.lock();
        _ = inner.next_epoch_start.replace(round);
    }

    pub fn set_next_peers(&self, peers: &Vec<PeerId>) {
        let mut inner = self.inner.lock();
        let next = inner.peers_resolved[2].as_ref();
        let old = peers
            .iter()
            .filter_map(|p| next.get(p).map(|b| (p.clone(), *b)))
            .collect::<Vec<_>>();
        let mut next = Arc::make_mut(&mut inner.peers_resolved[2]);
        next.clear();
        next.extend(peers.clone().into_iter().map(|a| (a, false)));
        next.extend(old);
    }

    /// Returns [true] if update was successfully applied
    fn set_resolved(&self, node: &PeerId, resolved: bool) -> bool {
        let mut is_applied = false;
        let mut inner = self.inner.lock();
        for i in 0..inner.peers_resolved.len() {
            let Some(b) = Arc::make_mut(&mut inner.peers_resolved[i]).get_mut(node) else {
                continue;
            };
            *b = resolved;
            is_applied = true;
        }
        is_applied
    }

    async fn listen(self) {
        let mut rx = self.overlay.read_entries().subscribe();
        loop {
            match rx.recv().await {
                Ok(ref event @ PrivateOverlayEntriesEvent::Resolved(node))
                    if node != self.local_id =>
                {
                    if !self.set_resolved(&node, true) {
                        tracing::debug!("Skipped {event:?}");
                    }
                }
                Ok(ref event @ PrivateOverlayEntriesEvent::Removed(node))
                    if node != self.local_id =>
                {
                    if !self.set_resolved(&node, true) {
                        tracing::debug!("Skipped {event:?}");
                    }
                }
                Err(RecvError::Closed) => {
                    let msg = "Fatal: peer info updates channel closed, \
                         cannot maintain node connectivity";
                    tracing::error!(msg);
                    panic!("{msg}")
                }
                Err(RecvError::Lagged(qnt)) => {
                    tracing::warn!(
                        "Skipped {qnt} peer info updates, node connectivity may suffer. \
                         Consider increasing channel capacity."
                    )
                }
                Ok(_) => {}
            }
        }
    }
}

pub struct PeerScheduleInner {
    // order to select leader by coin flip
    peers_resolved: [Arc<BTreeMap<PeerId, bool>>; 3],
    prev_epoch_start: Round,
    cur_epoch_start: Round,
    next_epoch_start: Option<Round>,
    empty: Arc<BTreeMap<PeerId, bool>>,
}

impl PeerScheduleInner {
    fn new(current_epoch_start: Round, current_peers: &Vec<PeerId>) -> Self {
        Self {
            peers_resolved: [
                Default::default(),
                Arc::new(current_peers.iter().map(|p| (p.clone(), false)).collect()),
                Default::default(),
            ],
            prev_epoch_start: Round(0),
            cur_epoch_start: current_epoch_start,
            next_epoch_start: None,
            empty: Default::default(),
        }
    }

    fn index_plus_one(&self, round: Round) -> u8 {
        if self.next_epoch_start.map_or(false, |r| r <= round) {
            3
        } else if self.cur_epoch_start <= round {
            2
        } else if self.prev_epoch_start <= round {
            1
        } else {
            0
        }
    }

    fn peers_for_index_plus_one(&self, index: u8) -> Arc<BTreeMap<PeerId, bool>> {
        match index {
            0 => self.empty.clone(),
            x if x <= 3 => self.peers_resolved[x as usize - 1].clone(),
            _ => unreachable!(),
        }
    }
}
