use std::array;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use tycho_network::{PeerId, PrivateOverlay};
use tycho_util::FastHashSet;

use crate::intercom::dto::PeerState;
use crate::models::{NodeCount, Round};

// As validators are elected for wall-clock time range,
// the round of validator set switch is not known beforehand
// and will be determined by the time in anchor vertices:
// it must reach some predefined time range,
// when the new set is supposed to be online and start to request points,
// and a (relatively high) predefined number of support rounds must follow
// for the anchor chain to be committed by majority and for the new nodes to gather data.
// The switch will occur for validator sets as a whole.

#[derive(Clone)]
pub struct PeerSchedule {
    // FIXME remove mutex ( parking_lot ! )
    //  and just restart updater when new peers or epoch start are known;
    //  use copy-on-write to replace Inner as a whole;
    //  maybe store schedule-per-round inside DAG round, but how to deal with download tasks then?
    inner: Arc<Mutex<PeerScheduleInner>>,
    // Connection to self is always "Added"
    // Updates are Resolved or Removed, sent single time
    updates: broadcast::Sender<(PeerId, PeerState)>,
    /// Keypair may be changed only with node restart, and is known before validator elections.
    /// Node should use its keypair only to produce own and sign others points.
    local_keys: Arc<KeyPair>,
}

impl PeerSchedule {
    pub fn new(local_keys: Arc<KeyPair>) -> Self {
        // TODO channel size is subtle: it cannot be large,
        //   but any skipped event breaks 2F+1 guarantees
        let (updates, _) = broadcast::channel(100);
        let this = Self {
            inner: Arc::new(Mutex::new(PeerScheduleInner::new())),
            updates,
            local_keys,
        };
        this
    }

    /// Does not return updates on local peer_id
    pub fn updates(&self) -> broadcast::Receiver<(PeerId, PeerState)> {
        tracing::debug!("subscribing to peer updates");
        self.updates.subscribe()
    }

    // To sign a point or to query for points, we need to know the intersection of:
    // * which nodes are in the validator set during the round of interest
    // * which nodes are able to connect at the moment
    pub async fn wait_for_peers(&self, round: &Round, node_count: NodeCount) {
        let mut rx = self.updates();
        let peers = (*self.peers_for(round)).clone();
        let local_id = self.local_id();
        let mut resolved = peers
            .iter()
            .filter(|(&peer_id, &state)| state == PeerState::Resolved && peer_id != local_id)
            .map(|(peer_id, _)| *peer_id)
            .collect::<FastHashSet<_>>();
        while resolved.len() < node_count.majority_of_others() {
            match rx.recv().await {
                Ok((peer_id, new_state)) => match new_state {
                    PeerState::Resolved => _ = resolved.insert(peer_id),
                    PeerState::Unknown => _ = resolved.remove(&peer_id),
                },
                _ => {}
            }
        }
    }

    /// Note: keep private, it's just a local shorthand
    pub(super) fn local_id(&self) -> PeerId {
        self.local_keys.public_key.into()
    }

    /// Note: signature designates signer's liability to include signed point's id into own point
    /// at the next round (to compare one's evidence against others' includes and witnesses).
    /// So any point is sent to nodes, scheduled for the next round only.
    /// So:
    /// * to create own point @ r+0, node needs a keypair for r+0
    /// * to sign others points @ r+0 during r+0 as inclusion for r+1, node needs a keypair for r+1
    /// * to sign others points @ r-1 during r+0 as a witness for r+1, node needs
    ///   * a keypair for r+0 to make a signature (as if it was wade during r-1)
    ///   * a keypair for r+1 to produce own point @ r+1
    ///
    /// The other way:
    ///   any point @ r+0 contains signatures made by nodes with keys, scheduled for r+0 only:
    /// * by the author at the same r+0
    /// * evidence of the author's point @ r-1:
    ///   * by those @ r-1 who includes @ r+0 (the direct receivers of the point @ r-1)
    ///   * by those @ r+0 who will witness @ r+1 (iff they are scheduled for r+0)
    ///
    /// Consensus progress is not guaranteed without witness (because of evidence requirement),
    /// but we don't care if the consensus of an ending epoch stalls at its last round.
    pub fn local_keys(&self, round: &Round) -> Option<Arc<KeyPair>> {
        if self.peers_for(round).contains_key(&self.local_id()) {
            Some(self.local_keys.clone())
        } else {
            None
        }
    }

    pub fn all_resolved(&self) -> FastHashSet<PeerId> {
        let inner = self.inner.lock();
        inner.all_resolved(self.local_id())
    }

    pub fn peers_for(&self, round: &Round) -> Arc<BTreeMap<PeerId, PeerState>> {
        let inner = self.inner.lock();
        inner.peers_for_index_plus_one(inner.index_plus_one(round))
    }

    pub fn peers_for_array<const N: usize>(
        &self,
        rounds: [Round; N],
    ) -> [Arc<BTreeMap<PeerId, PeerState>>; N] {
        let inner = self.inner.lock();
        array::from_fn(|i| inner.peers_for_index_plus_one(inner.index_plus_one(&rounds[i])))
    }

    /// does not return empty maps
    pub fn peers_for_range(&self, rounds: &Range<Round>) -> Vec<Arc<BTreeMap<PeerId, PeerState>>> {
        if rounds.end <= rounds.start {
            return vec![];
        }
        let inner = self.inner.lock();
        let mut first = inner.index_plus_one(&rounds.start);
        let last = inner.index_plus_one(&rounds.end.prev());
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
            panic!("attempt to change epoch, but next epoch start is not set");
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

    /// use [updater](super::PeerScheduleUpdater::set_next_peers())
    pub(super) fn set_next_peers(&self, peers: &Vec<PeerId>, overlay: &PrivateOverlay) {
        let local_id = self.local_id();
        let mut inner = self.inner.lock();
        // check resolved peers only after blocking other threads from updating inner;
        // note that entries are under read lock
        let resolved = overlay
            .read_entries()
            .iter()
            .filter(|a| a.resolver_handle.is_resolved())
            .map(|a| a.peer_id.clone())
            .collect::<FastHashSet<_>>();
        let peers = peers
            .iter()
            .map(|peer_id| {
                (
                    peer_id.clone(),
                    if resolved.contains(&peer_id) && peer_id != local_id {
                        PeerState::Resolved
                    } else {
                        PeerState::Unknown
                    },
                )
            })
            .collect::<Vec<_>>();
        // detach existing copies - they are tightened to use-site DAG round
        let next = Arc::make_mut(&mut inner.peers_resolved[2]);
        next.clear();
        next.extend(peers);
    }

    /// Returns [true] if update was successfully applied
    pub(super) fn set_resolved(&self, peer_id: &PeerId, resolved: bool) -> bool {
        let mut is_applied = false;
        let new_state = if resolved && peer_id != self.local_id() {
            PeerState::Resolved
        } else {
            PeerState::Unknown
        };
        {
            let mut inner = self.inner.lock();
            for i in 0..inner.peers_resolved.len() {
                let Some(b) = Arc::make_mut(&mut inner.peers_resolved[i]).get_mut(peer_id) else {
                    continue;
                };
                if *b != new_state {
                    *b = new_state.clone();
                    is_applied = true;
                }
            }
        }
        if is_applied {
            _ = self.updates.send((peer_id.clone(), new_state));
        }
        is_applied
    }
}

struct PeerScheduleInner {
    // order to select leader by coin flip
    peers_resolved: [Arc<BTreeMap<PeerId, PeerState>>; 3],
    prev_epoch_start: Round,
    cur_epoch_start: Round,
    next_epoch_start: Option<Round>,
    empty: Arc<BTreeMap<PeerId, PeerState>>,
}

impl PeerScheduleInner {
    fn new() -> Self {
        Self {
            peers_resolved: Default::default(),
            prev_epoch_start: Round(0),
            cur_epoch_start: Round(0),
            next_epoch_start: None,
            empty: Default::default(),
        }
    }

    fn index_plus_one(&self, round: &Round) -> u8 {
        if self.next_epoch_start.as_ref().map_or(false, |r| r <= round) {
            3
        } else if &self.cur_epoch_start <= round {
            2
        } else if &self.prev_epoch_start <= round {
            1
        } else {
            0
        }
    }

    fn peers_for_index_plus_one(&self, index: u8) -> Arc<BTreeMap<PeerId, PeerState>> {
        match index {
            0 => self.empty.clone(),
            x if x <= 3 => self.peers_resolved[x as usize - 1].clone(),
            _ => unreachable!(),
        }
    }

    fn all_resolved(&self, local_id: PeerId) -> FastHashSet<PeerId> {
        self.peers_resolved[0]
            .iter()
            .chain(self.peers_resolved[1].iter())
            .chain(self.peers_resolved[2].iter())
            .filter(|(peer_id, state)| *state == &PeerState::Resolved && peer_id != &local_id)
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }
}
