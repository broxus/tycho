use std::array;
use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_util::FastHashSet;

use crate::models::Round;

#[derive(Clone)]
pub struct PeerScheduleStateless {
    /// retrieved for arbitrary round
    local_keys: Arc<KeyPair>,
    /// order matters to derive leader in `AnchorStage`
    peer_vecs: [Arc<Vec<PeerId>>; 3],
    peer_sets: [Arc<FastHashSet<PeerId>>; 3],
    prev_epoch_start: Round,
    cur_epoch_start: Round,
    next_epoch_start: Option<Round>,
    empty_vec: Arc<Vec<PeerId>>,
    empty_set: Arc<FastHashSet<PeerId>>,
}

impl PeerScheduleStateless {
    pub fn new(local_keys: Arc<KeyPair>) -> Self {
        Self {
            local_keys,
            peer_vecs: Default::default(),
            peer_sets: Default::default(),
            prev_epoch_start: Round::BOTTOM,
            cur_epoch_start: Round::BOTTOM,
            next_epoch_start: None,
            empty_vec: Default::default(),
            empty_set: Default::default(),
        }
    }

    /// Note: signature designates signer's liability to include signed point's id into its point
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
    pub fn local_keys(&self, round: Round) -> Option<Arc<KeyPair>> {
        let local_id = PeerId::from(self.local_keys.public_key);
        if self.peers_for(round).contains(&local_id) {
            Some(self.local_keys.clone())
        } else {
            None
        }
    }

    /// local peer id is always kept as not resolved
    pub fn peers_for_array<const N: usize>(
        &self,
        rounds: [Round; N],
    ) -> [Arc<FastHashSet<PeerId>>; N] {
        array::from_fn(|i| self.peers_for(rounds[i]).clone())
    }

    pub fn peers_for(&self, round: Round) -> &Arc<FastHashSet<PeerId>> {
        if self.next_epoch_start.map_or(false, |r| round >= r) {
            &self.peer_sets[2]
        } else if round >= self.cur_epoch_start {
            &self.peer_sets[1]
        } else if round >= self.prev_epoch_start {
            &self.peer_sets[0]
        } else {
            &self.empty_set
        }
    }

    pub fn peers_ordered_for(&self, round: Round) -> &Arc<Vec<PeerId>> {
        if self.next_epoch_start.map_or(false, |r| round >= r) {
            &self.peer_vecs[2]
        } else if round >= self.cur_epoch_start {
            &self.peer_vecs[1]
        } else if round >= self.prev_epoch_start {
            &self.peer_vecs[0]
        } else {
            &self.empty_vec
        }
    }

    pub(super) fn set_next_peers(&mut self, peers: &[PeerId]) {
        self.peer_sets[2] = Arc::new(peers.iter().copied().collect());
        self.peer_vecs[2] = Arc::new(peers.to_vec());
    }

    pub(super) fn set_next_start(&mut self, round: Round) {
        _ = self.next_epoch_start.replace(round);
    }

    /// on epoch change
    pub(super) fn rotate(&mut self) {
        // make next from previous
        let next = self
            .next_epoch_start
            .expect("attempt to change epoch, but next epoch start is not set");
        self.prev_epoch_start = self.cur_epoch_start;
        self.cur_epoch_start = next;
        self.next_epoch_start = None; // makes next epoch peers inaccessible for reads

        self.forget_previous(); // in case it was not called manually earlier
        self.peer_sets.rotate_left(1);
        self.peer_vecs.rotate_left(1);
    }

    pub(super) fn forget_previous(&mut self) {
        self.peer_sets[0] = Default::default();
        self.peer_vecs[0] = Default::default();
    }
}
