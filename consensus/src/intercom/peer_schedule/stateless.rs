use std::array;
use std::collections::BTreeSet;
use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;
use tycho_network::PeerId;

use crate::models::Round;

#[derive(Clone)]
pub struct PeerScheduleStateless {
    /// retrieved for arbitrary round
    local_keys: Arc<KeyPair>,
    /// order matters to derive leader in `AnchorStage`
    peers: [Arc<BTreeSet<PeerId>>; 3],
    prev_epoch_start: Round,
    cur_epoch_start: Round,
    next_epoch_start: Option<Round>,
    empty: Arc<BTreeSet<PeerId>>,
}

impl PeerScheduleStateless {
    pub fn new(local_keys: Arc<KeyPair>) -> Self {
        Self {
            local_keys,
            peers: Default::default(),
            prev_epoch_start: Round::BOTTOM,
            cur_epoch_start: Round::BOTTOM,
            next_epoch_start: None,
            empty: Default::default(),
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
    ) -> [Arc<BTreeSet<PeerId>>; N] {
        array::from_fn(|i| self.peers_for(rounds[i]).clone())
    }

    pub fn peers_for(&self, round: Round) -> &'_ Arc<BTreeSet<PeerId>> {
        if self.next_epoch_start.map_or(false, |r| round >= r) {
            &self.peers[2]
        } else if round >= self.cur_epoch_start {
            &self.peers[1]
        } else if round >= self.prev_epoch_start {
            &self.peers[0]
        } else {
            &self.empty
        }
    }

    pub(super) fn set_next_peers(&mut self, peers: &[PeerId]) {
        self.peers[2] = Arc::new(peers.iter().copied().collect());
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
        self.peers.rotate_left(1);
    }

    pub(super) fn forget_previous(&mut self) {
        self.peers[0] = Default::default();
    }
}
