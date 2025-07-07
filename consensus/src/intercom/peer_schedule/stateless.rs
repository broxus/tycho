use std::array;
use std::sync::Arc;

use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_util::FastHashSet;

use crate::effects::{AltFmt, AltFormat};
use crate::models::Round;

#[derive(Clone)]
pub struct PeerScheduleStateless {
    /// retrieved for arbitrary round
    local_keys: Arc<KeyPair>,
    /// order matters to derive leader in `AnchorStage`
    peer_vecs: [Arc<Vec<PeerId>>; 4],
    peer_sets: [Arc<FastHashSet<PeerId>>; 4],
    epoch_starts: [Round; 3],
    pub(super) next_epoch_start: Option<Round>,
    empty_vec: Arc<Vec<PeerId>>,
    empty_set: Arc<FastHashSet<PeerId>>,
}

impl PeerScheduleStateless {
    pub fn new(local_keys: Arc<KeyPair>) -> Self {
        Self {
            local_keys,
            peer_vecs: Default::default(),
            peer_sets: Default::default(),
            epoch_starts: [Round::BOTTOM, Round::BOTTOM, Round::BOTTOM],
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

    pub(super) fn curr_epoch_start(&self) -> Round {
        self.epoch_starts[2]
    }

    /// local peer id is always kept as not resolved
    pub fn peers_for_array<const N: usize>(
        &self,
        rounds: [Round; N],
    ) -> [Arc<FastHashSet<PeerId>>; N] {
        array::from_fn(|i| self.peers_for(rounds[i]).clone())
    }

    pub fn peers_for(&self, round: Round) -> &Arc<FastHashSet<PeerId>> {
        let result = if self.next_epoch_start.is_some_and(|r| round >= r) {
            &self.peer_sets[3]
        } else if round >= self.epoch_starts[2] {
            &self.peer_sets[2]
        } else if round >= self.epoch_starts[1] {
            &self.peer_sets[1]
        } else if round >= self.epoch_starts[0] {
            &self.peer_sets[0]
        } else {
            &self.empty_set
        };
        if result.is_empty() {
            tracing::error!("empty peer set for {round:?}: {:?}", self.alt());
        }
        result
    }

    pub fn peers_ordered_for(&self, round: Round) -> &Arc<Vec<PeerId>> {
        let result = if self.next_epoch_start.is_some_and(|r| round >= r) {
            &self.peer_vecs[3]
        } else if round >= self.epoch_starts[2] {
            &self.peer_vecs[2]
        } else if round >= self.epoch_starts[1] {
            &self.peer_vecs[1]
        } else if round >= self.epoch_starts[0] {
            &self.peer_vecs[0]
        } else {
            &self.empty_vec
        };
        if result.is_empty() {
            tracing::error!("empty peer set for {round:?}: {:?}", self.alt());
        }
        result
    }

    pub(super) fn set_next_peers(&mut self, peers: &[PeerId]) {
        self.peer_sets[3] = Arc::new(peers.iter().copied().collect());
        self.peer_vecs[3] = Arc::new(peers.to_vec());
        self.meter();
    }

    /// on epoch change
    pub(super) fn rotate(&mut self) {
        assert!(
            self.peer_sets[0].is_empty() && self.peer_vecs[0].is_empty(),
            "oldest peer set was not cleaned; {:?}",
            self.alt()
        );
        // make next from oldest
        let next = self
            .next_epoch_start
            .ok_or_else(|| format!("{:?}", self.alt()))
            .expect("attempt to change epoch, but next epoch start is not set");

        assert!(
            next > self.curr_epoch_start(),
            "next start is not in future {:?}",
            self.alt()
        );

        self.next_epoch_start = None; // makes next epoch peers inaccessible for reads
        self.epoch_starts[0] = next;
        self.epoch_starts.rotate_left(1);

        self.peer_sets.rotate_left(1);
        self.peer_vecs.rotate_left(1);
        self.meter();
    }

    pub(super) fn forget_oldest(&mut self) {
        self.peer_sets[0] = Default::default();
        self.peer_vecs[0] = Default::default();
        self.meter();
    }

    fn meter(&self) {
        fn pos(among: &[PeerId], local_id: &PeerId) -> u8 {
            (among.iter())
                .position(|peer_id| local_id == peer_id)
                .map(|pos| pos + 1)
                .unwrap_or_default() as u8
        }
        let local_id = PeerId::from(self.local_keys.public_key);
        metrics::gauge!("tycho_mempool_peer_in_curr_vsubset")
            .set(pos(&self.peer_vecs[2], &local_id));
        metrics::gauge!("tycho_mempool_peer_in_next_vsubset")
            .set(pos(&self.peer_vecs[3], &local_id));

        metrics::gauge!("tycho_mempool_peer_vsubset_change", "epoch" => "curr")
            .set(self.epoch_starts[2].0);
        metrics::gauge!("tycho_mempool_peer_vsubset_change", "epoch" => "next").set(
            // decrease to prev means unset
            self.next_epoch_start
                .map_or(self.epoch_starts[1].0, |round| round.0),
        );
    }
}

impl AltFormat for PeerScheduleStateless {}
impl std::fmt::Debug for AltFmt<'_, PeerScheduleStateless> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);

        f.write_str("PeerScheduleStateless { ")?;

        write!(f, "oldest: {{ start: {}, ", inner.epoch_starts[0].0)?;
        write!(f, "{} }}, ", inner.peer_vecs[0].as_slice().alt())?;

        write!(f, "prev: {{ start: {}, ", inner.epoch_starts[1].0)?;
        write!(f, "{} }}, ", inner.peer_vecs[1].as_slice().alt())?;

        write!(f, "current: {{ start: {}, ", inner.curr_epoch_start().0)?;
        write!(f, "{} }}, ", inner.peer_vecs[2].as_slice().alt())?;

        let next_epoch_start = inner.next_epoch_start.map(|a| a.0);
        write!(f, "next: {{ start: {next_epoch_start:?}, ")?;
        write!(f, "{} }} ", inner.peer_vecs[3].as_slice().alt())?;

        f.write_str("}")
    }
}
