use std::array;
use std::sync::Arc;

use tycho_crypto::ed25519::KeyPair;
use tycho_network::PeerId;
use tycho_util::{FastHashMap, FastHashSet};

use crate::effects::{AltFmt, AltFormat};
use crate::intercom::peer_schedule::epoch_starts::EpochStarts;
use crate::intercom::peer_schedule::stats_ranges::StatsRanges;
use crate::models::Round;

/// All collections are peer subsets, not full vsets
#[derive(Clone)]
pub struct PeerScheduleStateless {
    /// retrieved for arbitrary round
    local_keys: Arc<KeyPair>,
    stats_slot_maps: [Arc<FastHashMap<PeerId, usize>>; 4],
    /// order matters to derive leader in `AnchorStage`
    peer_vecs: [Arc<Vec<PeerId>>; 4],
    peer_sets: [Arc<FastHashSet<PeerId>>; 4],

    pub(super) epoch_starts: EpochStarts,

    empty_slot_map: Arc<FastHashMap<PeerId, usize>>,
    empty_vec: Arc<Vec<PeerId>>,
    empty_set: Arc<FastHashSet<PeerId>>,
}

impl PeerScheduleStateless {
    pub fn new(local_keys: Arc<KeyPair>) -> Self {
        Self {
            local_keys,
            stats_slot_maps: Default::default(),
            peer_vecs: Default::default(),
            peer_sets: Default::default(),

            epoch_starts: EpochStarts::default(),

            empty_slot_map: Default::default(),
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

    pub fn stats_ranges(&self) -> StatsRanges {
        StatsRanges {
            stats_slot_maps: self.stats_slot_maps.clone(),
            epoch_starts: self.epoch_starts.clone(),
        }
    }

    pub fn peers_ordered_for(&self, round: Round) -> &Arc<Vec<PeerId>> {
        let result =
            (self.epoch_starts.arr_idx(round)).map_or(&self.empty_vec, |idx| &self.peer_vecs[idx]);
        if result.is_empty() {
            tracing::error!("empty peer vec for {round:?}: {:?}", self.alt());
        }
        result
    }

    pub fn peers_for(&self, round: Round) -> &Arc<FastHashSet<PeerId>> {
        let result =
            (self.epoch_starts.arr_idx(round)).map_or(&self.empty_set, |idx| &self.peer_sets[idx]);
        if result.is_empty() {
            tracing::error!("empty peer set for {round:?}: {:?}", self.alt());
        }
        result
    }

    /// local peer id is always kept as not resolved
    pub fn peers_for_array<const N: usize>(
        &self,
        rounds: [Round; N],
    ) -> [Arc<FastHashSet<PeerId>>; N] {
        array::from_fn(|i| self.peers_for(rounds[i]).clone())
    }

    pub(super) fn set_next_peers(
        &mut self,
        next_epoch_start: Round,
        working_subset: &[(PeerId, u16)],
    ) {
        let mut bc_config_vset_ordered = working_subset.to_vec();
        bc_config_vset_ordered.sort_unstable_by_key(|(_, validator_idx)| *validator_idx);
        let stats_slots = bc_config_vset_ordered
            .into_iter()
            .enumerate()
            .map(|(slot, (peer_id, _))| (peer_id, slot))
            .collect();
        self.stats_slot_maps[3] = Arc::new(stats_slots);
        self.peer_sets[3] = Arc::new(working_subset.iter().map(|(p, _)| *p).collect());
        self.peer_vecs[3] = Arc::new(working_subset.iter().map(|(p, _)| *p).collect());
        self.epoch_starts.next = Some(next_epoch_start);
        self.meter();
    }

    /// on epoch change
    pub(super) fn rotate(&mut self) {
        assert!(
            self.peer_sets[0].is_empty() && self.peer_vecs[0].is_empty(),
            "oldest peer set was not cleaned; {:?}",
            self.alt()
        );

        self.epoch_starts.rotate();

        self.stats_slot_maps.rotate_left(1);
        self.peer_sets.rotate_left(1);
        self.peer_vecs.rotate_left(1);
        self.meter();
    }

    pub(super) fn forget_oldest(&mut self) {
        self.stats_slot_maps[0] = self.empty_slot_map.clone();
        self.peer_sets[0] = self.empty_set.clone();
        self.peer_vecs[0] = self.empty_vec.clone();
        self.meter();
    }

    fn meter(&self) {
        fn pos(among: &[PeerId], local_id: &PeerId) -> u8 {
            (among.iter())
                .position(|peer_id| peer_id == local_id)
                .map(|pos| pos + 1)
                .unwrap_or_default() as u8
        }
        let local_id = PeerId::from(self.local_keys.public_key);
        metrics::gauge!("tycho_mempool_peer_in_curr_vsubset")
            .set(pos(&self.peer_vecs[2], &local_id));
        metrics::gauge!("tycho_mempool_peer_in_next_vsubset")
            .set(pos(&self.peer_vecs[3], &local_id));

        metrics::gauge!("tycho_mempool_peer_vsubset_change", "epoch" => "curr")
            .set(self.epoch_starts.curr().0);
        metrics::gauge!("tycho_mempool_peer_vsubset_change", "epoch" => "next").set(
            // decrease to prev means unset
            (self.epoch_starts.next).map_or(self.epoch_starts.prev().0, |round| round.0),
        );
    }
}

impl AltFormat for PeerScheduleStateless {}
impl std::fmt::Debug for AltFmt<'_, PeerScheduleStateless> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);

        f.write_str("PeerScheduleStateless { ")?;

        write!(f, "oldest: {{ start: {}, ", inner.epoch_starts.oldest().0)?;
        write!(f, "{} }}, ", inner.peer_vecs[0].as_slice().alt())?;

        write!(f, "prev: {{ start: {}, ", inner.epoch_starts.prev().0)?;
        write!(f, "{} }}, ", inner.peer_vecs[1].as_slice().alt())?;

        write!(f, "curr: {{ start: {}, ", inner.epoch_starts.curr().0)?;
        write!(f, "{} }}, ", inner.peer_vecs[2].as_slice().alt())?;

        let next_epoch_start = inner.epoch_starts.next.map(|a| a.0);
        write!(f, "next: {{ start: {next_epoch_start:?}, ")?;
        write!(f, "{} }} ", inner.peer_vecs[3].as_slice().alt())?;

        f.write_str("}")
    }
}
