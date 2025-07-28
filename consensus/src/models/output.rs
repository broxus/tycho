use tycho_network::PeerId;
use tycho_slasher_traits::MempoolPeerStats;
use tycho_util::FastHashMap;

use crate::effects::{AltFmt, AltFormat};
use crate::models::{PointInfo, Round};

pub struct AnchorData {
    pub anchor: PointInfo,
    // first anchor after Genesis is not linked to previous one
    pub prev_anchor: Option<Round>,
    pub history: Vec<PointInfo>,
}

pub enum MempoolOutput {
    // tells the mempool adapter which anchors to skip because some first ones after a gap
    // have incomplete history that should not be taken into account
    // (it's no harm to use it for deduplication - it will be evicted after buffer is refilled)
    NewStartAfterGap(Round),
    NextAnchor(AnchorData),
    // must be sent after NextAnchor to make data available for GC
    CommitFinished(Round),
    Running,
    Paused,
}

pub struct MempoolStatsOutput {
    pub anchor_round: Round,
    pub data: FastHashMap<PeerId, MempoolPeerStats>,
}

impl AltFormat for MempoolStatsOutput {}
impl std::fmt::Debug for AltFmt<'_, MempoolStatsOutput> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        let mut data = Vec::from_iter(&inner.data);
        data.sort_unstable_by(|(peer_a, _), (peer_b, _)| peer_a.cmp(peer_b));
        f.debug_struct("MempoolStatsOutput")
            .field("anchor_round", &inner.anchor_round)
            .field("data", &data)
            .finish()
    }
}
