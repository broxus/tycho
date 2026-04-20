use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::effects::{AltFmt, AltFormat};
use crate::models::{MempoolPeerStats, PointInfo, PointKey, Round};

pub struct AnchorData {
    pub proof_key: PointKey,
    pub anchor: PointInfo,
    /// first anchor after Genesis is not linked to previous one
    pub prev_anchor: Option<Round>,
    pub history: Vec<PointInfo>,
    pub is_executable: bool,
    pub needs_empty_cache: bool,
}

pub enum MempoolOutput {
    NextAnchor(Box<AnchorData>),
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
        data.sort_unstable_by_key(|(peer, _)| *peer);
        f.debug_struct("MempoolStatsOutput")
            .field("anchor_round", &inner.anchor_round)
            .field("data", &data)
            .finish()
    }
}
