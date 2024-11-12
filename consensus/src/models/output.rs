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
    Running,
    Paused,
}
