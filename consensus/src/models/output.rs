use crate::models::{PointInfo, PointKey, Round};

pub struct AnchorData {
    pub proof_key: PointKey,
    pub anchor: PointInfo,
    /// first anchor after Genesis is not linked to previous one
    pub prev_anchor: Option<Round>,
    pub history: Vec<PointInfo>,
    pub is_executable: bool,
}

pub enum MempoolOutput {
    NextAnchor(Box<AnchorData>),
    GapUpTo(Round),
    /// just an info message: `true` when set on pause
    Paused(bool),
}
