use std::ops::RangeInclusive;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AnchorStats {
    pub round_range: RangeInclusive<u32>,
    pub filled_rounds: u32,
    pub data: Arc<[AnchorPeerStats]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorPeerStats {
    pub points_proven: u16,
}
