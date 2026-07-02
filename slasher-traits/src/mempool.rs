use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AnchorStats(pub Arc<[AnchorPeerStats]>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorPeerStats {
    pub points_proven: u16,
}
