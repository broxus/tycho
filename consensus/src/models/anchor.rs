use crate::models::PointInfo;

pub struct AnchorData {
    pub anchor: PointInfo,
    pub history: Vec<PointInfo>,
}

pub enum CommitResult {
    UnrecoverableGap,
    Next(AnchorData),
}
