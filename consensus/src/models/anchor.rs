use crate::models::{PointInfo, Round};

pub struct AnchorData {
    pub anchor: PointInfo,
    pub history: Vec<PointInfo>,
}

pub enum CommitResult {
    NewStartAfterGap(Round),
    Next(AnchorData),
}
