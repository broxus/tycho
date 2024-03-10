use tycho_util::FastHashSet;

use crate::models::point::{NodeId, Round};

pub struct ThresholdClock {
    round: Round,
    signatures_received: FastHashSet<NodeId>,
    rejected: FastHashSet<NodeId>, // TODO reason
}
