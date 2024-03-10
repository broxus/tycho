use tycho_network::PeerId;
use tycho_util::FastHashSet;

use crate::models::point::Round;

pub struct ThresholdClock {
    round: Round,
    signatures_received: FastHashSet<PeerId>,
    rejected: FastHashSet<PeerId>, // TODO reason
}
