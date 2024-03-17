use std::time::SystemTime;

use tycho_network::PeerId;
use tycho_util::FastDashMap;

use crate::models::point::{Point, Round};

// from latest block
struct NodeInfo {
    round: Round,
    time: SystemTime,
}

pub struct NeighbourWatch {
    nodes: FastDashMap<PeerId, NodeInfo>,
}

impl NeighbourWatch {
    /// every node must provide:
    /// * increasing rounds (two points per same round are equivocation)
    /// * time increasing with every round
    /// * no prev_point - in case of a gap in rounds (no weak links)
    /// * prev_point - in case node made no gaps in rounds
    ///   *  TODO: insert linked (previous) point first, then current one; or move to DAG
    pub fn verify(&mut self, point: &Point) -> bool {
        let round = point.body.location.round;
        let time = point.body.time;
        let mut valid = true;
        // TODO move to as-is validation: let mut valid = prev_round.map_or(true, |prev| prev.0 + 1 == round.0);
        self.nodes
            .entry(point.body.location.author.clone())
            .and_modify(|e| {
                valid = e.round < round
                    && e.time < time
                    // node either skipped a round, or provided evidences for prev block
                    && e.round <= round.prev();
                if e.round < round {
                    (*e).round = round
                };
                if e.time < time {
                    (*e).time = time
                };
            })
            .or_insert(NodeInfo { round, time });
        valid
    }
}
