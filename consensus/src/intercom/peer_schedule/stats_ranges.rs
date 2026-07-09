use std::sync::Arc;

use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::engine::MempoolConfig;
use crate::intercom::peer_schedule::epoch_starts::EpochStarts;
use crate::models::Round;

pub struct StatsRanges {
    pub(super) stats_slot_maps: [Arc<FastHashMap<PeerId, usize>>; 4],
    pub(super) epoch_starts: EpochStarts,
}

impl StatsRanges {
    /// Slasher stats cannot be merged at epoch switch bounds because:
    /// * at vset change, different peers may have the same `validator_idx`
    /// * at subset change, the same peer may have different `validator_idx`
    ///
    /// We suppress stats of an older epoch in the anchors of a newer epoch:
    /// * last `max_consensus_lag_rounds` round stats in ending epoch are suppressed
    ///   because anchor data will be attributed to the next validator|slasher session
    /// * stats and achor must belong to the same epoch - that way we suppress
    ///   trailing stats from the previous epoch for a newer epoch anchor
    /// * stats for too old (removed from schedule) epoch are suppressed too
    ///
    /// Returns `None` if anchor stats are suppressed
    pub fn stats_since(&self, anchor_round: Round, conf: &MempoolConfig) -> Option<Round> {
        // anchors are expected at least every `max_consensus_lag_rounds` or consensus stalls,
        // so it's the longest tail of rounds to GC or (in this case) of stats to skip;
        // that's the rounds offset for `vset_switch_round` reserved by collator up to pause bound

        let a = self.epoch_starts.epoch_start(anchor_round);
        let b = (self.epoch_starts)
            .epoch_start(anchor_round + conf.consensus.max_consensus_lag_rounds.get());

        if a == b { a } else { None }
    }

    #[allow(dead_code, reason = "will be used soon")]
    pub fn stats_slot_map(&self, anchor_round: Round) -> Option<&Arc<FastHashMap<PeerId, usize>>> {
        let idx = self.epoch_starts.arr_idx(anchor_round)?;
        let result = &self.stats_slot_maps[idx];
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}
