use std::sync::Arc;

use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::intercom::peer_schedule::epoch_starts::EpochStarts;
use crate::models::Round;

pub struct StatsRanges {
    pub(super) stats_slot_maps: [Arc<FastHashMap<PeerId, usize>>; 4],
    pub(super) epoch_starts: EpochStarts,
}

impl StatsRanges {
    /// Slasher stats cannot be merged at epoch switch bounds because the same stats slot
    /// may be reused by different peers:
    /// * at vset change, peers may have the same `validator_idx`
    /// * at subset change, peers have different `validator_idx`
    ///
    /// We suppress stats of an older epoch in the anchors of a newer epoch
    pub fn can_emit_stats(&self, stats_round: Round, anchor_round: Round) -> bool {
        // pre-GC'ed stats round is always less than its anchor round
        assert!(stats_round < anchor_round, "method params in wrong order");

        let Some(stats_idx) = self.epoch_starts.arr_idx(stats_round) else {
            return false; // too old to guarantee subset equality
        };
        let Some(anchor_idx) = self.epoch_starts.arr_idx(anchor_round) else {
            unreachable!("stats_round < anchor_round"); // we've returned false already
        };
        // anchors are expected at least every `max_consensus_lag_rounds` or consensus stalls,
        // so it's the longest tail of rounds to GC or (in this case) of stats to skip;
        // that's the rounds offset for `vset_switch_round` reserved by collator up to pause bound
        stats_idx == anchor_idx
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
