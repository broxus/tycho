use std::ops::RangeInclusive;

use tycho_network::PeerId;
use tycho_util::FastHashMap;

pub trait MempoolEventsListener: Send + Sync {
    fn put_stats(&self, anchor_round: u32, data: FastHashMap<PeerId, MempoolPeerStats>);
}

#[async_trait::async_trait]
pub trait MempoolEventsCache: Send + Sync {
    async fn get_stats(&self, rounds: RangeInclusive<u32>)
    -> FastHashMap<PeerId, MempoolPeerStats>;
}

#[derive(Debug, thiserror::Error)]
pub enum MempoolStatsMergeError {
    #[error("stat range {:?} cannot include new out-of-order round {}", .0, .1)]
    RoundOutOfOrder(RangeInclusive<u32>, u32),
    #[error("stat range {:?} overlaps with {:?}", .0, .1)]
    OverlappingRanges(RangeInclusive<u32>, RangeInclusive<u32>),
}

#[derive(Debug)]
pub struct MempoolPeerStats {
    first_round: u32,
    filled_rounds: u32,
    cumulative: MempoolPeerCounters,
}

impl MempoolPeerStats {
    pub fn new(round: u32) -> Self {
        Self {
            first_round: round, // zero round cannot exist
            filled_rounds: 0,
            cumulative: MempoolPeerCounters::default(), // last round is zero
        }
    }

    fn range(&self) -> RangeInclusive<u32> {
        self.first_round..=self.cumulative.last_round
    }

    pub fn filled_rounds(&self) -> u32 {
        self.filled_rounds
    }

    pub fn counters(&self) -> Option<&MempoolPeerCounters> {
        (!self.range().is_empty()).then_some(&self.cumulative)
    }

    pub fn add_in_order(
        &mut self,
        stats: &MempoolPeerCounters,
    ) -> Result<(), MempoolStatsMergeError> {
        let old_range = self.range();
        if stats.last_round < *old_range.start() || old_range.contains(&stats.last_round) {
            return Err(MempoolStatsMergeError::RoundOutOfOrder(
                old_range,
                stats.last_round,
            ));
        }
        self.filled_rounds += 1;
        self.cumulative.merge(stats);
        Ok(())
    }

    // unfortunately this is not filled at the same time with the other stats
    pub fn add_references_skipped(&mut self, value: u32) {
        self.cumulative.references_skipped += value;
    }

    pub fn merge_with(&mut self, other: &Self) -> Result<(), MempoolStatsMergeError> {
        let stat_overlap = if self.first_round < other.first_round {
            self.range().contains(&other.first_round)
        } else {
            other.range().contains(&self.first_round)
        };
        if stat_overlap {
            return Err(MempoolStatsMergeError::OverlappingRanges(
                self.range(),
                other.range(),
            ));
        }
        self.first_round = self.first_round.min(other.first_round);
        self.filled_rounds += other.filled_rounds;
        self.cumulative.merge(&other.cumulative);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MempoolPeerCounters {
    pub last_round: u32,
    pub was_leader: u32,
    pub was_not_leader: u32,
    pub skipped_rounds: u32,
    pub valid_points: u32,
    pub equivocated: u32,
    pub invalid_points: u32,
    pub ill_formed_points: u32,
    pub references_skipped: u32,
}

impl MempoolPeerCounters {
    fn merge(&mut self, other: &Self) {
        self.last_round = self.last_round.max(other.last_round);
        self.was_leader += other.was_leader;
        self.was_not_leader += other.was_not_leader;
        self.skipped_rounds += other.skipped_rounds;
        self.valid_points += other.valid_points;
        self.equivocated += other.equivocated;
        self.invalid_points += other.invalid_points;
        self.ill_formed_points += other.ill_formed_points;
        self.references_skipped += other.references_skipped;
    }
}
