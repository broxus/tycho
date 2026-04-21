use std::fmt::{Debug, Formatter};
use std::ops::{Add, AddAssign, RangeInclusive};

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
    pub fn add_references_skipped(&mut self, value: usize) {
        self.cumulative.references_skipped += u16::try_from(value).unwrap_or(u16::MAX);
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
    pub was_leader: CounterU16,
    pub was_not_leader: CounterU16,
    pub skipped_rounds: CounterU16,
    pub valid_points: CounterU16,
    pub points_proved: CounterU16,
    pub equivocated: CounterU16,
    pub trans_invalid_points: CounterU16,
    pub invalid_points: CounterU16,
    pub ill_formed_points: CounterU16,
    pub references_skipped: CounterU16,
}

impl MempoolPeerCounters {
    fn merge(&mut self, other: &Self) {
        self.last_round = self.last_round.max(other.last_round);
        self.was_leader += other.was_leader;
        self.was_not_leader += other.was_not_leader;
        self.skipped_rounds += other.skipped_rounds;
        self.valid_points += other.valid_points;
        self.points_proved += other.points_proved;
        self.equivocated += other.equivocated;
        self.trans_invalid_points += other.trans_invalid_points;
        self.invalid_points += other.invalid_points;
        self.ill_formed_points += other.ill_formed_points;
        self.references_skipped += other.references_skipped;
    }
}

#[derive(Default, Clone, Copy)]
pub struct CounterU16(u16);

impl CounterU16 {
    pub fn inner(self) -> u16 {
        self.0
    }
}

impl Debug for CounterU16 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

macro_rules! impl_add {
    ($ty:ty, $($rhs:ty => $cast:ident),+ $(,)?) => {$(
        impl Add<$rhs> for $ty {
            type Output = Self;
            #[inline]
            fn add(self, rhs: $rhs) -> Self {
                Self(self.0.saturating_add(rhs.$cast()))
            }
        }
        impl AddAssign<$rhs> for $ty {
            #[inline]
            fn add_assign(&mut self, rhs: $rhs) {
                self.0 = self.0.saturating_add(rhs.$cast())
            }
        }
    )+};
}

impl_add! { CounterU16, CounterU16 => inner, bool => into, u16 => into, }
