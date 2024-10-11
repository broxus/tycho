use crate::dag::DagRound;
use crate::effects::{AltFmt, AltFormat};
use crate::engine::round_watch::Consensus;
use crate::engine::{Genesis, MempoolConfig};
use crate::intercom::PeerSchedule;
use crate::models::Round;

#[derive(Default)]
pub struct DagFront {
    // from the oldest in front to the current round and the next one in back
    rounds: Vec<DagRound>,
}

impl DagFront {
    // next round, current round and witness round are above depth for point validation
    const HOT_END_ROUNDS: u32 = 3;

    pub fn init(&mut self, bottom_round: DagRound) {
        assert!(self.rounds.is_empty(), "DAG already initialized");
        self.rounds.push(bottom_round);
    }

    /// the next after current engine round
    pub fn top(&self) -> &DagRound {
        match self.rounds.last() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some(top) => top,
        }
    }

    pub fn bottom_round(&self) -> Round {
        match self.rounds.first() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some(bottom) => bottom.round(),
        }
    }

    pub fn fill_to_top(&mut self, new_top: Round, peer_schedule: &PeerSchedule) -> Vec<DagRound> {
        let front_bottom_round = Round(
            (new_top.0.saturating_add(1))
                .saturating_sub(Self::HOT_END_ROUNDS)
                .saturating_sub(MempoolConfig::COMMIT_DEPTH as u32)
                .max(Genesis::round().0),
        );

        // extend to the max possible - will be shortened when top known is determined
        let back_bottom_round = Consensus::history_bottom(new_top);
        if self.top().round() < back_bottom_round {
            self.rounds.clear();
            self.init(DagRound::new_bottom(back_bottom_round, peer_schedule));
        }

        // to preserve contiguity; even if new rounds are drained, they will be passed to Back Dag
        for _ in self.top().round().next().0..=new_top.0 {
            let top = self.top();
            self.rounds.push(top.new_next(peer_schedule));
        }
        self.assert_len();

        let result = self.drain_upto(front_bottom_round);
        self.assert_len();

        if self.rounds.capacity() > self.rounds.len().saturating_mul(4) {
            self.rounds.shrink_to(self.rounds.len().saturating_mul(2));
        }

        result
    }

    fn assert_len(&self) {
        let top = self.top().round();
        let bottom = self.bottom_round();
        assert_eq!(
            (top.0 - bottom.0) as usize + 1,
            self.rounds.len(),
            "DAG has invalid length to be contiguous"
        );
    }

    pub fn as_slice(&self) -> &[DagRound] {
        &self.rounds
    }

    fn drain_upto(&mut self, new_bottom_round: Round) -> Vec<DagRound> {
        // strictly `COMMIT_DEPTH` rounds will precede the current one, which is +1 to the new length
        // let new_bottom_round = Round((current.0).saturating_sub(MempoolConfig::COMMIT_DEPTH as u32));

        let bottom = self.bottom_round();

        let amount = new_bottom_round.0.saturating_sub(bottom.0) as usize;

        // leaves bottom round in place
        let result = self.rounds.drain(..amount).collect::<Vec<_>>();

        assert_eq!(
            self.bottom_round(),
            new_bottom_round,
            "new bottom does not match expected; drained {:?}; modified dag {:?}",
            result.iter().map(|p| p.round()).collect::<Vec<_>>(),
            self.rounds.iter().map(|p| p.round()).collect::<Vec<_>>(),
        );

        result
    }
}

impl AltFormat for DagFront {}
impl std::fmt::Debug for AltFmt<'_, DagFront> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for dag_round in &AltFormat::unpack(self).rounds {
            write!(f, "{:?}; ", dag_round.alt())?;
        }
        Ok(())
    }
}
