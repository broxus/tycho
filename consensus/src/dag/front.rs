use crate::dag::{Committer, DagRound};
use crate::effects::{AltFmt, AltFormat};
use crate::engine::{Genesis, MempoolConfig};
use crate::intercom::PeerSchedule;
use crate::models::Round;

pub struct DagFront {
    // from the oldest in front to the current round and the next one in back
    rounds: Vec<DagRound>,
    // back bottom may be moved by commit
    last_back_bottom: Round,
    // keep until committer is resolved
    has_pending_back_reset: bool,
}

impl Default for DagFront {
    fn default() -> Self {
        Self {
            rounds: Vec::with_capacity(Self::MAX_TOTAL_DEPTH as usize + 1),
            last_back_bottom: Round::BOTTOM,
            has_pending_back_reset: false,
        }
    }
}

impl DagFront {
    // next round, current round and witness round are above COMMIT_DEPTH for point validation
    // also `-1` as value is subtracted from some top round (inclusive)
    const MIN_FRONT_DEPTH: u32 = MempoolConfig::COMMIT_DEPTH as u32 + 3 - 1;
    const RESET_DEPTH: u32 = Self::MIN_FRONT_DEPTH
        + MempoolConfig::DEDUPLICATE_ROUNDS as u32
        + MempoolConfig::MAX_ANCHOR_DISTANCE as u32;
    const MAX_TOTAL_DEPTH: u32 = Self::RESET_DEPTH + MempoolConfig::ACCEPTABLE_COLLATOR_LAG as u32;

    pub fn init(&mut self, dag_bottom_round: DagRound) -> Committer {
        assert!(self.rounds.is_empty(), "DAG already initialized");
        let mut committer = Committer::default();
        committer.init(&dag_bottom_round);
        self.last_back_bottom = dag_bottom_round.round();
        self.rounds.push(dag_bottom_round);
        assert_eq!(
            self.last_back_bottom,
            committer.bottom_round(),
            "committer botom after init does not match"
        );
        committer
    }

    /// the next after current engine round
    pub fn top(&self) -> &DagRound {
        match self.rounds.last() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some(top) => top,
        }
    }

    fn bottom_round(&self) -> Round {
        match self.rounds.first() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some(bottom) => bottom.round(),
        }
    }

    /// returns new bottom after an unrecoverable gap, and `None` otherwise
    pub fn fill_to_top(
        &mut self,
        new_top: Round,
        committer: Option<&mut Committer>,
        peer_schedule: &PeerSchedule,
    ) -> Option<Round> {
        //    RESET_DEPTH      front.top()
        //         ↓               ↓
        // |       :        =======|
        // ↑                   ↑
        // MAX_TOTAL_DEPTH     MIN_FRONT_DEPTH for front.len()

        // Normal: everything committed
        // |       :        =======| front: must never be shorter than MIN_FRONT_DEPTH+1
        // |       :          ---  | back: may be shorter than front and have older top

        // Normal: back is syncing to commit
        // |       :        =======| front: must be ahead of back without a gap (or may overlap)
        // |     --:----------     | back: total len does not exceed MAX_TOTAL_DEPTH+1

        // Normal: gap recovered by extending front
        // |     --:--------=======| front: len may exceed MIN_FRONT_DEPTH+1 and MAX_RESET_DEPTH+1
        // |  ---  :               | back: total len does not exceed MAX_TOTAL_DEPTH+1
        // => should become (preserving data)
        // |       :        =======| front: shrinks itself
        // |  -----:---------------| back: of total len

        // Unrecoverable gap: total len reaches MAX_TOTAL_DEPTH+1 (any scenario)
        // (assume back is still lagging to commit if we don't know its status)
        // |       :   -----=======| front: no matter if overlaps or has a gap with back
        // |-------:---            | back: front.top() - back.bottom() >= MAX_TOTAL_DEPTH
        // => should become (with reset of back bottom to drop trailing dag rounds and free mem)
        // |       :        =======| front: creates new dag round chain for back and shrinks itself
        // |       :---------------| back: chain of MAX_RESET_DEPTH+1 passed from front

        if let Some(ref committer) = committer {
            // update if we can; if None - use old value;
            // in a rare case committer may have finished its sync, but front decided to have a gap
            if !self.has_pending_back_reset {
                self.last_back_bottom = committer.bottom_round();
            }
        }

        if (new_top.0).saturating_sub(self.last_back_bottom.0) >= Self::MAX_TOTAL_DEPTH {
            // should drop validation tasks and restart them with new bottom to free memory
            self.rounds.clear();
            let new_bottom_round =
                Round(new_top.0.saturating_sub(Self::RESET_DEPTH)).max(Genesis::round());
            self.rounds
                .push(DagRound::new_bottom(new_bottom_round, peer_schedule));
            self.has_pending_back_reset = true;
            self.last_back_bottom = new_bottom_round;
        }

        // to preserve contiguity; even if new rounds are drained, they will be passed to Back Dag
        for _ in self.top().round().next().0..=new_top.0 {
            let top = self.top();
            self.rounds.push(top.new_next(peer_schedule));
        }

        let mut new_full_history_bottom = None;
        if let Some(committer) = committer {
            if self.has_pending_back_reset {
                self.has_pending_back_reset = false;
                *committer = Committer::default();
                let full_history_bottom =
                    committer.init(self.rounds.first().expect("must be init"));
                assert_eq!(
                    self.last_back_bottom,
                    committer.bottom_round(),
                    "committer botom after init does not match"
                );
                new_full_history_bottom = Some(full_history_bottom);
            }
            committer.extend_from_ahead(&self.drain_to_min_depth());
            committer.extend_from_ahead(&self.rounds);
        }

        new_full_history_bottom
    }

    fn drain_to_min_depth(&mut self) -> Vec<DagRound> {
        let new_bottom_round =
            Round(self.top().round().0.saturating_sub(Self::MIN_FRONT_DEPTH)).max(Genesis::round());

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
        let inner = AltFormat::unpack(self);
        for dag_round in &inner.rounds {
            write!(f, "{:?}; ", dag_round.alt())?;
        }
        write!(
            f,
            "back bottom {}{}",
            inner.last_back_bottom.0,
            if inner.has_pending_back_reset {
                " reset pending"
            } else {
                ""
            }
        )?;
        Ok(())
    }
}
impl std::fmt::Display for AltFmt<'_, DagFront> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let this = &AltFormat::unpack(self);
        write!(
            f,
            "DagFront len {} [{}..{}] back bottom {}{}",
            this.rounds.len(),
            this.bottom_round().0,
            this.top().round().0,
            this.last_back_bottom.0,
            if this.has_pending_back_reset {
                " reset pending"
            } else {
                ""
            }
        )
    }
}
