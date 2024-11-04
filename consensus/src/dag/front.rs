use crate::dag::{Committer, DagHead, DagRound};
use crate::effects::{AltFmt, AltFormat};
use crate::engine::{CachedConfig, Genesis};
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
            rounds: Vec::new(),
            last_back_bottom: Round::BOTTOM,
            has_pending_back_reset: false,
        }
    }
}

impl DagFront {
    pub fn default_front_bottom(top_round: Round) -> Round {
        // to validate for commit;
        // notice that procedure happens at round start, before new local point's dependencies
        // finished their validation, a new 'next' dag round will appear and so no `-1` below
        let min_front_rounds = 3 // new current, includes and witness rounds to validate
            + CachedConfig::commit_history_rounds(); // all committable history for every point

        Round((top_round.0).saturating_sub(min_front_rounds)).max(Genesis::round())
    }

    pub fn default_back_bottom(top_round: Round) -> Round {
        // we could `-1` to use both top and bottom as inclusive range bounds for lag rounds,
        // but collator may re-request TKA from collator, not only the next one
        let reset_rounds = CachedConfig::max_consensus_lag_rounds() // to collate
            + CachedConfig::deduplicate_rounds()  // to discard full anchor history after restart
            + CachedConfig::commit_history_rounds(); // to discard incomplete anchor history after restart

        Round((top_round.0).saturating_sub(reset_rounds)).max(Genesis::round())
    }

    pub fn max_history_bottom(top_round: Round) -> Round {
        // we could `-1` to use both top and bottom as inclusive range bounds for lag rounds,
        // but collator may re-request TKA from collator, not only the next one
        let max_total_rounds = CachedConfig::max_consensus_lag_rounds()
            + CachedConfig::sync_support_rounds() // to follow consensus during sync
            + CachedConfig::deduplicate_rounds() // to discard full anchor history after restart
            + CachedConfig::commit_history_rounds(); // to discard incomplete anchor history after restart

        Round((top_round.0).saturating_sub(max_total_rounds)).max(Genesis::round())
    }

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

    pub fn head(&self, peer_schedule: &PeerSchedule) -> DagHead {
        DagHead::new(peer_schedule, self.top(), self.last_back_bottom)
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
        //    RESET_ROUNDS      front.top()
        //         ↓               ↓
        // |       :        =======|
        // ↑                   ↑
        // MAX_TOTAL_ROUNDS     MIN_FRONT_ROUNDS for front.len()

        // Normal: everything committed
        // |       :        =======| front: must never be shorter than MIN_FRONT_ROUNDS
        // |       :          ---  | back: may be shorter than front and have older top

        // Normal: back is syncing to commit
        // |       :        =======| front: must be ahead of back without a gap (or may overlap)
        // |     --:----------     | back: total len does not exceed MAX_TOTAL_ROUNDS

        // Normal: gap recovered by extending front
        // |     --:--------=======| front: len may exceed MIN_FRONT_ROUNDS and MAX_RESET_ROUNDS
        // |  ---  :               | back: total len does not exceed MAX_TOTAL_ROUNDS
        // => should become (preserving data)
        // |       :        =======| front: shrinks itself
        // |  -----:---------------| back: of total len

        // Unrecoverable gap: total len reaches MAX_TOTAL_ROUNDS (any scenario)
        // (assume back is still lagging to commit if we don't know its status)
        // |       :   -----=======| front: no matter if overlaps or has a gap with back
        // |-------:---            | back: front.top() - back.bottom() >= MAX_TOTAL_ROUNDS
        // => should become (with reset of back bottom to drop trailing dag rounds and free mem)
        // |       :        =======| front: creates new dag round chain for back and shrinks itself
        // |       :---------------| back: chain of MAX_RESET_ROUNDS passed from front
        // Dropped tail gives local node time to download other's points as
        // every node cleans its storage with advance of consensus rounds
        // and points far behind consensus will not be downloaded after some time.

        if let Some(ref committer) = committer {
            // update if we can; if None - use old value;
            // in a rare case committer may have finished its sync, but front decided to have a gap
            if !self.has_pending_back_reset {
                self.last_back_bottom = committer.bottom_round();
            }
        }

        // FIXME should call peer_schedule.forget_previos(self.last_back_bottom) here
        //   and allow peer schedule to have more than one previous subset
        //   as dag bottom must be moved and old rounds dropped before subset is forgotten
        peer_schedule.apply_scheduled(new_top);

        if Self::max_history_bottom(new_top) > self.last_back_bottom {
            // should drop validation tasks and restart them with new bottom to free memory
            self.rounds.clear();
            let new_bottom_round = Self::default_back_bottom(new_top);
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
            committer.extend_from_ahead(&self.drain_upto(Self::default_front_bottom(new_top)));
            committer.extend_from_ahead(&self.rounds);
        }

        new_full_history_bottom
    }

    fn drain_upto(&mut self, new_bottom_round: Round) -> Vec<DagRound> {
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
