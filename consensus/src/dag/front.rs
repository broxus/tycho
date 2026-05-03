use tokio::sync::mpsc;

use crate::dag::dag_point_future::WeakDagPointFuture;
use crate::dag::{Committer, DagHead, DagRound};
use crate::effects::{AltFmt, AltFormat, Ctx, EngineCtx, RoundCtx};
use crate::engine::lifecycle::FixHistoryFlag;
use crate::engine::{ConsensusConfigExt, MempoolConfig};
use crate::intercom::PeerSchedule;
use crate::models::Round;

pub struct DagFront {
    triggers_tx: mpsc::UnboundedSender<WeakDagPointFuture>,
    // from the oldest in front to the current round and the next one in back
    rounds: Vec<DagRound>,
    // back bottom may be moved by commit
    last_back_bottom: Round,
    // keep until committer is resolved
    has_pending_back_reset: bool,
}

impl DagFront {
    pub fn new(
        round: Round,
        fix_history: FixHistoryFlag,
        peer_schedule: &PeerSchedule,
        conf: &MempoolConfig,
    ) -> (Self, Committer) {
        let (triggers_tx, triggers_rx) = mpsc::unbounded_channel();
        let dag_bottom_round = DagRound::new_bottom(round, &triggers_tx, peer_schedule, conf);

        let mut committer = Committer::new(triggers_rx);
        committer.init(&dag_bottom_round, fix_history.0, conf);

        let this = Self {
            triggers_tx,
            last_back_bottom: dag_bottom_round.round(),
            rounds: vec![dag_bottom_round],
            has_pending_back_reset: false,
        };
        (this, committer)
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

    pub(super) fn bottom_round(&self) -> Round {
        match self.rounds.first() {
            None => unreachable!("DAG cannot be empty if properly initialized"),
            Some(bottom) => bottom.round(),
        }
    }

    /// Returns new bottom after an unrecoverable gap, and `None` otherwise.
    ///
    /// See [`ConsensusConfigExt`] for logic description
    pub fn fill_to_top(
        &mut self,
        new_top: Round,
        committer: Option<&mut Committer>,
        peer_schedule: &PeerSchedule,
        round_ctx: &RoundCtx,
    ) {
        let _span = round_ctx.span().enter();
        let conf = round_ctx.conf();

        if let Some(ref committer) = committer {
            // update if we can; if None - use old value;
            // in a rare case committer may have finished its sync, but front decided to have a gap
            if !self.has_pending_back_reset {
                self.last_back_bottom = committer.bottom_round();
            }
        }

        peer_schedule.apply_scheduled(new_top);

        if new_top > self.last_back_bottom + conf.consensus.max_total_rounds() {
            // should drop validation tasks and restart them with new bottom to free memory
            self.rounds.clear();
            let new_bottom_round =
                (conf.genesis_round).max(new_top - conf.consensus.reset_rounds());
            self.rounds.push(DagRound::new_bottom(
                new_bottom_round,
                &self.triggers_tx,
                peer_schedule,
                conf,
            ));
            self.has_pending_back_reset = true;
            self.last_back_bottom = new_bottom_round;
        }

        // to preserve contiguity; even if new rounds are drained, they will be passed to Back Dag
        for _ in self.top().round().next().0..=new_top.0 {
            let top = self.top();
            (self.rounds).push(top.new_next(&self.triggers_tx, peer_schedule, conf));
        }

        if let Some(committer) = committer {
            if self.has_pending_back_reset {
                self.has_pending_back_reset = false;
                committer.reset();
                committer.init(self.rounds.first().expect("must be init"), true, conf);
                assert_eq!(
                    self.last_back_bottom,
                    committer.bottom_round(),
                    "committer botom after init does not match"
                );
            }
            committer
                .extend_from_ahead(&self.drain_upto(new_top - conf.consensus.min_front_rounds()));
            committer.extend_from_ahead(&self.rounds);
            EngineCtx::meter_dag_len(committer.dag_len());
        }
    }

    fn drain_upto(&mut self, new_bottom_round: Round) -> Vec<DagRound> {
        let bottom = self.bottom_round();

        let amount = (new_bottom_round - bottom.0).0 as usize;

        // leaves bottom round in place
        let result = self.rounds.drain(..amount).collect::<Vec<_>>();

        assert_eq!(
            self.bottom_round(),
            new_bottom_round.max(bottom),
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
