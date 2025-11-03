use std::sync::Arc;

use futures_util::never::Never;

use super::{POINT_KEY_LEN, fill_point_prefix};
use crate::effects::{Cancelled, Ctx, RoundCtx, Task};
use crate::engine::round_watch::{Commit, Consensus, RoundWatcher, TopKnownAnchor};
use crate::engine::{ConsensusConfigExt, MempoolConfig, NodeConfig};
use crate::models::Round;
use crate::storage::MempoolDb;

pub struct DbCleaner {
    mempool_db: Arc<MempoolDb>,
    top_known_anchor: RoundWatcher<TopKnownAnchor>,
    commit_finished: RoundWatcher<Commit>,
    consensus_round: RoundWatcher<Consensus>,
}

impl DbCleaner {
    pub fn new(
        mempool_db: Arc<MempoolDb>,
        top_known_anchor: RoundWatcher<TopKnownAnchor>,
        commit_finished: RoundWatcher<Commit>,
        consensus_round: RoundWatcher<Consensus>,
    ) -> Self {
        Self {
            mempool_db,
            top_known_anchor,
            commit_finished,
            consensus_round,
        }
    }

    fn least_to_keep(
        consensus: Round,
        committed: Round,
        top_known_anchor: Round,
        conf: &MempoolConfig,
    ) -> Round {
        // If the node is not scheduled, then it's paused and does not receive broadcasts:
        // mempool receives fresher TKA (via validator sync)
        // while BcastFilter has outdated consensus round.
        // In such case top DAG round follows TKA while Engine round does not advance,
        // DAG eventually shrinks by advancing its bottom and reports it to MempoolAdapter.

        // So `committed` follows both consensus and TKA, and does not stall while Engine can.

        let least_to_keep = (committed - conf.consensus.reset_rounds()).min(
            // consensus for general work and sync:  collator may observe a gap if lags too far
            // TKA for deep sync: consensus round is stalled, history not needed for others
            consensus.max(top_known_anchor) - conf.consensus.max_total_rounds(),
        );
        let remainder = least_to_keep.0 % NodeConfig::get().clean_db_period_rounds.get() as u32;
        (conf.genesis_round).max(least_to_keep - remainder)
    }

    pub fn run(mut self, round_ctx: &RoundCtx) -> Task<Never> {
        let task_ctx = round_ctx.task();
        let round_ctx = round_ctx.clone();

        task_ctx.spawn(async move {
            let mut consensus = self.consensus_round.get();
            let mut committed = self.commit_finished.get();
            let mut top_known = self.top_known_anchor.get();
            let mut prev_least_to_keep =
                Self::least_to_keep(consensus, committed, top_known, round_ctx.conf());
            loop {
                tokio::select! {
                    biased;
                    new_consensus = self.consensus_round.next() => consensus = new_consensus?,
                    new_committed = self.commit_finished.next() => committed = new_committed?,
                    new_top_known = self.top_known_anchor.next() => top_known = new_top_known?,
                }

                metrics::gauge!("tycho_mempool_consensus_current_round").set(consensus.0);
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_top_known")
                    .set(consensus.diff_f64(top_known));
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_committed")
                    .set(consensus.diff_f64(committed));
                metrics::gauge!("tycho_mempool_rounds_committed_ahead_top_known")
                    .set(committed.diff_f64(top_known));

                let new_least_to_keep =
                    Self::least_to_keep(consensus, committed, top_known, round_ctx.conf());
                metrics::gauge!("tycho_mempool_rounds_consensus_ahead_storage_round")
                    .set(consensus.diff_f64(new_least_to_keep));

                if prev_least_to_keep < new_least_to_keep {
                    let db = self.mempool_db.clone();
                    let task = round_ctx.task().spawn_blocking(move || {
                        let mut up_to_exclusive = [0_u8; POINT_KEY_LEN];
                        fill_point_prefix(new_least_to_keep.0, &mut up_to_exclusive);

                        const DB_CLEAN_ERRORS: &str = "tycho_mempool_db_clean_error_count";

                        match db.clean_points(&up_to_exclusive) {
                            Ok(Some((first, last))) => {
                                const CLEANED: &str = "tycho_mempool_rounds_db_cleaned";
                                metrics::gauge!(CLEANED, "kind" => "lower").set(first);
                                metrics::gauge!(CLEANED, "kind" => "upper").set(last);
                                tracing::info!(
                                    "mempool DB cleaned for rounds [{first}..{last}] before {}",
                                    new_least_to_keep.0
                                );
                            }
                            Ok(None) => {
                                tracing::info!(
                                    "mempool DB is already clean before {}",
                                    new_least_to_keep.0
                                );
                            }
                            Err(e) => {
                                metrics::gauge!(DB_CLEAN_ERRORS, "kind" => "points").increment(1);
                                tracing::error!(
                                    "delete range of mempool data before round {} failed: {e}",
                                    new_least_to_keep.0
                                );
                            }
                        }
                        match db.wait_for_compact() {
                            Ok(()) => {}
                            Err(e) => {
                                metrics::gauge!(DB_CLEAN_ERRORS, "kind" => "compact").increment(1);
                                tracing::error!("wait compaction of mempool DB failed: {e}");
                            }
                        };
                    });
                    task.await.inspect_err(|Cancelled()| {
                        tracing::warn!("mempool clean DB task cancelled");
                    })?;
                    prev_least_to_keep = new_least_to_keep;
                }
            }
        })
    }
}
