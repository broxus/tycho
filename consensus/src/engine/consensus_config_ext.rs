use std::num::NonZeroU64;

use anyhow::{Result, ensure};
use tycho_types::models::{ConsensusConfig, GenesisInfo};

use crate::dag::{AnchorStage, WAVE_ROUNDS};

/// ```text
///    RESET_ROUNDS      DagFront.top() == DagHead.next()
///         ↓               ↓
/// |       :        =======|
/// ↑                   ↑
/// MAX_TOTAL_ROUNDS     MIN_FRONT_ROUNDS for front.len()
///
/// Normal: everything committed
/// |       :        =======| front: must never be shorter than MIN_FRONT_ROUNDS
/// |       :          ---  | back: may be shorter than front and have older top
///
/// Normal: back is syncing to commit
/// |       :        =======| front: must be ahead of back without a gap (or may overlap)
/// |     --:----------     | back: total len does not exceed MAX_TOTAL_ROUNDS
///
/// Normal: gap recovered by extending front
/// |     --:--------=======| front: len may exceed MIN_FRONT_ROUNDS and MAX_RESET_ROUNDS
/// |  ---  :               | back: total len does not exceed MAX_TOTAL_ROUNDS
/// => should become (preserving data)
/// |       :        =======| front: shrinks itself
/// |  -----:---------------| back: of total len
///
/// Unrecoverable gap: total len reaches MAX_TOTAL_ROUNDS (any scenario)
/// (assume back is still lagging to commit if we don't know its status)
/// |       :   -----=======| front: no matter if overlaps or has a gap with back
/// |-------:---            | back: front.top() - back.bottom() >= MAX_TOTAL_ROUNDS
/// => should become (with reset of back bottom to drop trailing dag rounds and free mem)
/// |       :        =======| front: creates new dag round chain for back and shrinks itself
/// |       :---------------| back: chain of MAX_RESET_ROUNDS passed from front
/// Dropped tail gives local node time to download other's points as
/// every node cleans its storage with advance of consensus rounds
/// and points far behind consensus will not be downloaded after some time.
/// ```
/// This logic is implemented in  [`DagFront::fill_to_top()`](crate::dag::DagFront::fill_to_top)
/// but other parts of application relies on it
pub trait ConsensusConfigExt {
    fn min_front_rounds(&self) -> u32;

    fn replay_anchor_rounds(&self) -> u32;

    fn reset_rounds(&self) -> u32;

    fn max_total_rounds(&self) -> u32;

    fn min_round_duration_millis(&self) -> NonZeroU64;

    fn validate(&self) -> Result<()>;
}

/// 2 bottommost dag rounds after a dag bottom reset (gap or restore from DB) have invalid points:
/// their dependencies have no `DagRound`; but their dependers are trans-invalid and committable.
/// This offset allow to drop/ignore 2 bottommost rounds of DAG to resume validation and commits.
pub const DAG_ROUNDS_TO_DROP: u32 = 2;

impl ConsensusConfigExt for ConsensusConfig {
    /// includes additional [`DAG_ROUNDS_TO_DROP`] for invalid deps below trans-invalid decay
    fn min_front_rounds(&self) -> u32 {
        // to validate for commit;
        // notice that procedure happens at round start, before new local point's dependencies
        // finished their validation, a new 'next' dag round will appear and so no `-1` below
        3 // new current, includes and witness rounds to validate
            + self.commit_history_rounds.get() as u32 // all committable history for every point
            + DAG_ROUNDS_TO_DROP // not dropped, contain old invalid deps for valid broadcasts
    }

    /// includes additional [`DAG_ROUNDS_TO_DROP`] dag rounds just to be dropped from DAG
    fn replay_anchor_rounds(&self) -> u32 {
        self.commit_history_rounds.get() as u32 // to take full first anchor history
            + self.deduplicate_rounds as u32 // to discard full anchor history after restart
            + DAG_ROUNDS_TO_DROP
    }

    /// includes additional [`DAG_ROUNDS_TO_DROP`] dag rounds just to be dropped from DAG
    fn reset_rounds(&self) -> u32 {
        // we could `-1` to use both top and bottom as inclusive range bounds for lag rounds,
        // but collator may re-request TKA from collator, not only the next one
        self.max_consensus_lag_rounds.get() as u32 // assumed to contain at least one TKA
            + self.replay_anchor_rounds()
    }

    fn max_total_rounds(&self) -> u32 {
        self.sync_support_rounds.get() as u32 // to follow consensus during sync
            + self.reset_rounds()
            - DAG_ROUNDS_TO_DROP // pure config math: ignore implementation detail
    }

    /// applicable only if mempool is configured for stable round rate and it is not paused
    fn min_round_duration_millis(&self) -> NonZeroU64 {
        let value = self.broadcast_retry_millis.get() as u64
        * (self.min_sign_attempts.get() as u64 - 1) // intended: last attempt finishes before t/o
            .max(1) // .. until it is the single attempt which duration is unpredictable
         + 33; // observed duration for last sign attempt and round switch
        value.try_into().expect("math: cannot be zero")
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.commit_history_rounds.get() as u32 >= WAVE_ROUNDS,
            "commit depth must be at least WAVE_ROUNDS={WAVE_ROUNDS}"
        );

        ensure!(
            self.max_consensus_lag_rounds >= self.commit_history_rounds.into(),
            "max consensus lag must be at least commit depth"
        );

        ensure!(
            self.max_total_rounds() >= self.max_consensus_lag_rounds.get() as u32 * 2,
            "max_total_rounds() must include at least two `TopKnownAnchor`s"
        );

        ensure!(
            self.sync_support_rounds.get() as u32 >= DAG_ROUNDS_TO_DROP,
            "sync_support_rounds must include DAG_ROUNDS_TO_DROP={DAG_ROUNDS_TO_DROP}"
        );

        ensure!(
            self.payload_buffer_bytes >= self.payload_batch_bytes,
            "no need to evict cached externals if can send them in one message"
        );

        Ok(())
    }
}

pub trait GenesisInfoExt {
    fn start_round_aligned(&self) -> u32;
}

impl GenesisInfoExt for GenesisInfo {
    fn start_round_aligned(&self) -> u32 {
        AnchorStage::align_genesis(self.start_round).0
    }
}
