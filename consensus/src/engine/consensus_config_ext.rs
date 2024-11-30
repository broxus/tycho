use everscale_types::models::ConsensusConfig;

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
}

impl ConsensusConfigExt for ConsensusConfig {
    fn min_front_rounds(&self) -> u32 {
        // to validate for commit;
        // notice that procedure happens at round start, before new local point's dependencies
        // finished their validation, a new 'next' dag round will appear and so no `-1` below
        3 // new current, includes and witness rounds to validate
            + self.commit_history_rounds as u32 // all committable history for every point
    }

    fn replay_anchor_rounds(&self) -> u32 {
        self.commit_history_rounds as u32 // to take full first anchor history
            + self.deduplicate_rounds as u32 // to discard full anchor history after restart
            + 2 // bottommost includes and witness may not have dag round, so dependers are invalid
            + 2 // invalid but certified dependers are not eligible to be committed
    }

    fn reset_rounds(&self) -> u32 {
        // we could `-1` to use both top and bottom as inclusive range bounds for lag rounds,
        // but collator may re-request TKA from collator, not only the next one
        self.max_consensus_lag_rounds as u32 // to collate
            + self.commit_history_rounds as u32 // to take full first anchor history
            + self.deduplicate_rounds as u32 // to discard full anchor history after restart
            + 2 // includes and witness will not have dag round, so dependers are invalid
            + 2 // invalid but certified dependers are not eligible to be committed
    }

    fn max_total_rounds(&self) -> u32 {
        // we could `-1` to use both top and bottom as inclusive range bounds for lag rounds,
        // but collator may re-request TKA from collator, not only the next one
        self.sync_support_rounds as u32 // to follow consensus during sync
            + self.max_consensus_lag_rounds as u32 // to collate
            + self.commit_history_rounds as u32 // to take full first anchor history
            + self.deduplicate_rounds as u32 // to discard full anchor history after restart
    }
}
