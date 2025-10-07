use std::sync::Arc;

use tycho_types::models::{CollationConfig, ShardIdent};
use tycho_util::sync::CancellationFlag;

use super::{BlockCollationData, PrevData};
use crate::collator::do_collate::work_units::DoCollateWu;
use crate::internal_queue::types::ranges::QueueShardBoundedRange;
use crate::types::McData;

pub struct Phase<S: PhaseState> {
    pub state: Box<ActualState>,
    pub extra: S,
}

pub trait PhaseState {}

pub struct ActualState {
    pub collation_config: Arc<CollationConfig>,
    pub collation_data: Box<BlockCollationData>,
    pub mc_data: Arc<McData>,
    pub prev_shard_data: PrevData,
    pub shard_id: ShardIdent,
    /// For graceful collation cancellation
    pub collation_is_cancelled: CancellationFlag,
    /// Indicates if current collating block is first
    /// after previous master block
    pub is_first_block_after_prev_master: bool,
    pub part_stat_ranges: Option<Vec<QueueShardBoundedRange>>,
    pub do_collate_wu: DoCollateWu,
}
