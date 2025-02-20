use std::sync::Arc;

use everscale_types::models::{CollationConfig, ShardIdent};
use tycho_util::FastHashMap;

use super::{BlockCollationData, PrevData};
use crate::internal_queue::types::{DiffStatistics, PartitionRouter};
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
    pub diffs_info: FastHashMap<ShardIdent, (PartitionRouter, DiffStatistics)>,
}
