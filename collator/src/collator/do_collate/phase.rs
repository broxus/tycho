use std::sync::Arc;

use everscale_types::models::{CollationConfig, ShardIdent};
use tycho_block_util::queue::QueueKey;
use tycho_util::sync::CancellationFlag;
use tycho_util::FastHashMap;

use super::{BlockCollationData, PrevData};
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
    pub load_statistics_params: FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
    pub is_first_block_or_masterchain: bool,
}
