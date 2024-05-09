use std::collections::HashMap;
use std::sync::Arc;

use everscale_types::models::ShardIdent;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::snapshot::{MessageWithSource, ShardRange, StateSnapshot};

pub struct PersistentStateSnapshot {}

impl StateSnapshot for PersistentStateSnapshot {
    fn get_outgoing_messages_by_shard(
        &self,
        _shards: &mut HashMap<ShardIdent, ShardRange>,
        _shard_id: &ShardIdent,
    ) -> Result<Vec<Arc<MessageWithSource>>, QueueError> {
        Ok(vec![])
    }
}
