use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use everscale_types::models::ShardIdent;

use crate::internal_queue::error::QueueError;
use crate::internal_queue::types::{EnqueuedMessage, Lt};

#[derive(Debug, Clone, Eq)]
pub struct MessageWithSource {
    pub shard_id: ShardIdent,
    pub message: Arc<EnqueuedMessage>,
}

impl MessageWithSource {
    pub fn new(shard_id: ShardIdent, message: Arc<EnqueuedMessage>) -> Self {
        MessageWithSource { shard_id, message }
    }
}

impl PartialEq<Self> for MessageWithSource {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}

impl PartialOrd<Self> for MessageWithSource {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.message.cmp(&other.message))
    }
}

impl Ord for MessageWithSource {
    fn cmp(&self, other: &Self) -> Ordering {
        self.message.cmp(&other.message)
    }
}

#[derive(Debug, Clone)]
pub struct IterRange {
    pub shard_id: ShardIdent,
    pub lt: Lt,
}

#[derive(Debug)]
pub struct ShardRange {
    pub shard_id: ShardIdent,
    pub from_lt: Option<Lt>,
    pub to_lt: Option<Lt>,
}

pub trait StateSnapshot: Send {
    fn get_outgoing_messages_by_shard(
        &self,
        shards: &mut HashMap<ShardIdent, ShardRange>,
        shard_id: &ShardIdent,
    ) -> Result<Vec<Arc<MessageWithSource>>, QueueError>;

    fn next(
        &self,
        shard_range: &HashMap<ShardIdent, ShardRange>,
        for_shard: &ShardIdent,
    ) -> Option<Arc<MessageWithSource>>;
}
