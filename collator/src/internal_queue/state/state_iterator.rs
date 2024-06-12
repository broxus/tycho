use std::cmp::Ordering;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;
use everscale_types::prelude::HashBytes;

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
pub struct IterRangeFrom {
    pub shard_id: ShardIdent,
    pub lt: Lt,
    pub hash: HashBytes,
}

#[derive(Debug, Clone)]
pub struct IterRangeTo {
    pub shard_id: ShardIdent,
    pub lt: Lt,
}

#[derive(Debug, Clone)]
pub struct ShardRange {
    pub shard_id: ShardIdent,
    pub from_lt: Option<Lt>,
    pub from_hash: Option<HashBytes>,
    pub to_lt: Option<Lt>,
}

pub trait StateIterator: Send {
    fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>>;
    fn peek(&self) -> Result<Option<Arc<MessageWithSource>>>;
}
