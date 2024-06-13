use std::cmp::Ordering;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;

use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey};

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
    pub key: InternalMessageKey,
}

#[derive(Debug, Clone)]
pub struct ShardRange {
    pub shard_id: ShardIdent,
    pub from: Option<InternalMessageKey>,
    pub to: Option<InternalMessageKey>,
}

pub trait StateIterator: Send {
    fn next(&mut self) -> Result<Option<Arc<MessageWithSource>>>;
    fn peek(&self) -> Result<Option<Arc<MessageWithSource>>>;
}
