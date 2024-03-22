use anyhow::Result;
use async_trait::async_trait;

use everscale_types::models::{BlockId, ShardIdent};

// TYPES

mod type_stubs {
    use std::sync::Arc;

    use everscale_types::models::ShardIdent;

    pub trait Queue {
        fn new(base_shard: ShardIdent) -> Self;
    }
    pub struct QueueImpl;
    impl Queue for QueueImpl {
        fn new(base_shard: ShardIdent) -> Self {
            Self {}
        }
    }
    pub trait QueueIterator {}
    pub struct QueueIteratorImpl;
    impl QueueIterator for QueueIteratorImpl {}
    pub struct QueueDiff;

    pub struct IterItem {
        pub message: Arc<EnqueuedMessage>,
        pub is_new: bool,
    }

    pub type Lt = u64;
    pub type MessageHash = UInt256;

    pub struct EnqueuedMessage {
        pub created_lt: Lt,
        pub enqueued_lt: Lt,
        pub hash: MessageHash,
        pub env: MessageEnvelope,
    }

    #[derive(Hash)]
    pub struct MessageEnvelope {
        pub message: MessageContent,
        pub from_contract: Address,
        pub to_contract: Address,
    }

    pub type Address = String;
    pub type UInt256 = String;

    #[derive(Hash)]
    pub struct MessageContent {}
}
pub use type_stubs::*;

// ADAPTER

#[async_trait]
pub(crate) trait MessageQueueAdapter: Send + Sync + 'static {
    fn new() -> Self;
    /// Create iterator for specified shard and return it
    async fn get_iterator<QI>(&self, shard_id: ShardIdent) -> Result<QI>
    where
        QI: QueueIterator;
    /// Apply diff to the current queue session state (waiting for the operation to complete)
    async fn apply_diff(&self, diff: QueueDiff) -> Result<()>;
    /// Commit previously applied diff, saving changes to persistent state (waiting for the operation to complete).
    /// Return `None` if specified diff does not exist.
    async fn commit_diff(&self, diff_id: BlockId) -> Result<Option<()>>;
}

pub(crate) struct MessageQueueAdapterStdImpl<MQ>
where
    MQ: Queue,
{
    queue: MQ,
}

#[async_trait]
impl<MQ> MessageQueueAdapter for MessageQueueAdapterStdImpl<MQ>
where
    MQ: Queue + Send + Sync + 'static,
{
    fn new() -> Self {
        let base_shard = ShardIdent::new_full(0);
        Self {
            queue: MQ::new(base_shard),
        }
    }
    async fn get_iterator<QI>(&self, shard_id: ShardIdent) -> Result<QI>
    where
        QI: QueueIterator,
    {
        todo!()
    }
    async fn apply_diff(&self, diff: QueueDiff) -> Result<()> {
        todo!()
    }
    async fn commit_diff(&self, diff_id: BlockId) -> Result<Option<()>> {
        todo!()
    }
}
