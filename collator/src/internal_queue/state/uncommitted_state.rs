use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::HashMapExt;
use anyhow::Result;
use everscale_types::models::ShardIdent;
use tycho_block_util::queue::QueueKey;
use tycho_storage::Storage;
use tycho_util::FastHashMap;
use weedb::rocksdb::WriteBatch;
use weedb::OwnedSnapshot;

use crate::internal_queue::state::state_iterator::{
    ShardIteratorWithRange, StateIterator, StateIteratorImpl,
};
use crate::internal_queue::types::InternalMessageValue;

// CONFIG

pub struct UncommittedStateConfig {
    pub storage: Storage,
}

// FACTORY

impl<F, R, V> UncommittedStateFactory<V> for F
where
    F: Fn() -> R,
    R: UncommittedState<V>,
    V: InternalMessageValue,
{
    type UncommittedState = R;

    fn create(&self) -> Self::UncommittedState {
        self()
    }
}

pub struct UncommittedStateImplFactory {
    pub storage: Storage,
}

impl UncommittedStateImplFactory {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> UncommittedStateFactory<V> for UncommittedStateImplFactory {
    type UncommittedState = UncommittedStateStdImpl;

    fn create(&self) -> Self::UncommittedState {
        UncommittedStateStdImpl::new(self.storage.clone())
    }
}

pub trait UncommittedStateFactory<V: InternalMessageValue> {
    type UncommittedState: LocalUncommittedState<V>;

    fn create(&self) -> Self::UncommittedState;
}

// TRAIT

#[trait_variant::make(UncommittedState: Send)]
pub trait LocalUncommittedState<V: InternalMessageValue> {
    fn add_messages(&self, source: ShardIdent, messages: &BTreeMap<QueueKey, Arc<V>>)
        -> Result<()>;

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
    ) -> Box<dyn StateIterator<V>>;

    fn commit_messages(&self, ranges: &FastHashMap<ShardIdent, QueueKey>) -> Result<()>;
    fn truncate(&self) -> Result<()>;
}

// IMPLEMENTATION

pub struct UncommittedStateStdImpl {
    storage: Storage,
}

impl UncommittedStateStdImpl {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> UncommittedState<V> for UncommittedStateStdImpl {
    /// write new messages to storage
    fn add_messages(
        &self,
        source: ShardIdent,
        messages: &BTreeMap<QueueKey, Arc<V>>,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();

        for (internal_message_key, message) in messages.iter() {
            self.storage
                .internal_queue_storage()
                .insert_message_uncommitted(
                    &mut batch,
                    tycho_storage::model::ShardsInternalMessagesKey::new(
                        source,
                        *internal_message_key,
                    ),
                    message.destination(),
                    &message.serialize()?,
                )?;
        }

        self.storage.internal_queue_storage().write_batch(batch)?;

        Ok(())
    }

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, (QueueKey, QueueKey)>,
    ) -> Box<dyn StateIterator<V>> {
        let mut shard_iters_with_ranges = FastHashMap::with_capacity(ranges.len());

        for (&shard, (start, end)) in ranges {
            let iter = self
                .storage
                .internal_queue_storage()
                .build_iterator_uncommitted(snapshot);

            shard_iters_with_ranges.insert(shard, ShardIteratorWithRange::new(iter, *start, *end));
        }

        Box::new(StateIteratorImpl::new(shard_iters_with_ranges, receiver))
    }

    fn commit_messages(&self, ranges: &FastHashMap<ShardIdent, QueueKey>) -> Result<()> {
        let ranges = ranges.iter().map(|(shard, key)| (*shard, *key)).collect();
        self.storage.internal_queue_storage().commit(ranges)
    }

    fn truncate(&self) -> Result<()> {
        self.storage
            .internal_queue_storage()
            .clear_uncommitted_queue()
    }
}