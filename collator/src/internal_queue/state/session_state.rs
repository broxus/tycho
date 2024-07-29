use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::ShardIdent;
use tycho_storage::Storage;
use tycho_util::FastHashMap;
use weedb::rocksdb::WriteBatch;
use weedb::OwnedSnapshot;

use crate::internal_queue::state::state_iterator::{StateIterator, StateIteratorImpl};
use crate::internal_queue::types::{InternalMessageKey, InternalMessageValue};

// CONFIG

pub struct SessionStateConfig {
    pub storage: Storage,
}

// FACTORY

impl<F, R, V> SessionStateFactory<V> for F
where
    F: Fn() -> R,
    R: SessionState<V>,
    V: InternalMessageValue,
{
    type SessionState = R;

    fn create(&self) -> Self::SessionState {
        self()
    }
}

pub struct SessionStateImplFactory {
    pub storage: Storage,
}

impl SessionStateImplFactory {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> SessionStateFactory<V> for SessionStateImplFactory {
    type SessionState = SessionStateStdImpl;

    fn create(&self) -> Self::SessionState {
        SessionStateStdImpl::new(self.storage.clone())
    }
}

pub trait SessionStateFactory<V: InternalMessageValue> {
    type SessionState: LocalSessionState<V>;

    fn create(&self) -> Self::SessionState;
}

// TRAIT

#[trait_variant::make(SessionState: Send)]
pub trait LocalSessionState<V: InternalMessageValue> {
    fn add_messages(
        &self,
        shard: ShardIdent,
        messages: &BTreeMap<InternalMessageKey, Arc<V>>,
    ) -> anyhow::Result<()>;

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, (InternalMessageKey, InternalMessageKey)>,
    ) -> Box<dyn StateIterator<V>>;

    fn commit_messages(
        &self,
        shard: ShardIdent,
        from: &InternalMessageKey,
        to: &InternalMessageKey,
    ) -> Result<()>;
}

// IMPLEMENTATION

pub struct SessionStateStdImpl {
    storage: Storage,
}

impl SessionStateStdImpl {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> SessionState<V> for SessionStateStdImpl {
    /// write new messages to storage
    fn add_messages(
        &self,
        shard_ident: ShardIdent,
        messages: &BTreeMap<InternalMessageKey, Arc<V>>,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();

        for (internal_message_key, message) in messages.iter() {
            self.storage
                .internal_queue_storage()
                .insert_message_session(
                    &mut batch,
                    tycho_storage::model::ShardsInternalMessagesKey::new(
                        shard_ident,
                        internal_message_key.clone().into(),
                    ),
                    message.destination(),
                    &message.serialize()?,
                )?;
        }

        if !messages.is_empty() {
            self.storage.internal_queue_storage().write_batch(batch)?;
        }

        Ok(())
    }

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, (InternalMessageKey, InternalMessageKey)>,
    ) -> Box<dyn StateIterator<V>> {
        let mut shard_iters = BTreeMap::new();
        let shards = ranges.keys().cloned().collect::<Vec<_>>();
        for shard in shards {
            let iter = self
                .storage
                .internal_queue_storage()
                .build_iterator_session(snapshot);
            shard_iters.insert(shard, iter);
        }

        Box::new(StateIteratorImpl::new(
            shard_iters,
            receiver,
            ranges.clone(),
        ))
    }

    fn commit_messages(
        &self,
        shard: ShardIdent,
        from: &InternalMessageKey,
        to: &InternalMessageKey,
    ) -> Result<()> {
        self.storage
            .internal_queue_storage()
            .commit(shard, from.clone().into(), to.clone().into())
    }
}
