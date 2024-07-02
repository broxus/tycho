use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::boc::Boc;
use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
use tycho_storage::Storage;
use tycho_util::FastHashMap;
use weedb::rocksdb::WriteBatch;
use weedb::OwnedSnapshot;

use crate::internal_queue::state::state_iterator::{ShardRange, StateIterator, StateIteratorImpl};
use crate::internal_queue::types::{EnqueuedMessage, InternalMessageKey};
use crate::types::IntAdrExt;

// CONFIG

pub struct SessionStateConfig {
    pub storage: Storage,
}

// FACTORY

impl<F, R> SessionStateFactory for F
where
    F: Fn() -> R,
    R: SessionState,
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

impl SessionStateFactory for SessionStateImplFactory {
    type SessionState = SessionStateStdImpl;

    fn create(&self) -> Self::SessionState {
        SessionStateStdImpl::new(self.storage.clone())
    }
}

pub trait SessionStateFactory {
    type SessionState: LocalSessionState;

    fn create(&self) -> Self::SessionState;
}

// TRAIT

#[trait_variant::make(SessionState: Send)]
pub trait LocalSessionState {
    fn add_messages(
        &self,
        shard: ShardIdent,
        messages: &BTreeMap<InternalMessageKey, Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()>;

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
    ) -> Box<dyn StateIterator>;

    fn retrieve_messages(
        &self,
        shard: ShardIdent,
        range: (&InternalMessageKey, &InternalMessageKey),
    ) -> Result<Vec<(u64, HashBytes, i8, HashBytes, Vec<u8>)>>;
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

impl SessionState for SessionStateStdImpl {
    /// write new messages to storage
    fn add_messages(
        &self,
        shard_ident: ShardIdent,
        messages: &BTreeMap<InternalMessageKey, Arc<EnqueuedMessage>>,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();

        for (k, v) in messages.iter() {
            let (lt, hash, workchain, address, cell) = (
                k.lt,
                k.hash,
                v.info.dst.workchain() as i8,
                v.info.dst.get_address(),
                Boc::encode(&v.cell),
            );

            self.storage
                .internal_queue_storage()
                .write_messages_session_batch(
                    &mut batch,
                    shard_ident,
                    lt,
                    hash,
                    workchain,
                    address,
                    cell,
                );
        }

        if messages.len() > 0 {
            self.storage.internal_queue_storage().write_batch(batch)?;
        }

        Ok(())
    }

    fn iterator(
        &self,
        snapshot: &OwnedSnapshot,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
    ) -> Box<dyn StateIterator> {
        let shards = ranges.keys().cloned().collect::<Vec<_>>();
        let iter = self
            .storage
            .internal_queue_storage()
            .build_iterator_session(&snapshot, shards);

        Box::new(StateIteratorImpl::new(iter, receiver, ranges.clone()))
    }

    fn retrieve_messages(
        &self,
        shard: ShardIdent,
        range: (&InternalMessageKey, &InternalMessageKey),
    ) -> Result<Vec<(u64, HashBytes, i8, HashBytes, Vec<u8>)>> {
        let range = ((range.0.lt, range.0.hash), (range.1.lt, range.1.hash));
        self.storage
            .internal_queue_storage()
            .retrieve_and_delete_messages(shard, range)
    }
}
