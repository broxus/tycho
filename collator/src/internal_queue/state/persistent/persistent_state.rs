use std::collections::BTreeMap;
use std::sync::Arc;

use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::{BlockIdShort, IntAddr, ShardIdent};
use tycho_storage::Storage;
use tycho_util::FastHashMap;

use crate::internal_queue::state::persistent::persistent_state_iterator::PersistentStateIterator;
use crate::internal_queue::state::state_iterator::{ShardRange, StateIterator};
use crate::internal_queue::types::EnqueuedMessage;
use crate::types::IntAdrExt;

// CONFIG

pub struct PersistentStateConfig {
    pub storage: Storage,
}

// FACTORY

impl<F, R> PersistentStateFactory for F
where
    F: Fn() -> R,
    R: PersistentState,
{
    type PersistentState = R;

    fn create(&self) -> Self::PersistentState {
        self()
    }
}

pub struct PersistentStateImplFactory {
    pub storage: Storage,
}

impl PersistentStateImplFactory {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl PersistentStateFactory for PersistentStateImplFactory {
    type PersistentState = PersistentStateStdImpl;

    fn create(&self) -> Self::PersistentState {
        PersistentStateStdImpl::new(self.storage.clone())
    }
}

pub trait PersistentStateFactory {
    type PersistentState: LocalPersistentState;

    fn create(&self) -> Self::PersistentState;
}

// TRAIT

#[trait_variant::make(PersistentState: Send)]
pub trait LocalPersistentState {
    fn add_messages(
        &self,
        block_id_short: BlockIdShort,
        messages: Vec<Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()>;

    fn iterator(
        &self,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
    ) -> Box<dyn StateIterator>;

    fn delete_messages(
        &self,
        shard: ShardIdent,
        delete_until: BTreeMap<ShardIdent, (u64, HashBytes)>,
    ) -> anyhow::Result<()>;
}

// IMPLEMENTATION

pub struct PersistentStateStdImpl {
    storage: Storage,
}

impl PersistentStateStdImpl {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl PersistentState for PersistentStateStdImpl {
    fn add_messages(
        &self,
        block_id_short: BlockIdShort,
        messages: Vec<Arc<EnqueuedMessage>>,
    ) -> anyhow::Result<()> {
        let messages: Vec<(u64, HashBytes, HashBytes, Cell, HashBytes, i8)> = messages
            .iter()
            .map(|m| {
                let dest_address = &m.info.dst;
                let (address, workchain) = match dest_address {
                    IntAddr::Std(std) => (std.address, std.workchain),
                    IntAddr::Var(_) => {
                        panic!("Var address not supported")
                    }
                };

                (
                    m.info.created_lt,
                    m.hash,
                    m.info.dst.get_address(),
                    m.cell.clone(),
                    address,
                    workchain,
                )
            })
            .collect();
        self.storage
            .internal_queue_storage()
            .insert_messages(block_id_short.shard, &messages)?;
        Ok(())
    }

    fn iterator(
        &self,
        receiver: ShardIdent,
        ranges: &FastHashMap<ShardIdent, ShardRange>,
    ) -> Box<dyn StateIterator> {
        let snapshot = self.storage.internal_queue_storage().snapshot();

        let iter = self
            .storage
            .internal_queue_storage()
            .build_iterator(&snapshot);

        Box::new(PersistentStateIterator::new(iter, receiver, ranges.clone()))
    }

    fn delete_messages(
        &self,
        receiver: ShardIdent,
        delete_until: BTreeMap<ShardIdent, (u64, HashBytes)>,
    ) -> anyhow::Result<()> {
        for (shard, delete_until) in delete_until.iter() {
            self.storage.internal_queue_storage().delete_messages(
                *shard,
                *delete_until,
                receiver,
            )?;
        }
        Ok(())
    }
}
