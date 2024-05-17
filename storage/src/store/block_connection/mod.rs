use everscale_types::models::*;
use parking_lot::RwLock;
use tycho_util::FastDashMap;
use weedb::{ColumnFamily, Table};

use crate::db::*;
use crate::models::*;
use crate::util::*;

/// Stores relations between blocks
pub struct BlockConnectionStorage {
    db: BaseDb,
    next1_subscriptions: FastDashMap<BlockId, Subscriptions>,
    store_next1: RwLock<()>,
}

impl BlockConnectionStorage {
    pub fn new(db: BaseDb) -> Self {
        Self {
            db,
            next1_subscriptions: FastDashMap::default(),
            store_next1: RwLock::new(()),
        }
    }

    pub async fn wait_for_next1(&self, prev_block_id: &BlockId) -> BlockId {
        use dashmap::mapref::entry::Entry;

        let (tx, rx) = tokio::sync::oneshot::channel();

        let guard = self.store_next1.write();

        // Try to load the block data
        if let Some(block_id) = self.load_connection(prev_block_id, BlockConnection::Next1) {
            return block_id;
        }

        // Add subscription for the block and drop the lock
        let idx = match self.next1_subscriptions.entry(*prev_block_id) {
            Entry::Occupied(mut entry) => entry.get_mut().insert(tx),
            Entry::Vacant(entry) => {
                entry.insert(Subscriptions::new(tx));
                0
            }
        };
        drop(guard);

        let guard = SubscriptionGuard {
            next1_subscriptions: &self.next1_subscriptions,
            prev_block_id,
            index: Some(idx),
        };

        // NOTE: Subscriptions must not be dropped without sending a value
        let block_id = rx.await.unwrap();
        guard.disarm();

        block_id
    }

    pub fn store_connection(
        &self,
        handle: &BlockHandle,
        direction: BlockConnection,
        connected_block_id: &BlockId,
    ) {
        let is_next1 = direction == BlockConnection::Next1;
        let guard = if is_next1 {
            Some(self.store_next1.read())
        } else {
            None
        };

        // Use strange match because all columns have different types
        let store = match direction {
            BlockConnection::Prev1 if !handle.meta().has_prev1() => {
                store_block_connection_impl(&self.db.prev1, handle, connected_block_id);
                handle.meta().set_has_prev1()
            }
            BlockConnection::Prev2 if !handle.meta().has_prev2() => {
                store_block_connection_impl(&self.db.prev2, handle, connected_block_id);
                handle.meta().set_has_prev2()
            }
            BlockConnection::Next1 if !handle.meta().has_next1() => {
                store_block_connection_impl(&self.db.next1, handle, connected_block_id);
                handle.meta().set_has_next1()
            }
            BlockConnection::Next2 if !handle.meta().has_next2() => {
                store_block_connection_impl(&self.db.next2, handle, connected_block_id);
                handle.meta().set_has_next2()
            }
            _ => return,
        };

        if is_next1 {
            if let Some((_, subscriptions)) = self.next1_subscriptions.remove(handle.id()) {
                subscriptions.notify(connected_block_id);
            }
        }

        drop(guard);

        if !store {
            return;
        }

        let id = handle.id();

        if handle.is_key_block() {
            let mut write_batch = weedb::rocksdb::WriteBatch::default();

            write_batch.put_cf(
                &self.db.block_handles.cf(),
                id.root_hash.as_slice(),
                handle.meta().to_vec(),
            );
            write_batch.put_cf(
                &self.db.key_blocks.cf(),
                id.seqno.to_be_bytes(),
                id.to_vec(),
            );

            self.db.rocksdb().write(write_batch).unwrap();
        } else {
            self.db
                .block_handles
                .insert(id.root_hash.as_slice(), handle.meta().to_vec())
                .unwrap();
        }
    }

    pub fn load_connection(
        &self,
        block_id: &BlockId,
        direction: BlockConnection,
    ) -> Option<BlockId> {
        match direction {
            BlockConnection::Prev1 => load_block_connection_impl(&self.db.prev1, block_id),
            BlockConnection::Prev2 => load_block_connection_impl(&self.db.prev2, block_id),
            BlockConnection::Next1 => load_block_connection_impl(&self.db.next1, block_id),
            BlockConnection::Next2 => load_block_connection_impl(&self.db.next2, block_id),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BlockConnection {
    Prev1,
    Prev2,
    Next1,
    Next2,
}

struct Subscriptions {
    active: usize,
    slots: Vec<Option<BlockIdTx>>,
}

impl Subscriptions {
    fn new(tx: BlockIdTx) -> Self {
        Self {
            active: 1,
            slots: vec![Some(tx)],
        }
    }

    fn notify(self, block: &BlockId) {
        for tx in self.slots.into_iter().flatten() {
            tx.send(*block).ok();
        }
    }

    fn insert(&mut self, tx: BlockIdTx) -> usize {
        self.active += 1;

        // Reuse existing slot
        for (i, item) in self.slots.iter_mut().enumerate() {
            if item.is_some() {
                continue;
            }
            *item = Some(tx);
            return i;
        }

        // Add new slot
        let idx = self.slots.len();
        self.slots.push(Some(tx));
        idx
    }

    fn remove(&mut self, index: usize) {
        // NOTE: Slot count never decreases
        self.active -= 1;
        self.slots[index] = None;
    }
}

struct SubscriptionGuard<'a> {
    next1_subscriptions: &'a FastDashMap<BlockId, Subscriptions>,
    prev_block_id: &'a BlockId,
    index: Option<usize>,
}

impl SubscriptionGuard<'_> {
    fn disarm(mut self) {
        self.index = None;
    }
}

impl<'a> Drop for SubscriptionGuard<'a> {
    fn drop(&mut self) {
        let Some(index) = self.index else {
            return;
        };

        self.next1_subscriptions
            .remove_if_mut(self.prev_block_id, |_, slots| {
                slots.remove(index);
                slots.active == 0
            });
    }
}

// TODO: Use `Arc<BlockId>` instead?
type BlockIdTx = tokio::sync::oneshot::Sender<BlockId>;

#[inline]
fn store_block_connection_impl<T>(db: &Table<T>, handle: &BlockHandle, block_id: &BlockId)
where
    T: ColumnFamily,
{
    db.insert(
        handle.id().root_hash.as_slice(),
        write_block_id_le(block_id),
    )
    .unwrap();
}

fn load_block_connection_impl<T>(db: &Table<T>, block_id: &BlockId) -> Option<BlockId>
where
    T: ColumnFamily,
{
    let data = db.get(block_id.root_hash.as_slice()).unwrap()?;
    Some(read_block_id_le(&data))
}
