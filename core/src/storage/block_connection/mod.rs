use everscale_types::models::*;
use parking_lot::RwLock;
use tycho_storage::kv::StoredValue;

use super::block_handle::{BlockFlags, BlockHandle};
use super::db::CoreDb;
use super::tables;
use super::util::{read_block_id_le, write_block_id_le, SlotSubscriptions};

/// Stores relations between blocks
pub struct BlockConnectionStorage {
    db: CoreDb,
    next1_subscriptions: SlotSubscriptions<BlockId, BlockId>,
    store_next1: RwLock<()>,
}

impl BlockConnectionStorage {
    pub fn new(db: CoreDb) -> Self {
        Self {
            db,
            next1_subscriptions: Default::default(),
            store_next1: RwLock::new(()),
        }
    }

    pub async fn wait_for_next1(&self, prev_block_id: &BlockId) -> BlockId {
        let guard = self.store_next1.write();

        // Try to load the block data
        if let Some(block_id) = self.load_connection(prev_block_id, BlockConnection::Next1) {
            return block_id;
        }

        // Add subscription for the block and drop the lock
        let rx = self.next1_subscriptions.subscribe(prev_block_id);

        // NOTE: Guard must be dropped before awaiting
        drop(guard);

        rx.await
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

        let store = {
            let flag = direction.as_flag();
            if handle.meta().flags().contains(flag) {
                return;
            }

            let key = make_key(handle.id(), direction);
            self.db
                .block_connections
                .insert(key, write_block_id_le(connected_block_id))
                .unwrap();

            handle.meta().add_flags(flag)
        };

        if is_next1 {
            self.next1_subscriptions
                .notify(handle.id(), connected_block_id);
        }

        drop(guard);

        if !store {
            return;
        }

        let id = handle.id();

        let block_handle_cf = &self.db.block_handles.cf();
        let rocksdb = self.db.rocksdb();

        rocksdb
            .merge_cf_opt(
                block_handle_cf,
                id.root_hash.as_slice(),
                handle.meta().to_vec(),
                self.db.block_handles.write_config(),
            )
            .unwrap();
    }

    pub fn load_connection(
        &self,
        block_id: &BlockId,
        direction: BlockConnection,
    ) -> Option<BlockId> {
        let key = make_key(block_id, direction);
        let data = self.db.block_connections.get(key).unwrap()?;
        Some(read_block_id_le(&data))
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BlockConnection {
    Prev1 = 0,
    Prev2 = 1,
    Next1 = 2,
    Next2 = 3,
}

impl BlockConnection {
    const fn as_flag(self) -> BlockFlags {
        match self {
            Self::Prev1 => BlockFlags::HAS_PREV_1,
            Self::Prev2 => BlockFlags::HAS_PREV_2,
            Self::Next1 => BlockFlags::HAS_NEXT_1,
            Self::Next2 => BlockFlags::HAS_NEXT_2,
        }
    }
}

fn make_key(block_id: &BlockId, ty: BlockConnection) -> [u8; tables::BlockConnections::KEY_LEN] {
    let mut key = [0u8; tables::BlockConnections::KEY_LEN];
    key[..4].copy_from_slice(&block_id.shard.workchain().to_be_bytes());
    key[4..12].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
    key[12..16].copy_from_slice(&block_id.seqno.to_be_bytes());
    key[16..48].copy_from_slice(block_id.root_hash.as_slice());
    key[48] = ty as u8;
    key
}
