use std::sync::Arc;

use everscale_types::models::*;

use crate::db::*;
use crate::models::*;
use crate::util::*;

/// Stores relations between blocks
pub struct BlockConnectionStorage {
    db: Arc<Db>,
}

impl BlockConnectionStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    pub fn store_connection(
        &self,
        handle: &BlockHandle,
        direction: BlockConnection,
        connected_block_id: &BlockId,
    ) {
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

            self.db.raw().write(write_batch).unwrap();
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
