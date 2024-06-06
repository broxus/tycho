mod model;

use anyhow::Result;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::ShardIdent;
use weedb::rocksdb;
use weedb::rocksdb::{SnapshotWithThreadMode, DB};

use crate::db::*;
use crate::store::internal_queue::model::InternalMessagesKey;
use crate::util::StoredValue;

pub struct InternalQueueStorage {
    db: BaseDb,
}

// unsafe fn extend_lifetime<'a>(
//     r: SnapshotWithThreadMode<'a, DB>,
// ) -> SnapshotWithThreadMode<'static, DB> {
//     std::mem::transmute::<SnapshotWithThreadMode<'a, DB>, SnapshotWithThreadMode<'static, DB>>(r)
// }

impl InternalQueueStorage {
    pub fn new(db: BaseDb) -> Self {
        Self { db }
    }

    pub fn snapshot(&self) -> SnapshotWithThreadMode<'static, DB> {
        // unsafe { extend_lifetime(self.db.raw().snapshot()) }
        todo!("SNAPSHOT")
    }

    pub fn add_messages(&self, _id: ShardIdent, messages: &[(u64, HashBytes, Cell)]) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();

        for message in messages.iter() {
            let internal_message_key = InternalMessagesKey {
                lt: message.0,
                hash: message.1,
            };
            batch.put_cf(
                &self.db.internal_messages.cf(),
                internal_message_key.to_vec().as_slice(),
                Boc::encode(message.clone().2),
            );
        }

        self.db.rocksdb().as_ref().write(batch)?;
        Ok(())
    }

    // pub fn store_message(&self, id: &ShardIdent, lt: u64, message: Cell) -> Result<()> {
    //     let internal_messages = &self.db.internal_messages;
    //     let hash = *message.repr_hash();
    //     let internal_message_key = InternalMessagesKey { lt, hash };
    //     let message_content = Boc::encode(message);
    //     internal_messages.insert(internal_message_key.to_vec().as_slice(), message_content)?;
    //     let shards_internal_messages = &self.db.shards_internal_messages;
    //     let key = ShardsInternalMessagesKey {
    //         shard_ident: *id,
    //         lt,
    //     };
    //     shards_internal_messages.insert(key.to_vec().as_slice(), hash)?;
    //     Ok(())
    // }

    pub fn delete_message(
        &self,
        _shard_ident: ShardIdent,
        _lt: u64,
        _hash: HashBytes,
    ) -> Result<()> {
        // delete for messages from shards_internal_messages
        // let key_from = ShardsInternalMessagesKey { shard_ident, lt: 0 };
        // let key_to = ShardsInternalMessagesKey { shard_ident, lt };
        //
        // self.db.raw().delete_range_cf(
        //     &self.db.shards_internal_messages.cf(),
        //     key_from.to_vec().as_slice(),
        //     key_to.to_vec().as_slice(),
        // )?;
        //
        // // remove from internal_messages
        // let key_from = InternalMessagesKey {
        //     lt: 0,
        //     hash: HashBytes::default(),
        // };
        // let key_to = InternalMessagesKey { lt, hash };
        //
        // self.db.raw().delete_range_cf(
        //     &self.db.internal_messages.cf(),
        //     key_from.to_vec().as_slice(),
        //     key_to.to_vec().as_slice(),
        // )?;
        // Ok(())
        //
        Ok(())
    }
}
