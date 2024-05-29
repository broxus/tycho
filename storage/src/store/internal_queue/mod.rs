mod model;

use everscale_types::models::ShardIdent;
use weedb::rocksdb::{DB, SnapshotWithThreadMode};
use crate::db::*;
use crate::store::internal_queue::model::InternalMessagesKey;
use anyhow::Result;
use everscale_types::cell::Cell;

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

    pub fn store_message(&self, id: &ShardIdent, lt: u64, message: Cell) -> Result<()> {
        Ok(())
        // let internal_messages = &self.db.internal_messages;
        //
        // let hash = *message.repr_hash();
        //
        // let internal_message_key = InternalMessagesKey { lt, hash };
        //
        // let message_content = Boc::encode(message);
        //
        // internal_messages.insert(internal_message_key.to_vec().as_slice(), message_content)?;
        //
        // let shards_internal_messages = &self.db.shards_internal_messages;
        //
        // let key = ShardsInternalMessagesKey {
        //     shard_ident: *id,
        //     lt,
        // };
        //
        // shards_internal_messages.insert(key.to_vec().as_slice(), hash)?;
        //
        // Ok(())
    }
}
