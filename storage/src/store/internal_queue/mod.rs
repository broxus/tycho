mod model;

use std::hash::Hasher;
use std::sync::Arc;

use everscale_types::cell::Store;
use everscale_types::models::*;

use crate::db::*;
use crate::rocksdb::SnapshotWithThreadMode;
use crate::util::{StoredValue, StoredValueBuffer};

pub struct InternalQueueStorage {
    db: BaseDb,
}

// unsafe fn extend_lifetime<'a>(
//     r: SnapshotWithThreadMode<'a, BaseDb>,
// ) -> SnapshotWithThreadMode<'static, BaseDb> {
//     std::mem::transmute::<SnapshotWithThreadMode<'a, BaseDb>, SnapshotWithThreadMode<'static, BaseDb>>(r)
// }

impl InternalQueueStorage {
    pub fn new(db: BaseDb) -> Self {
        Self { db }
    }
    //
    // pub fn snapshot(&self) -> SnapshotWithThreadMode<'static, BaseDb> {
    //     unsafe { extend_lifetime(self.db.raw().snapshot()) }
    // }
    //
    // pub fn store_message(&self, id: &ShardIdent, lt: u64, message: Cell) -> Result<()> {
    //     let internal_messages = &self.db.internal_messages;
    //
    //     let hash = *message.repr_hash();
    //
    //     let internal_message_key = InternalMessagesKey { lt, hash };
    //
    //     let message_content = Boc::encode(message);
    //
    //     internal_messages.insert(internal_message_key.to_vec().as_slice(), message_content)?;
    //
    //     let shards_internal_messages = &self.db.shards_internal_messages;
    //
    //     let key = ShardsInternalMessagesKey {
    //         shard_ident: *id,
    //         lt,
    //     };
    //
    //     shards_internal_messages.insert(key.to_vec().as_slice(), hash)?;
    //
    //     Ok(())
    // }
}
