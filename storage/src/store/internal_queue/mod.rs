mod model;

use anyhow::Result;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::ShardIdent;
use weedb::rocksdb::{
    DBCommon, DBIterator, DBRawIteratorWithThreadMode, IteratorMode, MultiThreaded, ReadOptions,
    SnapshotWithThreadMode, DB,
};
use weedb::{rocksdb, OwnedSnapshot};

use crate::db::*;
use crate::store::internal_queue::model::InternalMessagesKey;
use crate::util::StoredValue;

pub struct InternalQueueStorage {
    db: BaseDb,
}

impl InternalQueueStorage {
    pub fn new(db: BaseDb) -> Self {
        Self { db }
    }

    pub fn snapshot(&self) -> OwnedSnapshot {
        let snapshot = self.db.owned_snapshot();
        snapshot
    }

    pub fn iterator_readopts(&self, snapshot: &OwnedSnapshot) -> ReadOptions {
        // let start_key = InternalMessagesKey {
        //     lt: 0,
        //     hash: HashBytes::default(),
        // };
        // let iterator = snapshot.iterator_cf_opt(
        //     &self.db.internal_messages.cf(),
        //     Default::default(),
        //     IteratorMode::From(start_key.to_vec().as_slice(), rocksdb::Direction::Forward),
        // );
        // let iterator = unsafe { Self::extend_lifetime(iterator) };

        let mut readopts = self.db.internal_messages.new_read_config();

        readopts.set_snapshot(snapshot);

        readopts
    }

    pub fn insert_messages(
        &self,
        shard_ident: ShardIdent,
        messages: &Vec<(u64, HashBytes, Cell)>,
    ) -> Result<()> {
        let mut batch_internal_messages = rocksdb::WriteBatch::default();
        let mut batch_shards_internal_messages = rocksdb::WriteBatch::default();

        for message in messages.iter() {
            let internal_message_key = InternalMessagesKey {
                lt: message.0,
                hash: message.1,
                shard_ident,
            };
            batch_internal_messages.put_cf(
                &self.db.internal_messages.cf(),
                internal_message_key.to_vec().as_slice(),
                Boc::encode(message.clone().2),
            );

            let shard_internal_message_key = model::ShardsInternalMessagesKey {
                shard_ident,
                lt: message.0,
            };

            batch_shards_internal_messages.put_cf(
                &self.db.shards_internal_messages.cf(),
                shard_internal_message_key.to_vec().as_slice(),
                message.1.as_slice(),
            );
        }

        self.db.rocksdb().as_ref().write(batch_internal_messages)?;
        self.db
            .rocksdb()
            .as_ref()
            .write(batch_shards_internal_messages)?;

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
