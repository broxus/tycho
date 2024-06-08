pub(crate) mod model;

use anyhow::{bail, Result};
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, HashBytes, Load};
use everscale_types::models::{IntAddr, Message, MsgInfo, ShardIdent};
pub use model::InternalMessageKey;
use weedb::rocksdb::WriteBatch;
use weedb::OwnedSnapshot;

use crate::db::*;
use crate::store::internal_queue::model::ShardsInternalMessagesKey;
use crate::util::{OwnedIterator, StoredValue};

pub struct InternalQueueStorage {
    db: BaseDb,
}

impl InternalQueueStorage {
    pub fn new(db: BaseDb) -> Self {
        Self { db }
    }

    pub fn snapshot(&self) -> OwnedSnapshot {
        self.db.owned_snapshot()
    }

    pub fn build_iterator(&self, snapshot: &OwnedSnapshot) -> OwnedIterator {
        let mut readopts = self.db.internal_messages.new_read_config();

        readopts.set_snapshot(snapshot);

        let internal_messages_cf = self.db.internal_messages.cf();

        let mut iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&internal_messages_cf, readopts);

        iter.seek_to_first();

        OwnedIterator::new(iter, self.db.rocksdb().clone())
    }

    /// Inserts messages into the database.
    pub fn insert_messages(
        &self,
        shard_ident: ShardIdent,
        messages: &[(u64, HashBytes, Cell)],
    ) -> Result<()> {
        let mut batch_internal_messages = WriteBatch::default();
        let mut batch_shards_internal_messages = WriteBatch::default();

        for (lt, hash, cell) in messages {
            let internal_message_key = InternalMessageKey {
                lt: *lt,
                hash: *hash,
                shard_ident,
            };

            batch_internal_messages.put_cf(
                &self.db.internal_messages.cf(),
                internal_message_key.to_vec().as_slice(),
                Boc::encode(cell.clone()),
            );

            let shard_internal_message_key = ShardsInternalMessagesKey {
                shard_ident,
                lt: *lt,
            };

            batch_shards_internal_messages.put_cf(
                &self.db.shards_internal_messages.cf(),
                shard_internal_message_key.to_vec().as_slice(),
                hash.as_slice(),
            );
        }

        self.db.rocksdb().as_ref().write(batch_internal_messages)?;
        self.db
            .rocksdb()
            .as_ref()
            .write(batch_shards_internal_messages)?;

        Ok(())
    }

    /// Deletes messages from the database.
    pub fn delete_messages(
        &self,
        source: ShardIdent,
        receiver: ShardIdent,
        lt_from: u64,
        lt_to: u64,
    ) -> Result<()> {
        let mut readopts = self.db.shards_internal_messages.new_read_config();
        let snapshot = self.snapshot();
        readopts.set_snapshot(&snapshot);

        let start_key = ShardsInternalMessagesKey {
            shard_ident: source,
            lt: lt_from,
        };

        let shards_internal_messages_cf = self.db.shards_internal_messages.cf();
        let mut iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&shards_internal_messages_cf, readopts);

        iter.seek(&start_key.to_vec());

        let mut batch = WriteBatch::default();
        let internal_messages_cf = self.db.internal_messages.cf();

        while iter.valid() {
            let (mut key, value) = match (iter.key(), iter.value()) {
                (Some(key), Some(value)) => (key, value),
                _ => break,
            };

            let key = ShardsInternalMessagesKey::deserialize(&mut key);
            if key.lt > lt_to {
                break;
            }

            let cell = Boc::decode(value)?;
            let hash = cell.repr_hash();
            let base_message = Message::load_from(&mut cell.as_slice()?)?;
            let dest = match base_message.info {
                MsgInfo::Int(int_msg_info) => int_msg_info.dst,
                _ => bail!("Expected internal message"),
            };

            let dest_addr = match dest {
                IntAddr::Std(addr) => addr,
                IntAddr::Var(_) => bail!("Expected standard address"),
            };

            if receiver.contains_account(&dest_addr.address) {
                iter.next();
                continue;
            }

            let internal_messages_key = InternalMessageKey {
                lt: key.lt,
                hash: *hash,
                shard_ident: source,
            };

            batch.delete_cf(&internal_messages_cf, &internal_messages_key.to_vec());
            batch.delete_cf(&shards_internal_messages_cf, &key.to_vec());
            iter.next();
        }

        self.db.rocksdb().as_ref().write(batch)?;
        Ok(())
    }
}
