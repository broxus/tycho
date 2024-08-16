use anyhow::Result;
use everscale_types::models::{IntAddr, ShardIdent};
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;
use weedb::rocksdb::{ReadOptions, WriteBatch};
use weedb::{BoundedCfHandle, OwnedSnapshot};

use crate::db::*;
use crate::model::ShardsInternalMessagesKey;
use crate::util::{OwnedIterator, StoredValue};

pub mod model;

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

    pub fn build_iterator_persistent(&self, snapshot: &OwnedSnapshot) -> OwnedIterator {
        self.build_iterator(
            self.db.shards_internal_messages.cf(),
            self.db.shards_internal_messages.new_read_config(),
            snapshot,
        )
    }

    pub fn build_iterator_session(&self, snapshot: &OwnedSnapshot) -> OwnedIterator {
        self.build_iterator(
            self.db.shards_internal_messages_session.cf(),
            self.db.shards_internal_messages_session.new_read_config(),
            snapshot,
        )
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db.rocksdb().write(batch)?;
        Ok(())
    }

    pub fn insert_message_session(
        &self,
        batch: &mut WriteBatch,
        key: ShardsInternalMessagesKey,
        dest: &IntAddr,
        value: &[u8],
    ) -> Result<()> {
        let cf = self.db.shards_internal_messages_session.cf();
        let dest_workchain = dest.workchain() as i8;
        let dest_prefix = dest.prefix();
        Self::insert_message(batch, cf, key, dest_workchain, dest_prefix, value)
    }

    fn build_iterator(
        &self,
        cf: BoundedCfHandle<'_>,
        mut read_config: ReadOptions,
        snapshot: &OwnedSnapshot,
    ) -> OwnedIterator {
        read_config.set_snapshot(snapshot);
        let iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        OwnedIterator::new(iter, self.db.rocksdb().clone())
    }

    fn insert_message(
        batch: &mut WriteBatch,
        cf: BoundedCfHandle<'_>,
        key: ShardsInternalMessagesKey,
        dest_workchain: i8,
        dest_prefix: u64,
        cell: &[u8],
    ) -> Result<()> {
        let mut buffer = Vec::with_capacity(1 + 8 + cell.len());

        unsafe {
            let ptr = buffer.as_mut_ptr();

            std::ptr::copy_nonoverlapping(dest_workchain.to_be_bytes().as_ptr(), ptr, 1);

            std::ptr::copy_nonoverlapping(dest_prefix.to_be_bytes().as_ptr(), ptr.add(1), 8);

            std::ptr::copy_nonoverlapping(cell.as_ptr(), ptr.add(1 + 8), cell.len());

            buffer.set_len(1 + 8 + cell.len());
        }

        batch.put_cf(&cf, key.to_vec().as_slice(), &buffer);

        Ok(())
    }

    pub fn delete_messages(
        &self,
        source_shard: ShardIdent,
        from: &QueueKey,
        to: &QueueKey,
    ) -> Result<()> {
        let start_key = ShardsInternalMessagesKey::new(source_shard, *from);
        let end_key = ShardsInternalMessagesKey::new(source_shard, *to);

        let shards_internal_messages_cf = self.db.shards_internal_messages.cf();

        let mut batch = WriteBatch::default();

        batch.delete_range_cf(
            &shards_internal_messages_cf,
            &start_key.to_vec(),
            &end_key.to_vec(),
        );
        batch.delete_cf(&shards_internal_messages_cf, end_key.to_vec());

        self.db.rocksdb().write(batch)?;

        Ok(())
    }

    pub fn commit(&self, ranges: FastHashMap<ShardIdent, QueueKey>) -> Result<()> {
        let snapshot = self.snapshot();

        let mut batch = WriteBatch::default();

        for range in ranges {
            let from = ShardsInternalMessagesKey {
                shard_ident: range.0,
                internal_message_key: QueueKey::MIN,
            };
            let to = ShardsInternalMessagesKey {
                shard_ident: range.0,
                internal_message_key: range.1,
            };

            let mut readopts = self.db.shards_internal_messages_session.new_read_config();
            readopts.set_snapshot(&snapshot);

            let internal_messages_session_cf = self.db.shards_internal_messages_session.cf();
            let internal_messages_cf = self.db.shards_internal_messages.cf();
            let mut iter = self
                .db
                .rocksdb()
                .raw_iterator_cf_opt(&internal_messages_session_cf, readopts);

            iter.seek(from.to_vec().as_slice());

            while iter.valid() {
                let (mut key, value) = match (iter.key(), iter.value()) {
                    (Some(key), Some(value)) => (key, value),
                    _ => break,
                };

                let current_position = ShardsInternalMessagesKey::deserialize(&mut key);

                if current_position > to || current_position < from {
                    break;
                }

                let dest_workchain = value[0] as i8;
                let dest_prefix = u64::from_be_bytes(value[1..9].try_into().unwrap());
                let cell_bytes = &value[9..];

                batch.delete_cf(&internal_messages_session_cf, current_position.to_vec());
                Self::insert_message(
                    &mut batch,
                    internal_messages_cf,
                    current_position,
                    dest_workchain,
                    dest_prefix,
                    cell_bytes,
                )?;

                iter.next();
            }
        }

        self.db.rocksdb().write(batch)?;

        Ok(())
    }
}
