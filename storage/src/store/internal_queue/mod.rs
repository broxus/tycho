use std::u64;

use anyhow::Result;
use everscale_types::models::{IntAddr, ShardIdent};
use weedb::rocksdb::{ReadOptions, WriteBatch};
use weedb::{BoundedCfHandle, OwnedSnapshot};

use crate::db::*;
use crate::model::{InternalMessageKey, ShardsInternalMessagesKey};
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
        from: InternalMessageKey,
        to: InternalMessageKey,
    ) -> Result<()> {
        let snapshot = self.snapshot();

        let mut readopts = self.db.shards_internal_messages.new_read_config();
        readopts.set_snapshot(&snapshot);

        let start_key = ShardsInternalMessagesKey::new(source_shard, from);
        let end_key = ShardsInternalMessagesKey::new(source_shard, to);

        let shards_internal_messages_cf = self.db.shards_internal_messages.cf();

        let mut iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&shards_internal_messages_cf, readopts);

        iter.seek(&start_key.to_vec());

        let mut batch = WriteBatch::default();

        while iter.valid() {
            let (mut key, _) = match (iter.key(), iter.value()) {
                (Some(key), Some(value)) => (key, value),
                _ => break,
            };

            let current_position = ShardsInternalMessagesKey::deserialize(&mut key);

            if current_position > end_key {
                break;
            }
            batch.delete_cf(&shards_internal_messages_cf, &current_position.to_vec());
            iter.next();
        }

        self.db.rocksdb().write(batch)?;
        let bound = Option::<[u8; 0]>::None;
        self.db
            .rocksdb()
            .compact_range_cf(&self.db.shards_internal_messages.cf(), bound, bound);

        Ok(())
    }

    pub fn commit(
        &self,
        source_shard: ShardIdent,
        from: InternalMessageKey,
        to: InternalMessageKey,
    ) -> Result<()> {
        let snapshot = self.snapshot();
        let from = ShardsInternalMessagesKey {
            shard_ident: source_shard,
            internal_message_key: from,
        };
        let to = ShardsInternalMessagesKey {
            shard_ident: source_shard,
            internal_message_key: to,
        };

        let mut batch = WriteBatch::default();
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

            batch.delete_cf(&internal_messages_session_cf, &current_position.to_vec());
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

        self.db.rocksdb().write(batch)?;

        Ok(())
    }

    pub fn print_column_families_row_count(&self) -> Result<()> {
        let column_families = vec![
            (
                self.db.shards_internal_messages.cf(),
                "shards_internal_messages".to_string(),
            ),
            (
                self.db.shards_internal_messages_session.cf(),
                "shards_internal_messages_session".to_string(),
            ), // Add other column families here as needed
        ];

        for cf in column_families {
            let mut read_opts = ReadOptions::default();
            let snapshot = self.snapshot();
            read_opts.set_snapshot(&snapshot);

            let iter = self.db.rocksdb().raw_iterator_cf_opt(&cf.0, read_opts);
            let mut count: u64 = 0;

            let mut iter = iter;
            iter.seek_to_first();
            while iter.valid() {
                if iter.key().is_some() {
                    count += 1;
                }
                iter.next();
            }

            tracing::info!(target: "local_debug", "CF: {:?}, count: {}", cf.1, count);
        }

        Ok(())
    }
}
