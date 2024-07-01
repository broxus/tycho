use std::collections::BTreeMap;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::ShardIdent;
pub use model::ShardsInternalMessagesKey;
use weedb::rocksdb::{IteratorMode, ReadOptions, WriteBatch};
use weedb::{BoundedCfHandle, OwnedSnapshot};

use crate::db::*;
use crate::util::{OwnedIterator, StoredValue, StoredValueBuffer};

pub(crate) mod model;

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

    pub fn build_iterator(
        &self,
        snapshot: &OwnedSnapshot,
        shards: Vec<ShardIdent>,
    ) -> BTreeMap<ShardIdent, OwnedIterator> {
        let mut iterators = BTreeMap::new();
        for source_shard in shards {
            let mut readopts = self.db.shards_internal_messages.new_read_config();
            readopts.set_snapshot(snapshot);
            let shards_internal_messages_cf = self.db.shards_internal_messages.cf();
            let iter = self
                .db
                .rocksdb()
                .raw_iterator_cf_opt(&shards_internal_messages_cf, readopts);

            let owned_iterator = OwnedIterator::new(iter, self.db.rocksdb().clone());

            iterators.insert(source_shard, owned_iterator);
        }

        iterators
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_messages_session_batch(
        &self,
        batch: &mut WriteBatch,
        shard_ident: ShardIdent,
        lt: u64,
        hash: HashBytes,
        workchain: i8,
        dest_address: HashBytes,
        cell: Vec<u8>,
    ) {
        let key = ShardsInternalMessagesKey {
            shard_ident,
            lt,
            hash,
        };

        let mut buffer = Vec::with_capacity(1 + 32 + cell.len());
        buffer.write_raw_slice(&workchain.to_be_bytes());
        buffer.write_raw_slice(dest_address.as_slice());
        buffer.write_raw_slice(&cell);

        batch.put_cf(
            &self.db.shards_internal_messages_session.cf(),
            key.to_vec().as_slice(),
            &buffer,
        );
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db.rocksdb().write(batch)?;
        Ok(())
    }

    pub fn build_iterator_session(
        &self,
        snapshot: &OwnedSnapshot,
        shards: Vec<ShardIdent>,
    ) -> BTreeMap<ShardIdent, OwnedIterator> {
        let mut iterators = BTreeMap::new();
        for source_shard in shards {
            let mut readopts = self.db.shards_internal_messages_session.new_read_config();
            readopts.set_snapshot(snapshot);
            let shards_internal_messages_cf = self.db.shards_internal_messages_session.cf();
            let iter = self
                .db
                .rocksdb()
                .raw_iterator_cf_opt(&shards_internal_messages_cf, readopts);

            let owned_iterator = OwnedIterator::new(iter, self.db.rocksdb().clone());

            iterators.insert(source_shard, owned_iterator);
        }

        iterators
    }

    #[allow(clippy::type_complexity)]
    pub fn retrieve_and_delete_messages(
        &self,
        snapshot: &OwnedSnapshot,
        shard_ident: ShardIdent,
        range: ((u64, HashBytes), (u64, HashBytes)),
    ) -> Result<Vec<(u64, HashBytes, i8, HashBytes, Vec<u8>)>> {
        let from = ShardsInternalMessagesKey {
            shard_ident,
            lt: range.0 .0,
            hash: range.0 .1,
        };
        let to = ShardsInternalMessagesKey {
            shard_ident,
            lt: range.1 .0,
            hash: range.1 .1,
        };

        let mut messages = Vec::new();
        let mut batch = WriteBatch::default();
        let mut readopts = self.db.shards_internal_messages_session.new_read_config();
        readopts.set_snapshot(snapshot);

        let cf = self.db.shards_internal_messages_session.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, readopts);

        iter.seek(from.to_vec().as_slice());

        while iter.valid() {
            let (mut key, value) = match (iter.key(), iter.value()) {
                (Some(key), Some(value)) => (key, value),
                _ => break,
            };

            let current_position = ShardsInternalMessagesKey::deserialize(&mut key);
            if current_position > to {
                break;
            }

            if current_position < from {
                break;
            }

            let workchain = value[0] as i8;
            let address = HashBytes::from_slice(&value[1..33]);
            let value = value[33..].to_vec();
            messages.push((
                current_position.lt,
                current_position.hash,
                workchain,
                address,
                value.clone(),
            ));

            batch.delete_cf(&cf, &current_position.to_vec());

            iter.next();
        }

        self.db.rocksdb().write(batch)?;

        Ok(messages)
    }
    pub fn delete_messages_session(
        &self,
        snapshot: &OwnedSnapshot,
        shard_ident: ShardIdent,
        range: ((u64, HashBytes), (u64, HashBytes)),
    ) -> Result<i32> {
        let mut total_deleted = 0;
        let from = ShardsInternalMessagesKey {
            shard_ident,
            lt: range.0 .0,
            hash: range.0 .1,
        };
        let to = ShardsInternalMessagesKey {
            shard_ident,
            lt: range.1 .0,
            hash: range.1 .1,
        };

        let mut batch = WriteBatch::default();
        let mut readopts = self.db.shards_internal_messages_session.new_read_config();
        readopts.set_snapshot(snapshot);

        let cf = self.db.shards_internal_messages_session.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, readopts);

        iter.seek(from.to_vec().as_slice());

        while iter.valid() {
            let (mut key, _) = match (iter.key(), iter.value()) {
                (Some(key), Some(value)) => (key, value),
                _ => break,
            };

            let current_position = ShardsInternalMessagesKey::deserialize(&mut key);
            if current_position > to {
                break;
            }

            total_deleted += 1;
            batch.delete_cf(&cf, &current_position.to_vec());
            iter.next();
        }

        self.db.rocksdb().write(batch)?;
        Ok(total_deleted)
    }

    pub fn insert_messages_session(
        &self,
        shard_ident: ShardIdent,
        messages: Vec<(u64, HashBytes, i8, HashBytes, Vec<u8>)>,
    ) -> Result<i32> {
        let cf = self.db.shards_internal_messages_session.cf();
        self.insert_messages(cf, shard_ident, messages)
    }

    pub fn insert_messages_persistent(
        &self,
        shard_ident: ShardIdent,
        messages: Vec<(u64, HashBytes, i8, HashBytes, Vec<u8>)>,
    ) -> Result<i32> {
        let cf = self.db.shards_internal_messages.cf();
        self.insert_messages(cf, shard_ident, messages)
    }

    pub fn insert_messages(
        &self,
        cf: BoundedCfHandle<'_>,
        shard_ident: ShardIdent,
        messages: Vec<(u64, HashBytes, i8, HashBytes, Vec<u8>)>,
    ) -> Result<i32> {
        let mut batch_shards_internal_messages = WriteBatch::default();
        let mut count = 0;

        for (lt, hash, workchain, dest_address, cell) in messages.iter() {
            let shard_internal_message_key = ShardsInternalMessagesKey {
                shard_ident,
                lt: *lt,
                hash: *hash,
            };

            let buffer = &mut Vec::with_capacity(1 + 32 + cell.len());

            buffer.write_raw_slice(&workchain.to_be_bytes());
            buffer.write_raw_slice(dest_address.as_slice());
            buffer.write_raw_slice(cell);

            batch_shards_internal_messages.put_cf(
                &cf,
                shard_internal_message_key.to_vec().as_slice(),
                buffer,
            );

            count += 1;
        }

        if count > 0 {
            self.db.rocksdb().write(batch_shards_internal_messages)?;
        }
        Ok(count)
    }

    pub fn insert_message_session(
        &self,
        shard_ident: ShardIdent,
        lt: u64,
        hash: HashBytes,
        workchain: i8,
        dest_address: HashBytes,
        cell: Vec<u8>,
    ) -> Result<()> {
        let key = ShardsInternalMessagesKey {
            shard_ident,
            lt,
            hash,
        };

        let mut buffer = Vec::with_capacity(1 + 32 + cell.len());
        buffer.write_raw_slice(&workchain.to_be_bytes()); // Use big-endian for proper ordering, cast to u8
        buffer.write_raw_slice(dest_address.as_slice()); // Directly write the byte array
        buffer.write_raw_slice(&cell);

        self.db
            .shards_internal_messages_session
            .insert(key.to_vec().as_slice(), &buffer)?;

        Ok(())
    }
    pub fn delete_messages(
        &self,
        shard: ShardIdent,
        delete_until: (u64, HashBytes),
        receiver: ShardIdent,
    ) -> Result<()> {
        let snapshot = self.snapshot();

        let mut readopts = self.db.shards_internal_messages.new_read_config();
        readopts.set_snapshot(&snapshot);

        let start_key = ShardsInternalMessagesKey {
            shard_ident: shard,
            lt: 0,
            hash: HashBytes::ZERO,
        };

        let end_key = ShardsInternalMessagesKey {
            shard_ident: shard,
            lt: delete_until.0,
            hash: delete_until.1,
        };

        let shards_internal_messages_cf = self.db.shards_internal_messages.cf();

        let mut iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&shards_internal_messages_cf, readopts);

        iter.seek(&start_key.to_vec());

        let mut batch = WriteBatch::default();

        while iter.valid() {
            let (mut key, value) = match (iter.key(), iter.value()) {
                (Some(key), Some(value)) => (key, value),
                _ => break,
            };

            let current_position = ShardsInternalMessagesKey::deserialize(&mut key);

            if current_position >= end_key {
                break;
            }

            let workchain = value[0] as i8;
            let address = HashBytes::from_slice(&value[1..33]);

            if !(receiver.workchain() == workchain as i32 && receiver.contains_account(&address)) {
                iter.next();
                continue;
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

    pub fn count_rows_iteratively(
        &self,
        snapshot: &OwnedSnapshot,
        cf_name: &str,
    ) -> Result<u64, String> {
        let cf_handle = self
            .db
            .rocksdb()
            .cf_handle(cf_name)
            .ok_or("Column family not found")?;
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(snapshot);

        let iter = self
            .db
            .rocksdb()
            .iterator_cf_opt(&cf_handle, readopts, IteratorMode::Start);
        let count = iter.count() as u64;
        Ok(count)
    }

    pub fn print_cf_sizes(&self) -> Result<()> {
        // let _snapshot = self.snapshot();
        // let cfs = ["shards_internal_messages", "shards_internal_messages_session"];
        // for cf in cfs {
        //     match self.count_rows_iteratively(&_snapshot, cf) {
        //         Ok(size) => {
        //             tracing::error!(target: "local_debug", "Column family '{}' size: {} rows", cf, size)
        //         }
        //         Err(err) => println!("Failed to get size for column family '{}': {:?}", cf, err),
        //     }
        // }
        Ok(())
    }
}
