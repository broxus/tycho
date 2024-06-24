pub(crate) mod model;

use anyhow::Result;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::ShardIdent;
pub use model::StorageInternalMessageKey;
use tycho_util::metrics::HistogramGuard;
use weedb::rocksdb::{IteratorMode, ReadOptions, WriteBatch};
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

        let iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&internal_messages_cf, readopts);

        let iterator = OwnedIterator::new(iter, self.db.rocksdb().clone());

        iterator
    }

    pub fn insert_messages(
        &self,
        shard_ident: ShardIdent,
        messages: &[(u64, HashBytes, HashBytes, Cell)],
    ) -> Result<()> {
        let mut batch_internal_messages = WriteBatch::default();
        let mut batch_shards_internal_messages = WriteBatch::default();

        for (lt, hash, dest, cell) in messages {
            let internal_message_key = StorageInternalMessageKey {
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
                hash: *hash,
            };

            batch_shards_internal_messages.put_cf(
                &self.db.shards_internal_messages.cf(),
                shard_internal_message_key.to_vec().as_slice(),
                dest.as_slice(),
            );
        }

        self.db.rocksdb().write(batch_internal_messages)?;
        self.db.rocksdb().write(batch_shards_internal_messages)?;

        let bound = Option::<[u8; 0]>::None;
        self.db
            .rocksdb()
            .compact_range_cf(&self.db.internal_messages.cf(), bound, bound);
        self.db
            .rocksdb()
            .compact_range_cf(&self.db.shards_internal_messages.cf(), bound, bound);

        Ok(())
    }

    pub fn delete_messages(
        &self,
        shard: ShardIdent,
        delete_until: (u64, HashBytes),
        receiver: ShardIdent,
    ) -> Result<()> {
        self.print_cf_sizes(&self.snapshot()).unwrap();

        // let mut total_deleted = 0;
        let mut readopts = self.db.shards_internal_messages.new_read_config();
        let snapshot = self.snapshot();
        readopts.set_snapshot(&snapshot);
        // readopts.fill_cache(false); // Optimize for deletion scan

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

        let histogram = HistogramGuard::begin("tycho_do_collate_init_iterator_time");

        let mut iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&shards_internal_messages_cf, readopts);

        histogram.finish();

        iter.seek(&start_key.to_vec());

        let mut batch = WriteBatch::default();
        let internal_messages_cf = self.db.internal_messages.cf();

        while iter.valid() {
            let (mut key, value) = match (iter.key(), iter.value()) {
                (Some(key), Some(value)) => (key, value),
                _ => break,
            };

            let current_position = ShardsInternalMessagesKey::deserialize(&mut key);

            if current_position > end_key {
                break;
            }

            let dest = HashBytes::from_slice(value);

            if !(receiver.contains_account(&dest)
                && receiver.workchain() == current_position.shard_ident.workchain())
            {
                iter.next();
                continue;
            }

            let internal_message_key_to_remove = StorageInternalMessageKey {
                lt: current_position.lt,
                hash: current_position.hash,
                shard_ident: shard,
            };

            // total_deleted += 1;

            batch.delete_cf(
                &internal_messages_cf,
                &internal_message_key_to_remove.to_vec(),
            );
            batch.delete_cf(&shards_internal_messages_cf, &current_position.to_vec());
            iter.next();
        }

        self.db.rocksdb().write(batch)?;

        let bound = Option::<[u8; 0]>::None;
        self.db
            .rocksdb()
            .compact_range_cf(&self.db.internal_messages.cf(), bound, bound);
        self.db
            .rocksdb()
            .compact_range_cf(&self.db.shards_internal_messages.cf(), bound, bound);

        // self.print_cf_sizes(&self.snapshot()).unwrap();

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
        // readopts.fill_cache(false); // Optimize for row count

        let iter = self
            .db
            .rocksdb()
            .iterator_cf_opt(&cf_handle, readopts, IteratorMode::Start);
        let count = iter.count() as u64;
        Ok(count)
    }

    pub fn print_cf_sizes(&self, _snapshot: &OwnedSnapshot) -> Result<()> {
        // let cfs = ["internal_messages", "shards_internal_messages"];
        // for cf in cfs {
        //     match self.count_rows_iteratively(&snapshot, cf) {
        //         Ok(size) => tracing::error!(target: "debug444", "Column family '{}' size: {} rows", cf, size),
        //         Err(err) => println!("Failed to get size for column family '{}': {:?}", cf, err),
        //     }
        // }
        Ok(())
    }
}
