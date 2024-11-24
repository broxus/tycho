use std::fs::File;

use anyhow::Result;
use everscale_types::models::{IntAddr, Message, MsgInfo, OutMsgQueueUpdates, ShardIdent};
use tycho_block_util::queue::QueueKey;
use tycho_util::FastHashMap;
use weedb::rocksdb::{ReadOptions, WriteBatch};
use weedb::{BoundedCfHandle, OwnedSnapshot};

use crate::db::*;
use crate::model::ShardsInternalMessagesKey;
use crate::store::QueueStateReader;
use crate::util::{OwnedIterator, StoredValue};

pub mod model;

#[derive(Clone)]
pub struct InternalQueueStorage {
    db: BaseDb,
}

impl InternalQueueStorage {
    pub fn new(db: BaseDb) -> Self {
        Self { db }
    }

    pub async fn insert_from_file(
        &self,
        shard_ident: ShardIdent,
        top_update: &OutMsgQueueUpdates,
        file: File,
    ) -> Result<()> {
        use everscale_types::boc::ser::BocHeader;

        let top_update = top_update.clone();
        let this = self.clone();

        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            let mapped = MappedFile::from_existing_file(file)?;

            let mut reader = QueueStateReader::begin_from_mapped(mapped.as_slice(), &top_update)?;

            let cf = this.db.shards_internal_messages.cf();
            let mut batch = weedb::rocksdb::WriteBatch::default();

            let mut buffer = Vec::new();
            while let Some(cell) = reader.read_next_message()? {
                let msg_hash = cell.repr_hash();
                let msg = cell.parse::<Message<'_>>()?;
                let MsgInfo::Int(int_msg_info) = &msg.info else {
                    anyhow::bail!("non-internal message in the queue in msg {msg_hash}");
                };

                let IntAddr::Std(dest) = &int_msg_info.dst else {
                    anyhow::bail!("non-std destination address in msg {msg_hash}");
                };

                let key = ShardsInternalMessagesKey {
                    shard_ident,
                    internal_message_key: QueueKey {
                        lt: int_msg_info.created_lt,
                        hash: *msg_hash,
                    },
                };

                buffer.clear();
                buffer.push(dest.workchain as u8);
                buffer.extend_from_slice(&dest.prefix().to_be_bytes());
                BocHeader::<ahash::RandomState>::with_root(cell.as_ref()).encode(&mut buffer);
                batch.put_cf(&cf, key.to_vec(), &buffer);
            }

            reader.finish()?;

            this.db.rocksdb().write(batch)?;
            Ok(())
        })
        .await?
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

        let start_key = start_key.to_vec();
        let end_key = end_key.to_vec();

        batch.delete_range_cf(&shards_internal_messages_cf, &start_key, &end_key);
        batch.delete_cf(&shards_internal_messages_cf, &end_key);

        self.db.rocksdb().write(batch)?;
        self.db.rocksdb().compact_range_cf(
            &shards_internal_messages_cf,
            Some(&start_key),
            Some(&end_key),
        );

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

            let mut readopts = self
                .db
                .shards_internal_messages_uncommitted
                .new_read_config();
            readopts.set_snapshot(&snapshot);

            let internal_messages_uncommitted_cf =
                self.db.shards_internal_messages_uncommitted.cf();
            let internal_messages_cf = self.db.shards_internal_messages.cf();
            let mut iter = self
                .db
                .rocksdb()
                .raw_iterator_cf_opt(&internal_messages_uncommitted_cf, readopts);

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
                let current_position_vec = current_position.to_vec();
                batch.delete_cf(&internal_messages_uncommitted_cf, &current_position_vec);
                batch.put_cf(&internal_messages_cf, &current_position_vec, value);

                iter.next();
            }
        }

        self.db.rocksdb().write(batch)?;

        Ok(())
    }

    pub fn snapshot(&self) -> OwnedSnapshot {
        self.db.owned_snapshot()
    }

    pub fn build_iterator_committed(&self, snapshot: &OwnedSnapshot) -> OwnedIterator {
        self.build_iterator(
            self.db.shards_internal_messages.cf(),
            self.db.shards_internal_messages.new_read_config(),
            snapshot,
        )
    }

    pub fn build_iterator_uncommitted(&self, snapshot: &OwnedSnapshot) -> OwnedIterator {
        self.build_iterator(
            self.db.shards_internal_messages_uncommitted.cf(),
            self.db
                .shards_internal_messages_uncommitted
                .new_read_config(),
            snapshot,
        )
    }
    pub fn clear_uncommitted_queue(&self) -> Result<()> {
        let cf = self.db.shards_internal_messages_uncommitted.cf();
        self.clear_queue(&cf)
    }

    pub fn clear_committed_queue(&self) -> Result<()> {
        let cf = self.db.shards_internal_messages.cf();
        self.clear_queue(&cf)
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db.rocksdb().write(batch)?;
        Ok(())
    }

    pub fn create_batch(&self) -> WriteBatch {
        WriteBatch::default()
    }

    pub fn insert_message_uncommitted(
        &self,
        batch: &mut WriteBatch,
        key: ShardsInternalMessagesKey,
        dest: &IntAddr,
        value: &[u8],
    ) -> Result<()> {
        let cf = self.db.shards_internal_messages_uncommitted.cf();
        Self::insert_message(batch, cf, key, dest.workchain() as i8, dest.prefix(), value)
    }

    fn clear_queue(&self, cf: &BoundedCfHandle<'_>) -> Result<()> {
        let start_key = [0x00; ShardsInternalMessagesKey::SIZE_HINT];
        let end_key = [0xFF; ShardsInternalMessagesKey::SIZE_HINT];
        self.db
            .rocksdb()
            .delete_range_cf(cf, &start_key, &end_key)?;
        self.db
            .rocksdb()
            .compact_range_cf(cf, Some(start_key), Some(end_key));
        Ok(())
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
}
