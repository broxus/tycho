use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;

use anyhow::Result;
use everscale_types::models::{IntAddr, Message, MsgInfo, OutMsgQueueUpdates, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartition, RouterAddr, RouterDirection};
use tycho_util::FastHashMap;
use weedb::rocksdb::{DBRawIterator, ReadOptions, WriteBatch, WriteBatchWithTransaction};
use weedb::{BoundedCfHandle, OwnedSnapshot};

use crate::db::*;
use crate::model::{QueueRange, ShardsInternalMessagesKey, StatKey};
use crate::util::{OwnedIterator, StoredValue};
use crate::QueueStateReader;

pub mod model;

#[derive(Clone)]
pub struct InternalQueueStorage {
    db: BaseDb,
}

impl InternalQueueStorage {
    pub fn new(db: BaseDb) -> Self {
        Self { db }
    }

    pub fn insert_statistics_uncommitted(
        &self,
        batch: &mut WriteBatchWithTransaction<false>,
        key: &StatKey,
        count: u64,
    ) -> Result<()> {
        let cf = self.db.internal_messages_statistics_uncommitted.cf();
        self.insert_statistics(batch, &cf, key, count)
    }

    pub fn insert_statistics_committed(
        &self,
        batch: &mut WriteBatchWithTransaction<false>,
        key: &StatKey,
        count: u64,
    ) -> Result<()> {
        let cf = self.db.internal_messages_statistics_committed.cf();
        self.insert_statistics(batch, &cf, key, count)
    }

    pub fn collect_committed_stats_in_range(
        &self,
        snapshot: &OwnedSnapshot,
        shard_ident: ShardIdent,
        partition: QueuePartition,
        from: QueueKey,
        to: QueueKey,
        result: &mut FastHashMap<IntAddr, u64>,
    ) -> Result<()> {
        let mut read_config = self
            .db
            .internal_messages_statistics_committed
            .new_read_config();
        read_config.set_snapshot(snapshot);
        let cf = self.db.internal_messages_statistics_committed.cf();

        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        self.collect_dest_counts_in_range(&mut iter, shard_ident, partition, from, to, result)
    }

    pub fn collect_uncommitted_stats_in_range(
        &self,
        snapshot: &OwnedSnapshot,
        shard_ident: ShardIdent,
        partition: QueuePartition,
        from: QueueKey,
        to: QueueKey,
        result: &mut FastHashMap<IntAddr, u64>,
    ) -> Result<()> {
        let mut read_config = self
            .db
            .internal_messages_statistics_uncommitted
            .new_read_config();
        read_config.set_snapshot(snapshot);
        let cf = self.db.internal_messages_statistics_uncommitted.cf();

        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        self.collect_dest_counts_in_range(&mut iter, shard_ident, partition, from, to, result)
    }

    fn collect_dest_counts_in_range(
        &self,
        iter: &mut DBRawIterator<'_>,
        shard_ident: ShardIdent,
        partition: QueuePartition,
        from: QueueKey,
        to: QueueKey,
        result: &mut FastHashMap<IntAddr, u64>,
    ) -> Result<()> {
        let from_key = StatKey {
            shard_ident,
            partition,
            min_message: from,
            max_message: QueueKey::MIN,
            dest: RouterAddr::MIN,
        };

        let from_key_bytes = {
            let mut buf = Vec::with_capacity(StatKey::SIZE_HINT);
            from_key.serialize(&mut buf);
            buf
        };

        iter.seek(&from_key_bytes);

        while iter.valid() {
            let key_bytes = iter.key();
            let value_bytes = iter.value();

            match (key_bytes, value_bytes) {
                (Some(mut k), Some(v)) => {
                    let current_key = StatKey::deserialize(&mut k);

                    if current_key.shard_ident != shard_ident || current_key.partition != partition
                    {
                        break;
                    }

                    if current_key.max_message > to {
                        break;
                    }

                    let count_bytes = v;

                    let count = u64::from_be_bytes(count_bytes.try_into().unwrap());

                    let entry = result.entry(current_key.dest.to_int_addr()).or_insert(0);
                    *entry += count;
                }
                _ => {
                    break;
                }
            }

            iter.next();
        }

        Ok(())
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

            let get_partition = |router: &BTreeMap<
                RouterDirection,
                BTreeMap<QueuePartition, BTreeSet<RouterAddr>>,
            >,
                                 router_direction: &RouterDirection,
                                 router_addr: &RouterAddr| {
                let mut partition = None;
                if let Some(partitions) = router.get(router_direction) {
                    for (p, addresses) in partitions {
                        if addresses.contains(router_addr) {
                            partition = Some(p);
                            break;
                        }
                    }
                }

                partition.cloned()
            };

            let mapped = MappedFile::from_existing_file(file)?;

            let mut reader = QueueStateReader::begin_from_mapped(mapped.as_slice(), &top_update)?;

            let messages_cf = this.db.shards_internal_messages.cf();
            let mut batch = weedb::rocksdb::WriteBatch::default();

            let mut buffer = Vec::new();
            let mut statistics: FastHashMap<QueuePartition, FastHashMap<RouterAddr, u64>> =
                FastHashMap::default();
            while reader.read_next_diff()?.is_some() {
                let current_diff_index = reader.next_queue_diff_index() - 1;

                while let Some(cell) = reader.read_next_message()? {
                    let msg_hash = cell.repr_hash();
                    let msg = cell.parse::<Message<'_>>()?;
                    let MsgInfo::Int(int_msg_info) = &msg.info else {
                        anyhow::bail!("non-internal message in the queue in msg {msg_hash}");
                    };

                    let IntAddr::Std(dest) = &int_msg_info.dst else {
                        anyhow::bail!("non-std destination address in msg {msg_hash}");
                    };

                    let IntAddr::Std(src) = &int_msg_info.src else {
                        anyhow::bail!("non-std destination address in msg {msg_hash}");
                    };

                    let src_addr = RouterAddr {
                        workchain: src.workchain,
                        account: src.address,
                    };

                    let dest_addr = RouterAddr {
                        workchain: dest.workchain,
                        account: dest.address,
                    };
                    let current_diff = &reader.state().header.queue_diffs[current_diff_index];

                    let mut partition = get_partition(
                        &current_diff.partition_router,
                        &RouterDirection::Dest,
                        &dest_addr,
                    );
                    if partition.is_none() {
                        partition = get_partition(
                            &current_diff.partition_router,
                            &RouterDirection::Src,
                            &src_addr,
                        );
                    }
                    let partition = partition.unwrap_or_default();

                    let key = ShardsInternalMessagesKey {
                        partition,
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
                    batch.put_cf(&messages_cf, key.to_vec(), &buffer);

                    let partition_stats = statistics.entry(partition).or_default();
                    *partition_stats.entry(dest_addr).or_insert(0) += 1;
                }

                let current_diff = &reader.state().header.queue_diffs[current_diff_index];

                for (partition, statistics) in statistics.drain() {
                    for (dest, count) in statistics.iter() {
                        let key = StatKey {
                            shard_ident,
                            partition,
                            min_message: current_diff.min_message,
                            max_message: current_diff.max_message,
                            dest: *dest,
                        };

                        this.insert_statistics_committed(&mut batch, &key, *count)?;
                    }
                }
            }

            reader.finish()?;

            this.db.rocksdb().write(batch)?;
            Ok(())
        })
        .await?
    }

    pub fn commit(&self, ranges: Vec<QueueRange>) -> Result<()> {
        let snapshot = self.snapshot();

        let mut batch = WriteBatch::default();

        for range in &ranges {
            let mut readopts = self
                .db
                .shards_internal_messages_uncommitted
                .new_read_config();
            readopts.set_snapshot(&snapshot);

            let from_message_key = ShardsInternalMessagesKey {
                partition: range.partition,
                shard_ident: range.shard_ident,
                internal_message_key: range.from,
            };
            let to_message_key = ShardsInternalMessagesKey {
                partition: range.partition,
                shard_ident: range.shard_ident,
                internal_message_key: range.to,
            };

            self.commit_range(
                &mut batch,
                readopts,
                &from_message_key.to_vec(),
                &to_message_key.to_vec(),
                &self.db.shards_internal_messages_uncommitted.cf(),
                &self.db.shards_internal_messages.cf(),
            )?;

            let mut readopts = self
                .db
                .internal_messages_statistics_uncommitted
                .new_read_config();
            readopts.set_snapshot(&snapshot);

            let from_stat_key = StatKey {
                shard_ident: range.shard_ident,
                partition: range.partition,
                min_message: range.from,
                max_message: QueueKey::MIN,
                dest: RouterAddr::MIN,
            };
            let to_stat_key = StatKey {
                shard_ident: range.shard_ident,
                partition: range.partition,
                min_message: range.to,
                max_message: QueueKey::MAX,
                dest: RouterAddr::MAX,
            };

            self.commit_range(
                &mut batch,
                readopts,
                &from_stat_key.to_vec(),
                &to_stat_key.to_vec(),
                &self.db.internal_messages_statistics_uncommitted.cf(),
                &self.db.internal_messages_statistics_committed.cf(),
            )?;
        }

        self.db.rocksdb().write(batch)?;

        Ok(())
    }

    fn commit_range(
        &self,
        batch: &mut WriteBatch,
        readopts: ReadOptions,
        from_key: &[u8],
        to_key: &[u8],
        source_cf: &BoundedCfHandle<'_>,
        target_cf: &BoundedCfHandle<'_>,
    ) -> Result<()> {
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(source_cf, readopts);

        iter.seek(from_key);

        while iter.valid() {
            let key = match iter.key() {
                Some(key) => key,
                None => break,
            };

            if key > to_key {
                break;
            }

            let value = match iter.value() {
                Some(value) => value,
                None => break,
            };

            batch.delete_cf(source_cf, key);
            batch.put_cf(target_cf, key, value);

            iter.next();
        }

        Ok(())
    }

    pub fn delete(&self, ranges: Vec<QueueRange>) -> Result<()> {
        let mut batch = WriteBatch::default();

        let mut compact_intervals = Vec::new();

        for range in &ranges {
            let (start_stat_key, end_stat_key, start_msg_key, end_msg_key) =
                Self::build_range_keys(range);

            self.delete_range(
                &mut batch,
                &self.db.internal_messages_statistics_committed.cf(),
                &start_stat_key,
                &end_stat_key,
            );

            self.delete_range(
                &mut batch,
                &self.db.shards_internal_messages.cf(),
                &start_msg_key,
                &end_msg_key,
            );

            compact_intervals.push((
                self.db.internal_messages_statistics_committed.cf(),
                start_stat_key.clone(),
                end_stat_key.clone(),
            ));
            compact_intervals.push((
                self.db.shards_internal_messages.cf(),
                start_msg_key.clone(),
                end_msg_key.clone(),
            ));
        }

        self.db.rocksdb().write(batch)?;

        for (cf, start_key, end_key) in compact_intervals {
            self.db
                .rocksdb()
                .compact_range_cf(&cf, Some(&start_key), Some(&end_key));
        }

        Ok(())
    }

    fn build_range_keys(range: &QueueRange) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
        let start_stat_key = StatKey {
            shard_ident: range.shard_ident,
            partition: range.partition,
            min_message: range.from,
            max_message: QueueKey::MIN,
            dest: RouterAddr::MIN,
        }
        .to_vec()
        .to_vec();

        let end_stat_key = StatKey {
            shard_ident: range.shard_ident,
            partition: range.partition,
            min_message: range.to,
            max_message: QueueKey::MAX,
            dest: RouterAddr::MAX,
        }
        .to_vec()
        .to_vec();

        let start_msg_key =
            ShardsInternalMessagesKey::new(range.partition, range.shard_ident, range.from)
                .to_vec()
                .to_vec();

        let end_msg_key =
            ShardsInternalMessagesKey::new(range.partition, range.shard_ident, range.to)
                .to_vec()
                .to_vec();

        (start_stat_key, end_stat_key, start_msg_key, end_msg_key)
    }

    fn delete_range(
        &self,
        batch: &mut WriteBatch,
        cf: &BoundedCfHandle<'_>,
        start_key: &[u8],
        end_key: &[u8],
    ) {
        batch.delete_range_cf(cf, start_key, end_key);
        batch.delete_cf(cf, end_key);
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
    pub fn clear_uncommitted_state(&self) -> Result<()> {
        let cf = self.db.shards_internal_messages_uncommitted.cf();
        self.clear(&cf)?;

        let cf = self.db.internal_messages_statistics_uncommitted.cf();
        let start_key = [0x00; StatKey::SIZE_HINT];
        let end_key = [0xFF; StatKey::SIZE_HINT];
        self.db
            .rocksdb()
            .delete_range_cf(&cf, &start_key, &end_key)?;
        self.db
            .rocksdb()
            .compact_range_cf(&cf, Some(start_key), Some(end_key));

        Ok(())
    }

    pub fn clear_committed_queue(&self) -> Result<()> {
        let cf = self.db.shards_internal_messages.cf();
        self.clear(&cf)
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

    fn clear(&self, cf: &BoundedCfHandle<'_>) -> Result<()> {
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
        buffer.extend_from_slice(&dest_workchain.to_be_bytes());
        buffer.extend_from_slice(&dest_prefix.to_be_bytes());
        buffer.extend_from_slice(cell);

        batch.put_cf(&cf, key.to_vec().as_slice(), &buffer);

        Ok(())
    }

    fn insert_statistics(
        &self,
        batch: &mut WriteBatchWithTransaction<false>,
        cf: &BoundedCfHandle<'_>,
        key: &StatKey,
        count: u64,
    ) -> Result<()> {
        let mut key_buffer = Vec::with_capacity(StatKey::SIZE_HINT);
        key.serialize(&mut key_buffer);

        let mut value_buffer = Vec::with_capacity(std::mem::size_of::<u64>());
        value_buffer.extend_from_slice(&count.to_be_bytes());

        batch.put_cf(cf, &key_buffer, &value_buffer);

        Ok(())
    }
}
