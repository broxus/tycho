use std::fs::File;

use anyhow::Result;
use everscale_types::models::{IntAddr, Message, MsgInfo, OutMsgQueueUpdates, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr, RouterPartitions};
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::{DBRawIterator, WriteBatch};
use weedb::{BoundedCfHandle, ColumnFamily, OwnedRawIterator, OwnedSnapshot, Table};

use crate::db::*;
use crate::model::{QueueRange, ShardsInternalMessagesKey, StatKey, Statistics};
use crate::util::StoredValue;
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

    pub fn begin_transaction(&self) -> InternalQueueTransaction {
        InternalQueueTransaction {
            db: self.db.clone(),
            batch: Default::default(),
            buffer: Vec::new(),
        }
    }

    pub fn make_snapshot(&self) -> InternalQueueSnapshot {
        InternalQueueSnapshot {
            db: self.db.clone(),
            snapshot: self.db.owned_snapshot(),
        }
    }

    pub async fn import_from_file(
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

            let get_partition = |partitions: &RouterPartitions, router_addr: &RouterAddr| {
                for (p, addresses) in partitions {
                    if addresses.contains(router_addr) {
                        return Some(*p);
                    }
                }
                None
            };

            let mapped = MappedFile::from_existing_file(file)?;

            let mut reader = QueueStateReader::begin_from_mapped(mapped.as_slice(), &top_update)?;

            let messages_cf = this.db.shard_internal_messages.cf();
            let stats_cf = this.db.internal_message_stats.cf();

            let mut batch = weedb::rocksdb::WriteBatch::default();

            let mut buffer = Vec::new();
            let mut statistics: FastHashMap<QueuePartitionIdx, FastHashMap<RouterAddr, u64>> =
                FastHashMap::default();
            while let Some(mut part) = reader.read_next_queue_diff()? {
                while let Some(cell) = part.read_next_message()? {
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

                    let queue_diff = part.queue_diff();
                    let partition = get_partition(&queue_diff.router_partitions_dst, &dest_addr)
                        .or_else(|| get_partition(&queue_diff.router_partitions_src, &src_addr))
                        .unwrap_or_default();

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
                    buffer.extend_from_slice(&dest.prefix().to_le_bytes());
                    BocHeader::<ahash::RandomState>::with_root(cell.as_ref()).encode(&mut buffer);
                    batch.put_cf(&messages_cf, key.to_vec(), &buffer);

                    let partition_stats = statistics.entry(partition).or_default();
                    *partition_stats.entry(dest_addr).or_insert(0) += 1;
                }

                let queue_diff = part.queue_diff();
                for (partition, statistics) in statistics.drain() {
                    for (dest, count) in statistics.iter() {
                        let key = StatKey {
                            shard_ident,
                            partition,
                            min_message: queue_diff.min_message,
                            max_message: queue_diff.max_message,
                            dest: *dest,
                        };

                        batch.put_cf(&stats_cf, key.to_vec(), count.to_le_bytes());
                    }
                }
            }

            reader.finish()?;

            this.db.rocksdb().write(batch)?;
            Ok(())
        })
        .await?
    }

    pub fn commit<I: IntoIterator<Item = QueueRange>>(&self, ranges: I) -> Result<()> {
        let snapshot = self.db.rocksdb().snapshot();

        let db = self.db.rocksdb().as_ref();
        let mut batch = WriteBatch::default();

        let mut commit_range = |source_iter: &mut DBRawIterator<'_>,
                                from_key: &[u8],
                                to_key: &[u8],
                                source_cf: &BoundedCfHandle<'_>,
                                target_cf: &BoundedCfHandle<'_>| {
            source_iter.seek(from_key);

            loop {
                let (key, value) = match source_iter.item() {
                    Some(item) => item,
                    None => return source_iter.status(),
                };

                if key > to_key {
                    break;
                }

                batch.delete_cf(source_cf, key);
                batch.put_cf(target_cf, key, value);

                source_iter.next();
            }

            Ok(())
        };

        let messages = &self.db.shard_internal_messages;
        let messages_cf = &messages.cf();

        let uncommited_messages = &self.db.shard_internal_messages_uncommitted;
        let uncommited_messages_cf = &uncommited_messages.cf();

        let mut uncommited_messages_iter = {
            let mut readopts = uncommited_messages.new_read_config();
            readopts.set_snapshot(&snapshot);
            db.raw_iterator_cf_opt(uncommited_messages_cf, readopts)
        };

        let stats = &self.db.internal_message_stats;
        let stats_cf = &stats.cf();

        let uncommited_stats = &self.db.internal_message_stats_uncommitted;
        let uncommited_stats_cf = &uncommited_stats.cf();

        let mut uncommited_stats_iter = {
            let mut readopts = uncommited_stats.new_read_config();
            readopts.set_snapshot(&snapshot);
            db.raw_iterator_cf_opt(uncommited_stats_cf, readopts)
        };

        for range in ranges {
            // Commit messages for one range
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

            commit_range(
                &mut uncommited_messages_iter,
                &from_message_key.to_vec(),
                &to_message_key.to_vec(),
                uncommited_messages_cf,
                messages_cf,
            )?;

            // Commit stats for one range
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

            commit_range(
                &mut uncommited_stats_iter,
                &from_stat_key.to_vec(),
                &to_stat_key.to_vec(),
                uncommited_stats_cf,
                stats_cf,
            )?;
        }

        // Apply batch
        self.db.rocksdb().write(batch).map_err(Into::into)
    }

    pub fn delete<I: IntoIterator<Item = QueueRange>>(&self, ranges: I) -> Result<()> {
        fn delete_range<'a>(
            batch: &mut WriteBatch,
            cf: &'_ BoundedCfHandle<'_>,
            start_key: &'_ [u8],
            end_key: &'_ [u8],
            bump: &'a bumpalo::Bump,
            to_compact: &mut Vec<(&'a [u8], &'a [u8])>,
        ) {
            batch.delete_range_cf(cf, start_key, end_key);
            batch.delete_cf(cf, end_key);
            to_compact.push((
                bump.alloc_slice_copy(start_key),
                bump.alloc_slice_copy(end_key),
            ));
        }

        let mut batch = WriteBatch::default();

        let bump = bumpalo::Bump::new();

        let mut msgs_to_compact = Vec::new();
        let mut stats_to_compact = Vec::new();

        let messages_cf = &self.db.shard_internal_messages.cf();
        let stats_cf = &self.db.internal_message_stats.cf();

        for range in ranges {
            // Delete messages in one range
            let start_msg_key =
                ShardsInternalMessagesKey::new(range.partition, range.shard_ident, range.from);

            let end_msg_key =
                ShardsInternalMessagesKey::new(range.partition, range.shard_ident, range.to);

            delete_range(
                &mut batch,
                messages_cf,
                &start_msg_key.to_vec(),
                &end_msg_key.to_vec(),
                &bump,
                &mut msgs_to_compact,
            );

            // Delete stats in one range
            let start_stat_key = StatKey {
                shard_ident: range.shard_ident,
                partition: range.partition,
                min_message: range.from,
                max_message: QueueKey::MIN,
                dest: RouterAddr::MIN,
            };

            let end_stat_key = StatKey {
                shard_ident: range.shard_ident,
                partition: range.partition,
                min_message: range.to,
                max_message: QueueKey::MAX,
                dest: RouterAddr::MAX,
            };

            delete_range(
                &mut batch,
                stats_cf,
                &start_stat_key.to_vec(),
                &end_stat_key.to_vec(),
                &bump,
                &mut stats_to_compact,
            );
        }

        let db = self.db.rocksdb().as_ref();
        db.write(batch)?;

        for (start_key, end_key) in msgs_to_compact {
            db.compact_range_cf(messages_cf, Some(start_key), Some(end_key));
        }
        for (start_key, end_key) in stats_to_compact {
            db.compact_range_cf(stats_cf, Some(start_key), Some(end_key));
        }

        Ok(())
    }

    pub fn clear_uncommited(&self) -> Result<()> {
        let mut batch = WriteBatch::default();

        let mut clear_table = |cf: &BoundedCfHandle<'_>, from: &[u8], to: &[u8]| {
            batch.delete_range_cf(cf, from, to);
            batch.delete_cf(cf, to);
        };

        let messages_cf = &self.db.shard_internal_messages_uncommitted.cf();
        clear_table(
            messages_cf,
            &[0x00; ShardsInternalMessagesKey::SIZE_HINT],
            &[0xff; ShardsInternalMessagesKey::SIZE_HINT],
        );

        let stats_cf = &self.db.internal_message_stats_uncommitted.cf();
        clear_table(
            stats_cf,
            &[0x00; StatKey::SIZE_HINT],
            &[0xff; StatKey::SIZE_HINT],
        );

        let db = self.db.rocksdb().as_ref();
        db.write(batch)?;

        db.compact_range_cf(messages_cf, None::<[u8; 0]>, None::<[u8; 0]>);
        db.compact_range_cf(stats_cf, None::<[u8; 0]>, None::<[u8; 0]>);
        Ok(())
    }
}

pub struct InternalQueueTransaction {
    db: BaseDb,
    batch: WriteBatch,
    buffer: Vec<u8>,
}

impl InternalQueueTransaction {
    pub fn write(self) -> Result<()> {
        self.db
            .rocksdb()
            .write_opt(self.batch, self.db.shard_internal_messages.write_config())
            .map_err(Into::into)
    }

    pub fn insert_statistics_uncommitted(&mut self, key: &StatKey, count: u64) {
        let cf = self.db.internal_message_stats_uncommitted.cf();
        self.batch.put_cf(&cf, key.to_vec(), count.to_le_bytes());
    }

    pub fn insert_message_uncommitted(
        &mut self,
        key: &ShardsInternalMessagesKey,
        dest: &IntAddr,
        value: &[u8],
    ) {
        let cf = self.db.shard_internal_messages_uncommitted.cf();

        self.buffer.clear();
        self.buffer.reserve(1 + 8 + value.len());

        self.buffer.push(dest.workchain() as i8 as u8);
        self.buffer.extend_from_slice(&dest.prefix().to_le_bytes());
        self.buffer.extend_from_slice(value);

        self.batch.put_cf(&cf, key.to_vec(), self.buffer.as_slice());
    }
}

pub struct InternalQueueSnapshot {
    db: BaseDb,
    snapshot: OwnedSnapshot,
}

impl InternalQueueSnapshot {
    pub fn iter_messages_commited(
        &self,
        from: ShardsInternalMessagesKey,
        to: ShardsInternalMessagesKey,
    ) -> InternalQueueMessagesIter {
        self.iter_messages(&self.db.shard_internal_messages, from, to)
    }

    pub fn iter_messages_uncommited(
        &self,
        from: ShardsInternalMessagesKey,
        to: ShardsInternalMessagesKey,
    ) -> InternalQueueMessagesIter {
        self.iter_messages(&self.db.shard_internal_messages_uncommitted, from, to)
    }

    fn iter_messages<T: ColumnFamily>(
        &self,
        table: &Table<T>,
        from: ShardsInternalMessagesKey,
        to: ShardsInternalMessagesKey,
    ) -> InternalQueueMessagesIter {
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        read_config.set_iterate_lower_bound(from.to_vec().to_vec());
        read_config.set_iterate_upper_bound(to.to_vec().to_vec());

        let db = self.db.rocksdb();
        let iter = db.raw_iterator_cf_opt(&table.cf(), read_config);

        InternalQueueMessagesIter {
            // SAFETY: Iterator was created from the same DB instance.
            inner: unsafe { OwnedRawIterator::new(db.clone(), iter) },
            first: true,
        }
    }

    pub fn collect_committed_stats_in_range(
        &self,
        shard_ident: ShardIdent,
        partition: QueuePartitionIdx,
        from: &QueueKey,
        to: &QueueKey,
        shards: &FastHashSet<ShardIdent>,
        result: &mut Statistics,
    ) -> Result<()> {
        let mut read_config = self.db.internal_message_stats.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let cf = self.db.internal_message_stats.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        Self::collect_dest_counts_in_range(
            &mut iter,
            shard_ident,
            partition,
            *from,
            *to,
            shards,
            result,
        )
    }

    pub fn collect_uncommitted_stats_in_range(
        &self,
        shard_ident: ShardIdent,
        partition: QueuePartitionIdx,
        from: &QueueKey,
        to: &QueueKey,
        shards: &FastHashSet<ShardIdent>,
        result: &mut Statistics,
    ) -> Result<()> {
        let mut read_config = self.db.internal_message_stats_uncommitted.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let cf = self.db.internal_message_stats_uncommitted.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        Self::collect_dest_counts_in_range(
            &mut iter,
            shard_ident,
            partition,
            *from,
            *to,
            shards,
            result,
        )
    }

    fn collect_dest_counts_in_range(
        iter: &mut DBRawIterator<'_>,
        shard_ident: ShardIdent,
        partition: QueuePartitionIdx,
        from: QueueKey,
        to: QueueKey,
        shards: &FastHashSet<ShardIdent>,
        result: &mut Statistics,
    ) -> Result<()> {
        let from_key = StatKey {
            shard_ident,
            partition,
            min_message: from,
            max_message: QueueKey::MIN,
            dest: RouterAddr::MIN,
        };
        iter.seek(from_key.to_vec());

        loop {
            let (key, value) = match iter.item() {
                Some(item) => item,
                None => match iter.status() {
                    Ok(()) => break,
                    Err(e) => return Err(e.into()),
                },
            };

            let current_key = StatKey::from_slice(key);
            if current_key.shard_ident != shard_ident || current_key.partition != partition {
                break;
            }

            if current_key.max_message > to {
                break;
            }

            let count = u64::from_le_bytes(value.try_into().unwrap());
            let entry = result
                .statistics
                .entry(current_key.dest.to_int_addr())
                .or_insert(0);
            *entry += count;

            for shard in shards {
                if shard.contains_address(&current_key.dest.to_int_addr()) {
                    let entry = result.shards_messages_amount.entry(*shard).or_insert(0);
                    *entry += count;
                }
            }

            iter.next();
        }

        Ok(())
    }
}

pub struct InternalQueueMessagesIter {
    #[allow(unused)]
    inner: OwnedRawIterator,
    first: bool,
}

impl InternalQueueMessagesIter {
    pub fn seek(&mut self, key: &ShardsInternalMessagesKey) {
        self.inner.seek(key.to_vec());
        self.first = true;
    }

    pub fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.first = true;
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<InternalQueueMessage<'_>>> {
        if !std::mem::take(&mut self.first) {
            self.inner.next();
        }

        let Some((key, value)) = self.inner.item() else {
            match self.inner.status() {
                Ok(()) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        };

        let key = ShardsInternalMessagesKey::from(key);
        Ok(Some(InternalQueueMessage {
            key,
            workchain: value[0] as i8,
            prefix: u64::from_le_bytes(value[1..9].try_into().unwrap()),
            message_boc: &value[9..],
        }))
    }
}

pub struct InternalQueueMessage<'a> {
    pub key: ShardsInternalMessagesKey,
    pub workchain: i8,
    pub prefix: u64,
    pub message_boc: &'a [u8],
}
