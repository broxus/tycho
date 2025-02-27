use std::fs::File;

use anyhow::Result;
use everscale_types::models::{BlockId, IntAddr, Message, MsgInfo, OutMsgQueueUpdates, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr, RouterPartitions};
use tycho_util::FastHashMap;
use weedb::rocksdb::{DBRawIterator, WriteBatch};
use weedb::{rocksdb, BoundedCfHandle, ColumnFamily, OwnedRawIterator, OwnedSnapshot, Table};

use crate::db::*;
use crate::model::{
    DiffInfo, DiffInfoKey, DiffTailKey, QueueRange, ShardsInternalMessagesKey, StatKey,
};
use crate::util::StoredValue;
use crate::QueueStateReader;

pub mod model;

#[derive(Clone)]
pub struct InternalQueueStorage {
    db: BaseDb,
}
// Constant for the last applied mc block id key
const INT_QUEUE_LAST_APPLIED_MC_BLOCK_ID_KEY: &[u8] = b"last_applied_mc_block_id";

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
        top_update: &OutMsgQueueUpdates,
        file: File,
        block_id: BlockId,
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
            let var_cf = this.db.internal_message_var.cf();
            let diffs_tail_cf = this.db.internal_message_diffs_tail.cf();
            let diff_infos_cf = this.db.internal_message_diff_info.cf();

            let mut batch = weedb::rocksdb::WriteBatch::default();

            let mut buffer = Vec::new();
            let mut statistics: FastHashMap<QueuePartitionIdx, FastHashMap<RouterAddr, u64>> =
                FastHashMap::default();
            while let Some(mut part) = reader.read_next_queue_diff()? {
                let mut shards_messages_count = FastHashMap::default();

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

                    // TODO after split/merge implementation we should use detailed counter for 256 shards
                    let dest_shard = ShardIdent::new_full(dest_addr.workchain as i32);

                    shards_messages_count
                        .entry(dest_shard)
                        .and_modify(|count| *count += 1)
                        .or_insert(1);

                    let queue_diff = part.queue_diff();
                    let partition = get_partition(&queue_diff.router_partitions_dst, &dest_addr)
                        .or_else(|| get_partition(&queue_diff.router_partitions_src, &src_addr))
                        .unwrap_or_default();

                    let key = ShardsInternalMessagesKey {
                        partition,
                        shard_ident: block_id.shard,
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

                // insert diff tail
                let diff_tail_key = DiffTailKey {
                    shard_ident: queue_diff.shard_ident,
                    max_message: queue_diff.max_message,
                };

                batch.put_cf(
                    &diffs_tail_cf,
                    diff_tail_key.to_vec(),
                    queue_diff.seqno.to_le_bytes(),
                );

                // insert diff info
                let diff_info_key = DiffInfoKey {
                    shard_ident: queue_diff.shard_ident,
                    seqno: queue_diff.seqno,
                };

                let diff_info = DiffInfo {
                    min_message: queue_diff.min_message,
                    max_message: queue_diff.max_message,
                    shards_messages_count,
                    hash: queue_diff.hash,
                    processed_to: queue_diff.processed_to.clone(),
                    router_partitions_src: queue_diff.router_partitions_src.clone(),
                    router_partitions_dst: queue_diff.router_partitions_dst.clone(),
                };

                batch.put_cf(
                    &diff_infos_cf,
                    diff_info_key.to_vec(),
                    tl_proto::serialize(diff_info),
                );

                for (partition, statistics) in statistics.drain() {
                    for (dest, count) in statistics.iter() {
                        let key = StatKey {
                            shard_ident: queue_diff.shard_ident,
                            partition,
                            min_message: queue_diff.min_message,
                            max_message: queue_diff.max_message,
                            dest: *dest,
                        };

                        batch.put_cf(&stats_cf, key.to_vec(), count.to_le_bytes());
                    }
                }
            }

            // insert last applied diff
            batch.put_cf(
                &var_cf,
                INT_QUEUE_LAST_APPLIED_MC_BLOCK_ID_KEY,
                block_id.to_vec(),
            );

            reader.finish()?;

            this.db.rocksdb().write(batch)?;
            Ok(())
        })
        .await?
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
        let mut diffs_tail_to_compact = Vec::new();
        let mut diff_info_to_compact = Vec::new();

        let messages_cf = &self.db.shard_internal_messages.cf();
        let stats_cf = &self.db.internal_message_stats.cf();
        let diffs_tail_cf = &self.db.internal_message_diffs_tail.cf();
        let diff_info_cf = &self.db.internal_message_diff_info.cf();

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

            // delete tail and info
            let start_diff_tail_key = DiffTailKey {
                shard_ident: range.shard_ident,
                max_message: range.from,
            };

            let end_diff_tail_key = DiffTailKey {
                shard_ident: range.shard_ident,
                max_message: range.to,
            };

            let from_diff_tail_bytes = start_diff_tail_key.to_vec();
            let to_diff_tail_bytes = end_diff_tail_key.to_vec();

            let (min_seqno, max_seqno) = delete_diff_tails_and_collect_seqno(
                &mut batch,
                self.db.rocksdb().as_ref(),
                &self.db.internal_message_diffs_tail,
                range.shard_ident,
                &from_diff_tail_bytes,
                &to_diff_tail_bytes,
            )?;

            diffs_tail_to_compact.push((
                bump.alloc_slice_copy(&from_diff_tail_bytes),
                bump.alloc_slice_copy(&to_diff_tail_bytes),
            ));

            // if we found some valid seqnos, also delete the [min_seqno .. max_seqno] from diff_info
            if min_seqno != u32::MAX && max_seqno != 0 {
                let from_diff_info = DiffInfoKey {
                    shard_ident: range.shard_ident,
                    seqno: min_seqno,
                }
                .to_vec();
                let to_diff_info = DiffInfoKey {
                    shard_ident: range.shard_ident,
                    seqno: max_seqno,
                }
                .to_vec();

                // Range-delete for diff_info
                batch.delete_range_cf(diff_info_cf, &from_diff_info, &to_diff_info);
                batch.delete_cf(diff_info_cf, &to_diff_info);

                diff_info_to_compact.push((
                    bump.alloc_slice_copy(&from_diff_info),
                    bump.alloc_slice_copy(&to_diff_info),
                ));
            }
        }

        let db = self.db.rocksdb().as_ref();
        db.write(batch)?;

        for (start_key, end_key) in msgs_to_compact {
            db.compact_range_cf(messages_cf, Some(start_key), Some(end_key));
        }
        for (start_key, end_key) in stats_to_compact {
            db.compact_range_cf(stats_cf, Some(start_key), Some(end_key));
        }
        for (start_key, end_key) in diffs_tail_to_compact {
            db.compact_range_cf(diffs_tail_cf, Some(start_key), Some(end_key));
        }
        for (start_key, end_key) in diff_info_to_compact {
            db.compact_range_cf(diff_info_cf, Some(start_key), Some(end_key));
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

        let diffs_tail_cf = &self.db.internal_message_diffs_tail_uncommitted.cf();
        clear_table(
            diffs_tail_cf,
            &[0x00; StatKey::SIZE_HINT],
            &[0xff; StatKey::SIZE_HINT],
        );

        let diffs_info_cf = &self.db.internal_message_diff_info_uncommitted.cf();
        clear_table(
            diffs_info_cf,
            &[0x00; StatKey::SIZE_HINT],
            &[0xff; StatKey::SIZE_HINT],
        );

        let db = self.db.rocksdb().as_ref();
        db.write(batch)?;

        db.compact_range_cf(messages_cf, None::<[u8; 0]>, None::<[u8; 0]>);
        db.compact_range_cf(stats_cf, None::<[u8; 0]>, None::<[u8; 0]>);
        db.compact_range_cf(diffs_tail_cf, None::<[u8; 0]>, None::<[u8; 0]>);
        db.compact_range_cf(diffs_info_cf, None::<[u8; 0]>, None::<[u8; 0]>);
        Ok(())
    }

    /// Retrieves the queue version from the `internal_message_version` column family under the key `mc_version`
    pub fn get_last_applied_mc_block_id(&self) -> Result<Option<BlockId>> {
        let cf = self.db.internal_message_var.cf();
        let data = self
            .db
            .rocksdb()
            .get_cf(&cf, INT_QUEUE_LAST_APPLIED_MC_BLOCK_ID_KEY)?;
        if let Some(bytes) = data {
            return Ok(Some(BlockId::from_slice(&bytes)));
        }

        Ok(None)
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

    pub fn insert_diff_tail_uncommitted(&mut self, key: &DiffTailKey, value: &[u8]) {
        let cf = self.db.internal_message_diffs_tail_uncommitted.cf();
        self.batch.put_cf(&cf, key.to_vec(), value);
    }

    pub fn insert_diff_info_uncommitted(&mut self, key: &DiffInfoKey, value: &[u8]) {
        let cf = self.db.internal_message_diff_info_uncommitted.cf();
        self.batch.put_cf(&cf, key.to_vec(), value);
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

    fn commit_range(
        batch: &mut WriteBatch,
        source_iter: &mut DBRawIterator<'_>,
        from_key: &[u8],
        to_key: &[u8],
        source_cf: &BoundedCfHandle<'_>,
        target_cf: &BoundedCfHandle<'_>,
    ) -> Result<()> {
        source_iter.seek(from_key);

        loop {
            let (key, value) = match source_iter.item() {
                Some(item) => item,
                None => return source_iter.status().map_err(Into::into),
            };

            if key > to_key {
                break;
            }

            // Move from uncommitted => committed
            batch.delete_cf(source_cf, key);
            batch.put_cf(target_cf, key, value);

            source_iter.next();
        }

        Ok(())
    }

    pub fn commit_messages<I: IntoIterator<Item = QueueRange>>(
        &mut self,
        snapshot: &InternalQueueSnapshot,
        ranges: I,
    ) -> Result<()> {
        let db = self.db.rocksdb().as_ref();

        // -- Prepare CF handles --
        let messages_cf = self.db.shard_internal_messages.cf();
        let uncommited_messages_cf = self.db.shard_internal_messages_uncommitted.cf();

        let stats_cf = self.db.internal_message_stats.cf();
        let uncommited_stats_cf = self.db.internal_message_stats_uncommitted.cf();

        let diff_tail_committed_cf = self.db.internal_message_diffs_tail.cf();
        let diff_tail_uncommitted_cf = self.db.internal_message_diffs_tail_uncommitted.cf();

        let diff_info_committed_cf = self.db.internal_message_diff_info.cf();
        let diff_info_uncommitted_cf = self.db.internal_message_diff_info_uncommitted.cf();

        // -- Prepare iterators --
        let mut uncommited_messages_iter = {
            let mut readopts = self
                .db
                .shard_internal_messages_uncommitted
                .new_read_config();
            readopts.set_snapshot(&snapshot.snapshot);
            db.raw_iterator_cf_opt(&uncommited_messages_cf, readopts)
        };

        let mut uncommited_stats_iter = {
            let mut readopts = self.db.internal_message_stats_uncommitted.new_read_config();
            readopts.set_snapshot(&snapshot.snapshot);
            db.raw_iterator_cf_opt(&uncommited_stats_cf, readopts)
        };

        let mut uncommited_diff_tail_iter = {
            let mut readopts = self
                .db
                .internal_message_diff_info_uncommitted
                .new_read_config();
            readopts.set_snapshot(&snapshot.snapshot);
            db.raw_iterator_cf_opt(&diff_tail_uncommitted_cf, readopts)
        };

        let mut uncommited_diff_info_iter = {
            let mut readopts = self
                .db
                .internal_message_diff_info_uncommitted
                .new_read_config();
            readopts.set_snapshot(&snapshot.snapshot);
            db.raw_iterator_cf_opt(&diff_info_uncommitted_cf, readopts)
        };

        // -- Process each range --
        for range in ranges {
            // 1) Commit messages in [from..to]
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

            Self::commit_range(
                &mut self.batch,
                &mut uncommited_messages_iter,
                &from_message_key.to_vec(),
                &to_message_key.to_vec(),
                &uncommited_messages_cf,
                &messages_cf,
            )?;

            // 2) Commit stats in [from..to]
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

            Self::commit_range(
                &mut self.batch,
                &mut uncommited_stats_iter,
                &from_stat_key.to_vec(),
                &to_stat_key.to_vec(),
                &uncommited_stats_cf,
                &stats_cf,
            )?;

            // 3) Collect diff tails in [from..to]
            let from_diff_tail_key = DiffTailKey {
                shard_ident: range.shard_ident,
                max_message: range.from,
            };
            let to_diff_tail_key = DiffTailKey {
                shard_ident: range.shard_ident,
                max_message: range.to,
            };

            let from_diff_tail_bytes = from_diff_tail_key.to_vec();
            let to_diff_tail_bytes = to_diff_tail_key.to_vec();

            // Track min/max seqno encountered
            uncommited_diff_tail_iter.seek(&from_diff_tail_bytes);

            let mut min_seqno = u32::MAX;
            let mut max_seqno = 0;

            loop {
                let Some((raw_key, raw_value)) = uncommited_diff_tail_iter.item() else {
                    match uncommited_diff_tail_iter.status() {
                        Ok(()) => break,
                        Err(e) => return Err(e.into()),
                    }
                };

                if raw_key > &to_diff_tail_bytes[..] {
                    break;
                }

                // block_seqno is stored in the first 4 bytes
                if raw_value.len() < 4 {
                    return Err(anyhow::anyhow!("Invalid diff tail value length"));
                }
                let block_seqno = u32::from_le_bytes(raw_value[..4].try_into()?);

                if block_seqno < min_seqno {
                    min_seqno = block_seqno;
                }
                if block_seqno > max_seqno {
                    max_seqno = block_seqno;
                }

                self.batch.delete_cf(&diff_tail_uncommitted_cf, raw_key);
                self.batch
                    .put_cf(&diff_tail_committed_cf, raw_key, raw_value);

                uncommited_diff_tail_iter.next();
            }

            // 4) Commit diff info [min_seqno..max_seqno]
            if min_seqno != u32::MAX && max_seqno != 0 {
                let from_diff_info_key = DiffInfoKey {
                    shard_ident: range.shard_ident,
                    seqno: min_seqno,
                }
                .to_vec();
                let to_diff_info_key = DiffInfoKey {
                    shard_ident: range.shard_ident,
                    seqno: max_seqno,
                }
                .to_vec();

                // Move the diff info
                Self::commit_range(
                    &mut self.batch,
                    &mut uncommited_diff_info_iter,
                    &from_diff_info_key,
                    &to_diff_info_key,
                    &diff_info_uncommitted_cf,
                    &diff_info_committed_cf,
                )?;
            }
        }

        Ok(())
    }

    /// Stores the queue version in the `internal_message_version` column family under the key `mc_version`
    pub fn set_last_applied_mc_block_id(&mut self, mc_block_id: &BlockId) {
        // Retrieve the column family handle for "internal_message_version"
        let cf = self.db.internal_message_var.cf();
        // Convert the version into a little-endian byte array and store it
        self.batch.put_cf(
            &cf,
            INT_QUEUE_LAST_APPLIED_MC_BLOCK_ID_KEY,
            mc_block_id.to_vec(),
        );
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

    pub fn calc_diffs_tail_committed(&self, from: &DiffTailKey) -> u32 {
        self.calc_diffs_tail(&self.db.internal_message_diffs_tail, from)
    }

    pub fn calc_diffs_tail_uncommitted(&self, from: &DiffTailKey) -> u32 {
        self.calc_diffs_tail(&self.db.internal_message_diffs_tail_uncommitted, from)
    }

    pub fn get_diff_info_committed(&self, key: &DiffInfoKey) -> Result<Option<Vec<u8>>> {
        self.get_diff_info(&self.db.internal_message_diff_info, key)
    }

    pub fn get_diff_info_uncommitted(&self, key: &DiffInfoKey) -> Result<Option<Vec<u8>>> {
        self.get_diff_info(&self.db.internal_message_diff_info_uncommitted, key)
    }

    pub fn get_last_applied_diff_info<T: ColumnFamily>(
        &self,
        table: &Table<T>,
        shard_ident: &ShardIdent,
    ) -> Result<Option<u32>> {
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        // Set the range to iterate over all the keys in the table for the given shard
        let from = DiffInfoKey::new(*shard_ident, 0);
        read_config.set_iterate_lower_bound(from.to_vec().to_vec());

        let to = DiffInfoKey::new(*shard_ident, u32::MAX);
        read_config.set_iterate_upper_bound(to.to_vec().to_vec());

        let cf = table.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        let key = DiffInfoKey::new(*shard_ident, u32::MAX);

        iter.seek_for_prev(key.to_vec().as_slice());

        let value = match iter.key() {
            Some(mut value) => {
                let key = DiffInfoKey::deserialize(&mut value);
                key.seqno
            }
            None => return Ok(None),
        };

        Ok(Some(value))
    }

    pub fn get_last_applied_block_seqno_uncommitted(
        &self,
        shard_ident: &ShardIdent,
    ) -> Result<Option<u32>> {
        let table = &self.db.internal_message_diff_info_uncommitted;
        self.get_last_applied_diff_info(table, shard_ident)
    }

    pub fn get_last_applied_block_seqno_committed(
        &self,
        shard_ident: &ShardIdent,
    ) -> Result<Option<u32>> {
        let table = &self.db.internal_message_diff_info;
        self.get_last_applied_diff_info(table, shard_ident)
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

    fn calc_diffs_tail<T: ColumnFamily>(&self, table: &Table<T>, from: &DiffTailKey) -> u32 {
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let from_bytes = from.to_vec();
        read_config.set_iterate_lower_bound(from_bytes.as_slice());
        let to = DiffTailKey {
            shard_ident: from.shard_ident,
            max_message: QueueKey::MAX,
        };
        read_config.set_iterate_upper_bound(to.to_vec().to_vec());

        let cf = table.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        iter.seek(&from_bytes);

        let mut count = 0;
        while let Some((_, _)) = iter.item() {
            count += 1;
            iter.next();
        }

        count
    }

    pub fn get_diff_info<T: ColumnFamily>(
        &self,
        table: &Table<T>,
        key: &DiffInfoKey,
    ) -> Result<Option<Vec<u8>>> {
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let cf = table.cf();
        let data = self.db.rocksdb().get_cf(&cf, key.to_vec().as_slice())?;

        Ok(data)
    }

    pub fn collect_committed_stats_in_range(
        &self,
        shard_ident: ShardIdent,
        partition: QueuePartitionIdx,
        from: &QueueKey,
        to: &QueueKey,
        result: &mut FastHashMap<IntAddr, u64>,
    ) -> Result<()> {
        let mut read_config = self.db.internal_message_stats.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let cf = self.db.internal_message_stats.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        Self::collect_dest_counts_in_range(&mut iter, shard_ident, partition, *from, *to, result)
    }

    pub fn collect_uncommitted_stats_in_range(
        &self,
        shard_ident: ShardIdent,
        partition: QueuePartitionIdx,
        from: &QueueKey,
        to: &QueueKey,
        result: &mut FastHashMap<IntAddr, u64>,
    ) -> Result<()> {
        let mut read_config = self.db.internal_message_stats_uncommitted.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let cf = self.db.internal_message_stats_uncommitted.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        Self::collect_dest_counts_in_range(&mut iter, shard_ident, partition, *from, *to, result)
    }

    fn collect_dest_counts_in_range(
        iter: &mut DBRawIterator<'_>,
        shard_ident: ShardIdent,
        partition: QueuePartitionIdx,
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
            let entry = result.entry(current_key.dest.to_int_addr()).or_insert(0);
            *entry += count;

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

fn delete_diff_tails_and_collect_seqno<T: ColumnFamily>(
    batch: &mut WriteBatch,
    db: &rocksdb::DB,
    diffs_tail_table: &Table<T>,
    shard_ident: ShardIdent,
    from_key: &[u8],
    to_key: &[u8],
) -> Result<(u32, u32)> {
    let read_opts = diffs_tail_table.new_read_config();
    let mut iter = db.raw_iterator_cf_opt(&diffs_tail_table.cf(), read_opts);

    iter.seek(from_key);

    let mut min_seqno = u32::MAX;
    let mut max_seqno = 0;

    loop {
        let Some((raw_key, raw_value)) = iter.item() else {
            match iter.status() {
                Ok(()) => break,
                Err(e) => return Err(e.into()),
            }
        };

        if raw_key > to_key {
            break;
        }

        let current_tail_key = DiffTailKey::from_slice(raw_key);
        if current_tail_key.shard_ident != shard_ident {
            break;
        }

        if raw_value.len() < 4 {
            return Err(anyhow::anyhow!(
                "Invalid diff tail value length: {} < 4",
                raw_value.len()
            ));
        }
        let block_seqno = u32::from_le_bytes(raw_value[..4].try_into()?);

        if block_seqno < min_seqno {
            min_seqno = block_seqno;
        }
        if block_seqno > max_seqno {
            max_seqno = block_seqno;
        }

        // Delete this tail
        batch.delete_cf(&diffs_tail_table.cf(), raw_key);

        // Move to next
        iter.next();
    }

    Ok((min_seqno, max_seqno))
}
