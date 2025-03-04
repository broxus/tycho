use std::cmp::Ordering;
use std::fs::File;

use anyhow::{bail, ensure, Result};
use everscale_types::models::{BlockId, IntAddr, Message, MsgInfo, OutMsgQueueUpdates, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr, RouterPartitions};
use tycho_util::FastHashMap;
use weedb::rocksdb::WriteBatch;
use weedb::{rocksdb, BoundedCfHandle, ColumnFamily, OwnedRawIterator, OwnedSnapshot, Table};

use crate::db::*;
use crate::model::{
    CommitPointerKey, CommitPointerValue, DiffInfo, DiffInfoKey, DiffTailKey, QueueRange,
    ShardsInternalMessagesKey, StatKey,
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
                    seqno: queue_diff.seqno,
                };

                batch.put_cf(
                    &diff_infos_cf,
                    diff_info_key.to_vec(),
                    tl_proto::serialize(diff_info),
                );

                // set commit pointer
                let commit_pointer_key = CommitPointerKey {
                    shard_ident: queue_diff.shard_ident,
                };

                let commit_pointer_value = CommitPointerValue {
                    queue_key: queue_diff.max_message,
                };

                batch.put_cf(
                    &this.db.internal_message_commit_pointer.cf(),
                    commit_pointer_key.to_vec(),
                    commit_pointer_value.to_vec(),
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

    pub fn insert_statistics(&mut self, key: &StatKey, count: u64) {
        let cf = self.db.internal_message_stats.cf();
        self.batch.put_cf(&cf, key.to_vec(), count.to_le_bytes());
    }

    pub fn insert_diff_tail(&mut self, key: &DiffTailKey, value: &[u8]) {
        let cf = self.db.internal_message_diffs_tail.cf();
        self.batch.put_cf(&cf, key.to_vec(), value);
    }

    pub fn insert_diff_info(&mut self, key: &DiffInfoKey, value: &[u8]) {
        let cf = self.db.internal_message_diff_info.cf();
        self.batch.put_cf(&cf, key.to_vec(), value);
    }

    pub fn insert_message(
        &mut self,
        key: &ShardsInternalMessagesKey,
        dest: &IntAddr,
        value: &[u8],
    ) {
        let cf = self.db.shard_internal_messages.cf();

        self.buffer.clear();
        self.buffer.reserve(1 + 8 + value.len());

        self.buffer.push(dest.workchain() as i8 as u8);
        self.buffer.extend_from_slice(&dest.prefix().to_le_bytes());
        self.buffer.extend_from_slice(value);

        self.batch.put_cf(&cf, key.to_vec(), self.buffer.as_slice());
    }

    pub fn commit_messages(
        &mut self,
        commit_pointers: &FastHashMap<ShardIdent, QueueKey>,
    ) -> Result<()> {
        let commit_pointers_cf = self.db.internal_message_commit_pointer.cf();

        for (&shard_ident, &queue_key) in commit_pointers.iter() {
            let key = CommitPointerKey { shard_ident }.to_vec();

            // Get the old value if it exists
            let old_pointer = self
                .db
                .rocksdb()
                .get_cf(&commit_pointers_cf, &key)?
                .map(|bytes| CommitPointerValue::from_slice(&bytes))
                .unwrap_or_default();

            match queue_key.cmp(&old_pointer.queue_key) {
                Ordering::Less => {
                    bail!("Trying to commit a pointer that is less than the old pointer")
                }
                Ordering::Greater => {
                    let new_val = CommitPointerValue { queue_key };
                    self.batch
                        .put_cf(&commit_pointers_cf, key, new_val.to_vec());
                }
                Ordering::Equal => {} // Ничего не делаем, если указатели равны
            }
        }

        Ok(())
    }

    /// Removes all keys that are strictly above the committed pointers in each partition.
    /// (Anything above `pointer + 1` is considered "uncommitted" and will be deleted.)
    ///
    /// - `commit_pointers`: a map of (`ShardIdent` -> last committed `QueueKey`)
    /// - `partitions`: a list of partitions (e.g. 0..255) to clear
    pub fn clear_uncommitted(
        &self,
        partitions: &[QueuePartitionIdx],
        commit_pointers: &FastHashMap<ShardIdent, CommitPointerValue>,
    ) -> Result<()> {
        let mut ranges = Vec::new();

        for (&shard_ident, pointer_val) in commit_pointers {
            // delete from next value of the pointer
            let from = pointer_val.queue_key.next_value();
            // to the maximum value
            let to = QueueKey::MAX;

            for &partition in partitions {
                ranges.push(QueueRange {
                    shard_ident,
                    partition,
                    from,
                    to,
                });
            }
        }

        self.delete(&ranges)
    }

    pub fn delete(&self, ranges: &[QueueRange]) -> Result<()> {
        let mut batch = WriteBatch::default();
        let snapshot = self.db.owned_snapshot();

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
                &from_diff_tail_bytes,
                &to_diff_tail_bytes,
                &snapshot,
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
pub struct InternalQueueSnapshot {
    db: BaseDb,
    snapshot: OwnedSnapshot,
}

impl InternalQueueSnapshot {
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
            Some(value) => {
                let key = DiffInfoKey::from_slice(value);
                key.seqno
            }
            None => return Ok(None),
        };

        Ok(Some(value))
    }

    pub fn get_last_applied_block_seqno(&self, shard_ident: &ShardIdent) -> Result<Option<u32>> {
        let table = &self.db.internal_message_diff_info;
        self.get_last_applied_diff_info(table, shard_ident)
    }

    pub fn iter_messages(
        &self,
        from: ShardsInternalMessagesKey,
        to: ShardsInternalMessagesKey,
    ) -> InternalQueueMessagesIter {
        let table = &self.db.shard_internal_messages;
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

    pub fn calc_diffs_tail(&self, from: &DiffTailKey) -> u32 {
        let table = &self.db.internal_message_diffs_tail;
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

    pub fn get_diff_info(&self, key: &DiffInfoKey) -> Result<Option<Vec<u8>>> {
        let table = &self.db.internal_message_diff_info;
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let cf = table.cf();
        let data = self.db.rocksdb().get_cf(&cf, key.to_vec().as_slice())?;

        Ok(data)
    }

    pub fn collect_stats_in_range(
        &self,
        shard_ident: ShardIdent,
        partition: QueuePartitionIdx,
        from: &QueueKey,
        to: &QueueKey,
        result: &mut FastHashMap<IntAddr, u64>,
    ) -> Result<()> {
        let mut read_config = self.db.internal_message_stats.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let from = StatKey {
            shard_ident,
            partition,
            min_message: *from,
            max_message: QueueKey::MIN,
            dest: RouterAddr::MIN,
        };

        let to = StatKey {
            shard_ident,
            partition,
            min_message: *to,
            max_message: QueueKey::MAX,
            dest: RouterAddr::MAX,
        };

        let from_bytes = from.to_vec();
        let to_bytes = to.to_vec();

        read_config.set_iterate_lower_bound(from_bytes.as_slice());
        read_config.set_iterate_upper_bound(to_bytes.as_slice());

        let cf = self.db.internal_message_stats.cf();
        let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);

        iter.seek_to_first();

        loop {
            let (key, value) = match iter.item() {
                Some(item) => item,
                None => match iter.status() {
                    Ok(()) => break,
                    Err(e) => return Err(e.into()),
                },
            };

            let current_key = StatKey::from_slice(key);

            let count = u64::from_le_bytes(value.try_into().unwrap());
            let entry = result.entry(current_key.dest.to_int_addr()).or_insert(0);
            *entry += count;

            iter.next();
        }

        Ok(())
    }

    /// Reads all commit pointers from the `internal_message_commit_pointer` CF.
    /// Returns a map: `ShardIdent` -> last committed `QueueKey`.
    pub fn read_commit_pointers(&self) -> Result<FastHashMap<ShardIdent, CommitPointerValue>> {
        let mut result = FastHashMap::default();

        // Access the commit pointer CF
        let commit_pointers_cf = self.db.internal_message_commit_pointer.cf();
        let mut read_config = self.db.internal_message_commit_pointer.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let mut iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&commit_pointers_cf, read_config);

        // Seek to the first key
        iter.seek_to_first();

        // Iterate through all commit pointers
        while iter.valid() {
            let (raw_key, raw_value) = match iter.item() {
                Some(item) => item,
                None => {
                    break;
                }
            };

            // Deserialize the commit pointer key
            let cp_key = CommitPointerKey::from_slice(raw_key);
            // Deserialize the commit pointer value
            let cp_val = CommitPointerValue::from_slice(raw_value);

            result.insert(cp_key.shard_ident, cp_val);

            iter.next();
        }
        // Check for any iteration errors
        iter.status()?;

        Ok(result)
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
    from_key: &[u8],
    to_key: &[u8],
    snapshot: &OwnedSnapshot,
) -> Result<(u32, u32)> {
    let mut read_opts = diffs_tail_table.new_read_config();
    read_opts.set_iterate_lower_bound(from_key);
    read_opts.set_snapshot(snapshot);

    // Create a raw iterator over the diffs_tail_table
    let mut iter = db.raw_iterator_cf_opt(&diffs_tail_table.cf(), read_opts);

    // Seek to the lower boundary
    iter.seek(from_key);

    let mut min_seqno = u32::MAX;
    let mut max_seqno = 0;

    // Iterate as long as the iterator is valid
    while iter.valid() {
        // Extract the current key
        let raw_key = match iter.key() {
            Some(k) => k,
            None => break, // if no key is available, stop
        };

        // Stop if we've gone past the upper boundary
        if raw_key > to_key {
            break;
        }

        // Extract the current value
        let raw_value = match iter.value() {
            Some(v) => v,
            None => break,
        };

        // Decode the seqno (first 4 bytes)
        ensure!(
            raw_value.len() >= 4,
            "Invalid diff tail value length: {} < 4",
            raw_value.len()
        );
        let block_seqno = u32::from_le_bytes(raw_value[..4].try_into()?);

        // Update min/max sequence numbers
        if block_seqno < min_seqno {
            min_seqno = block_seqno;
        }
        if block_seqno > max_seqno {
            max_seqno = block_seqno;
        }

        // Move to the next item
        iter.next();
    }

    batch.delete_range_cf(&diffs_tail_table.cf(), from_key, to_key);
    batch.delete_cf(&diffs_tail_table.cf(), to_key);

    // Check the iterator status for any internal errors
    iter.status()?;

    Ok((min_seqno, max_seqno))
}
