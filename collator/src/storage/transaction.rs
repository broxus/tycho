use anyhow::{Result, ensure};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr};
use tycho_storage::kv::StoredValue;
use tycho_types::models::{BlockId, IntAddr, ShardIdent};
use tycho_util::{FastHashMap, FastHashSet};
use weedb::rocksdb::WriteBatch;
use weedb::{BoundedCfHandle, ColumnFamily, OwnedSnapshot, Table, rocksdb};

use super::INT_QUEUE_LAST_COMMITTED_MC_BLOCK_ID_KEY;
use super::db::InternalQueueDB;
use super::models::{
    CommitPointerKey, CommitPointerValue, DiffInfoKey, DiffTailKey, QueueRange,
    ShardsInternalMessagesKey, StatKey,
};
use crate::tracing_targets;

pub struct InternalQueueTransaction {
    pub db: InternalQueueDB,
    pub batch: WriteBatch,
    pub buffer: Vec<u8>,
}

impl InternalQueueTransaction {
    pub fn size(&self) -> (usize, usize) {
        (self.batch.len(), self.batch.size_in_bytes())
    }

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

    /// Updates commit pointers in message queue.
    /// ATTENTION! Overrides old value without checks. Should validate the new value in the calling code.
    pub fn commit_messages(
        &mut self,
        commit_pointers: FastHashMap<ShardIdent, (QueueKey, u32)>,
    ) -> Result<()> {
        let commit_pointers_cf = self.db.internal_message_commit_pointer.cf();

        for (shard_ident, (queue_key, seqno)) in commit_pointers {
            let key = CommitPointerKey { shard_ident }.to_vec();

            let new_val = CommitPointerValue { queue_key, seqno };

            self.batch
                .put_cf(&commit_pointers_cf, key, new_val.to_vec());
        }

        Ok(())
    }

    /// Removes all keys that are strictly above the committed pointers in each partition.
    /// (Anything above `pointer + 1` is considered "uncommitted" and will be deleted.)
    ///
    /// - `commit_pointers`: a map of (`ShardIdent` -> last committed `QueueKey`)
    /// - `partitions`: a list of partitions (e.g. 0..255) to clear
    /// - `top_shards`: a list of all shards for backoff when no commit pointers
    pub fn clear_uncommitted(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        commit_pointers: &FastHashMap<ShardIdent, CommitPointerValue>,
        top_shards: &[ShardIdent],
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

        // backoff: if no commit pointers (no any committed diff)
        //          create full ranges for delete for each shard
        if ranges.is_empty() {
            for &shard_ident in top_shards {
                for &partition in partitions {
                    ranges.push(QueueRange {
                        shard_ident,
                        partition,
                        from: QueueKey::MIN,
                        to: QueueKey::MAX,
                    });
                }
            }
        }

        tracing::debug!(target: tracing_targets::MQ,
            ?commit_pointers,
            ?ranges,
            "clear_uncommitted_state",
        );

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
                max_message: range.from,
                dest: RouterAddr::MIN,
            };

            let end_stat_key = StatKey {
                shard_ident: range.shard_ident,
                partition: range.partition,
                max_message: range.to,
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

    /// Stores mc block id on which the queue was committed.
    /// ATTENTION! Overrides old value without checks. Should validate the new value in the calling code.
    pub fn set_last_committed_mc_block_id(&mut self, mc_block_id: &BlockId) -> Result<()> {
        let cf = self.db.internal_message_var.cf();

        self.batch.put_cf(
            &cf,
            INT_QUEUE_LAST_COMMITTED_MC_BLOCK_ID_KEY,
            mc_block_id.to_vec(),
        );

        Ok(())
    }
}

pub fn delete_diff_tails_and_collect_seqno<T: ColumnFamily>(
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
