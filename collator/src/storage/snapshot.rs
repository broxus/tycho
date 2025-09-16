use std::collections::BTreeMap;

use ahash::HashMapExt;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr};
use tycho_storage::kv::StoredValue;
use tycho_types::models::{BlockId, IntAddr, ShardIdent};
use tycho_util::{FastHashMap, FastHashSet};
use weedb::{OwnedRawIterator, OwnedSnapshot};

use super::INT_QUEUE_LAST_COMMITTED_MC_BLOCK_ID_KEY;
use super::db::InternalQueueDB;
use super::iterator::InternalQueueMessagesIter;
use super::models::{
    CommitPointerKey, CommitPointerValue, DiffInfoKey, DiffTailKey, ShardsInternalMessagesKey,
    StatKey,
};

pub type AccountStatistics = FastHashMap<IntAddr, u64>;
pub type SeparatedStatisticsByPartitions =
    FastHashMap<QueuePartitionIdx, BTreeMap<QueueKey, AccountStatistics>>;

/// Represents a snapshot of the internal queue in the database.
pub struct InternalQueueSnapshot {
    pub db: InternalQueueDB,
    pub snapshot: OwnedSnapshot,
}

impl InternalQueueSnapshot {
    /// Retrieves the last applied diff sequence number for a given shard.
    ///
    /// # Arguments
    /// * `shard_ident` - The shard identifier.
    ///
    /// # Returns
    /// * `Ok(Some(u32))` if a sequence number exists.
    /// * `Ok(None)` if no sequence number is found.
    /// * `Err(anyhow::Error)` if an error occurs.
    pub fn get_last_applied_diff_seqno(
        &self,
        shard_ident: &ShardIdent,
    ) -> anyhow::Result<Option<u32>> {
        let table = &self.db.internal_message_diff_info;
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
            Some(value) => DiffInfoKey::from_slice(value).seqno,

            None => return Ok(None),
        };

        Ok(Some(value))
    }

    /// Creates an iterator over internal queue messages within a specified key range.
    ///
    /// # Arguments
    /// * `from` - The lower bound key.
    /// * `to` - The upper bound key.
    ///
    /// # Returns
    /// * An iterator over the messages.
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

    /// Calculates the number of message diffs in a given range.
    ///
    /// # Arguments
    /// * `from` - The starting key.
    ///
    /// # Returns
    /// * The count of message diffs.
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

    /// Retrieves diff information for a specific shard and seqno
    ///
    /// # Arguments
    /// * `key` - The diff key to retrieve.
    ///
    /// # Returns
    /// * Bytes of diff information.
    pub fn get_diff_info(&self, key: &DiffInfoKey) -> anyhow::Result<Option<Vec<u8>>> {
        let table = &self.db.internal_message_diff_info;
        let mut read_config = table.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let cf = table.cf();
        let data = self.db.rocksdb().get_cf(&cf, key.to_vec().as_slice())?;

        Ok(data)
    }

    /// Collects statistics in a specified range.
    /// It loads by diffs and adds to the result
    pub fn collect_stats_in_range(
        &self,
        shard_ident: &ShardIdent,
        partition: QueuePartitionIdx,
        from: &QueueKey,
        to: &QueueKey,
        result: &mut FastHashMap<IntAddr, u64>,
    ) -> anyhow::Result<()> {
        let mut read_config = self.db.internal_message_stats.new_read_config();
        read_config.set_snapshot(&self.snapshot);

        let from = StatKey {
            shard_ident: *shard_ident,
            partition,
            max_message: *from,
            dest: RouterAddr::MIN,
        };

        let to = StatKey {
            shard_ident: *shard_ident,
            partition,
            max_message: *to,
            dest: RouterAddr::MAX,
        };

        let from_bytes = from.to_vec();
        let to_bytes = to.to_vec();

        read_config.set_iterate_lower_bound(&from_bytes[..StatKey::PREFIX_SIZE]);
        read_config.set_iterate_upper_bound(&to_bytes[..StatKey::PREFIX_SIZE]);

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

    /// Collects statistics in the specified [from..=to] range for a single `shard_ident`,
    /// by `partitions`.
    ///
    /// The final result merges all data into a single structure:
    ///   `FastHashMap<QueuePartitionIdx, BTreeMap<QueueKey, FastHashMap<IntAddr, u64>>>`
    ///
    /// Where each `QueuePartitionIdx` maps to `BTreeMap<QueueKey, FastHashMap<IntAddr, u64>>`,
    /// and each `QueueKey` maps to a `FastHashMap<IntAddr, u64>` (destination -> count).
    pub fn collect_separated_stats_in_range_for_partitions(
        &self,
        shard_ident: &ShardIdent,
        partitions: &FastHashSet<QueuePartitionIdx>,
        from: &QueueKey,
        to: &QueueKey,
    ) -> anyhow::Result<SeparatedStatisticsByPartitions> {
        let mut result = SeparatedStatisticsByPartitions::new();

        for &partition in partitions {
            let mut read_config = self.db.internal_message_stats.new_read_config();
            read_config.set_snapshot(&self.snapshot);

            // We'll set the bounds for this specific partition, from..to.
            let from_key = StatKey {
                shard_ident: *shard_ident,
                partition,
                max_message: *from,
                dest: RouterAddr::MIN,
            };
            let to_key = StatKey {
                shard_ident: *shard_ident,
                partition,
                max_message: *to,
                dest: RouterAddr::MAX,
            };

            let from_bytes = from_key.to_vec();
            let to_bytes = to_key.to_vec();

            // Restrict the iterator to only the specified prefix range.
            read_config.set_iterate_lower_bound(&from_bytes[..StatKey::PREFIX_SIZE]);
            read_config.set_iterate_upper_bound(&to_bytes[..StatKey::PREFIX_SIZE]);

            let cf = self.db.internal_message_stats.cf();
            let mut iter = self.db.rocksdb().raw_iterator_cf_opt(&cf, read_config);
            iter.seek_to_first();

            // Read each matching entry for this partition and add to results
            let partition_stats = result.entry(partition).or_default();
            loop {
                let (key_bytes, value_bytes) = match iter.item() {
                    Some(item) => item,
                    None => {
                        // No more items; check iterator status to detect errors.
                        match iter.status() {
                            Ok(()) => break,
                            Err(e) => return Err(e.into()),
                        }
                    }
                };

                let current_key = StatKey::from_slice(key_bytes);
                let count = u64::from_le_bytes(value_bytes.try_into().unwrap());

                // Insert into partition stats, grouped by the `current_key.max_message`.
                partition_stats
                    .entry(current_key.max_message)
                    .or_default()
                    .entry(current_key.dest.to_int_addr())
                    .and_modify(|c| *c += count)
                    .or_insert(count);

                iter.next();
            }
        }

        Ok(result)
    }

    /// Reads all commit pointers from the `internal_message_commit_pointer` CF.
    /// Returns a map: `ShardIdent` -> last committed `QueueKey`.
    pub fn read_commit_pointers(
        &self,
    ) -> anyhow::Result<FastHashMap<ShardIdent, CommitPointerValue>> {
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

    pub fn get_last_committed_mc_block_id(&self) -> anyhow::Result<Option<BlockId>> {
        let cf = self.db.internal_message_var.cf();
        let res = self
            .db
            .rocksdb()
            .get_cf(&cf, INT_QUEUE_LAST_COMMITTED_MC_BLOCK_ID_KEY)?
            .map(|bytes| BlockId::from_slice(&bytes));
        Ok(res)
    }
}
