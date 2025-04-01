use everscale_types::models::{BlockId, IntAddr, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr};
use tycho_util::FastHashMap;
use weedb::{OwnedRawIterator, OwnedSnapshot};

use crate::model::{
    CommitPointerKey, CommitPointerValue, DiffInfoKey, DiffTailKey, ShardsInternalMessagesKey,
    StatKey,
};
use crate::store::internal_queue::iterator::InternalQueueMessagesIter;
use crate::util::StoredValue;
use crate::{BaseDb, INT_QUEUE_LAST_COMMITTED_MC_BLOCK_ID_KEY};

/// Represents a snapshot of the internal queue in the database.
pub struct InternalQueueSnapshot {
    pub db: BaseDb,
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

    /// Collects statistics in a specified range
    /// it's loading by diffs and adding to the result
    pub fn collect_stats_in_range(
        &self,
        shard_ident: ShardIdent,
        partition: QueuePartitionIdx,
        from: &QueueKey,
        to: &QueueKey,
        result: &mut FastHashMap<IntAddr, u64>,
    ) -> anyhow::Result<()> {
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

    /// Retrieves the queue version from the `internal_message_version` column family under the key `last_committed_mc_block_id`
    pub fn get_last_committed_mc_block_id(&self) -> anyhow::Result<Option<BlockId>> {
        let cf = self.db.internal_message_var.cf();
        let data = self
            .db
            .rocksdb()
            .get_cf(&cf, INT_QUEUE_LAST_COMMITTED_MC_BLOCK_ID_KEY)?;
        if let Some(bytes) = data {
            return Ok(Some(BlockId::from_slice(&bytes)));
        }

        Ok(None)
    }
}
