use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent};
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx, RouterAddr, RouterPartitions};
use tycho_storage::model::{
    CommitPointerValue, DiffInfo, DiffInfoKey, DiffTailKey, ShardsInternalMessagesKey, StatKey,
};
use tycho_storage::snapshot::InternalQueueSnapshot;
use tycho_storage::transaction::InternalQueueTransaction;
use tycho_storage::Storage;
use tycho_util::metrics::HistogramGuard;
use tycho_util::{FastHashMap, FastHashSet};

use crate::internal_queue::state::state_iterator::{StateIterator, StateIteratorImpl};
use crate::internal_queue::types::{
    AccountStatistics, DiffStatistics, DiffZone, InternalMessageValue, PartitionRouter,
    QueueDiffWithMessages, QueueShardRange, SeparatedStatisticsByPartitions,
};
use crate::types::ProcessedTo;
// CONFIG

pub struct QueueStateConfig {
    pub storage: Storage,
}

// FACTORY

impl<F, R, V> QueueStateFactory<V> for F
where
    F: Fn() -> R,
    R: QueueState<V>,
    V: InternalMessageValue,
{
    type QueueState = R;

    fn create(&self) -> Self::QueueState {
        self()
    }
}

pub struct QueueStateImplFactory {
    pub storage: Storage,
}

impl QueueStateImplFactory {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> QueueStateFactory<V> for QueueStateImplFactory {
    type QueueState = QueueStateStdImpl;

    fn create(&self) -> Self::QueueState {
        QueueStateStdImpl::new(self.storage.clone())
    }
}

pub trait QueueStateFactory<V: InternalMessageValue> {
    type QueueState: QueueState<V>;

    fn create(&self) -> Self::QueueState;
}

// TRAIT

pub trait QueueState<V: InternalMessageValue>: Send + Sync {
    /// Create snapshot
    fn snapshot(&self) -> InternalQueueSnapshot;

    /// Create iterator for given partition and ranges
    fn iterator(
        &self,
        snapshot: &InternalQueueSnapshot,
        receiver: ShardIdent,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<Box<dyn StateIterator<V>>>;

    /// Delete messages in given partition and ranges
    fn delete(&self, partition: QueuePartitionIdx, ranges: &[QueueShardRange]) -> Result<()>;
    /// Set commit pointers and last applied mc block id
    fn commit(
        &self,
        commit_pointers: &FastHashMap<ShardIdent, (QueueKey, u32)>,
        mc_block_id: &BlockId,
    ) -> Result<()>;

    /// Load statistics for given partition and ranges
    fn load_diff_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        range: &[QueueShardRange],
    ) -> Result<AccountStatistics>;

    fn load_separated_diff_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        range: &QueueShardRange,
    ) -> Result<SeparatedStatisticsByPartitions>;

    /// Get last committed mc block id
    /// Returns None if no block was applied
    fn get_last_committed_mc_block_id(&self) -> Result<Option<BlockId>>;
    /// Get length of diffs tail
    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32;
    /// Get diff info by diff seqno
    fn get_diff_info(
        &self,
        shard_ident: &ShardIdent,
        seqno: u32,
        zone: DiffZone,
    ) -> Result<Option<DiffInfo>>;
    /// Get last applied block seqno by shard ident from committed and uncommited zone
    fn get_last_applied_seqno(&self, shard_ident: &ShardIdent) -> Result<Option<u32>>;
    /// Get commit pointers
    fn get_commit_pointers(&self) -> Result<FastHashMap<ShardIdent, CommitPointerValue>>;

    fn write_diff(
        &self,
        block_id_short: &BlockIdShort,
        statistics: &DiffStatistics,
        hash: HashBytes,
        diff: QueueDiffWithMessages<V>,
    ) -> Result<()>;
    fn clear_uncommitted(&self, partitions: &FastHashSet<QueuePartitionIdx>) -> Result<()>;
}

// IMPLEMENTATION

pub struct QueueStateStdImpl {
    storage: Storage,
}

impl QueueStateStdImpl {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl<V: InternalMessageValue> QueueState<V> for QueueStateStdImpl {
    fn snapshot(&self) -> InternalQueueSnapshot {
        let _histogram = HistogramGuard::begin("tycho_internal_queue_snapshot_time");
        self.storage.internal_queue_storage().make_snapshot()
    }

    fn iterator(
        &self,
        snapshot: &InternalQueueSnapshot,
        receiver: ShardIdent,
        partition: QueuePartitionIdx,
        ranges: &[QueueShardRange],
    ) -> Result<Box<dyn StateIterator<V>>> {
        let mut shards_iters = Vec::new();

        for range in ranges {
            // exclude from key
            let from_key = range.from.next_value();
            let from = ShardsInternalMessagesKey::new(partition, range.shard_ident, from_key);
            // include to key
            let to_key = range.to.next_value();
            let to = ShardsInternalMessagesKey::new(partition, range.shard_ident, to_key);
            shards_iters.push((snapshot.iter_messages(from, to), range.shard_ident));
        }

        let iterator = StateIteratorImpl::new(shards_iters, receiver)?;
        Ok(Box::new(iterator))
    }

    fn delete(&self, partition: QueuePartitionIdx, ranges: &[QueueShardRange]) -> Result<()> {
        let mut queue_ranges = vec![];
        for range in ranges {
            queue_ranges.push(tycho_storage::model::QueueRange {
                partition,
                shard_ident: range.shard_ident,
                from: range.from,
                to: range.to,
            });
        }

        let tx = self.storage.internal_queue_storage().begin_transaction();
        tx.delete(&queue_ranges)?;
        tx.write()
    }

    fn commit(
        &self,
        commit_pointers: &FastHashMap<ShardIdent, (QueueKey, u32)>,
        mc_block_id: &BlockId,
    ) -> Result<()> {
        let mut tx = self.storage.internal_queue_storage().begin_transaction();
        tx.commit_messages(commit_pointers)?;
        tx.set_last_applied_mc_block_id(mc_block_id);
        tx.write()
    }

    fn load_diff_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        ranges: &[QueueShardRange],
    ) -> Result<AccountStatistics> {
        let _histogram = HistogramGuard::begin("tycho_internal_queue_statistics_load_time");
        let snapshot = self.storage.internal_queue_storage().make_snapshot();
        let mut result = FastHashMap::default();

        for range in ranges {
            for partition in partitions {
                snapshot.collect_stats_in_range(
                    &range.shard_ident,
                    *partition,
                    &range.from,
                    &range.to,
                    &mut result,
                )?;
            }
        }

        Ok(result)
    }

    fn load_separated_diff_statistics(
        &self,
        partitions: &FastHashSet<QueuePartitionIdx>,
        range: &QueueShardRange,
    ) -> Result<SeparatedStatisticsByPartitions> {
        let _histogram = HistogramGuard::begin("tycho_internal_queue_statistics_load_time");
        let snapshot = self.storage.internal_queue_storage().make_snapshot();

        let result = snapshot.collect_separated_stats_in_range_for_partitions(
            &range.shard_ident,
            partitions,
            &range.from,
            &range.to,
        )?;

        Ok(result)
    }

    fn get_last_committed_mc_block_id(&self) -> Result<Option<BlockId>> {
        let snapshot = self.storage.internal_queue_storage().make_snapshot();
        snapshot.get_last_committed_mc_block_id()
    }

    fn get_diffs_tail_len(&self, shard_ident: &ShardIdent, from: &QueueKey) -> u32 {
        let snapshot = self.storage.internal_queue_storage().make_snapshot();
        snapshot.calc_diffs_tail(&DiffTailKey {
            shard_ident: *shard_ident,
            max_message: *from,
        })
    }

    fn get_diff_info(
        &self,
        shard_ident: &ShardIdent,
        seqno: u32,
        zone: DiffZone,
    ) -> Result<Option<DiffInfo>> {
        let snapshot = self.storage.internal_queue_storage().make_snapshot();

        let diff_info_bytes = snapshot.get_diff_info(&DiffInfoKey {
            shard_ident: *shard_ident,
            seqno,
        })?;

        let diff_info_bytes = match diff_info_bytes {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let diff_info: DiffInfo = tl_proto::deserialize(&diff_info_bytes)?;

        match zone {
            DiffZone::Both => {}
            DiffZone::Committed => {
                let commit_pointers = snapshot.read_commit_pointers()?;
                if let Some(commit_pointer) = commit_pointers.get(shard_ident) {
                    // if true then diff is in uncommitted zone
                    if commit_pointer.queue_key < diff_info.max_message {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
            DiffZone::Uncommitted => {
                let commit_pointers = snapshot.read_commit_pointers()?;
                if let Some(commit_pointer) = commit_pointers.get(shard_ident) {
                    // if true then diff is in committed zone
                    if commit_pointer.queue_key >= diff_info.max_message {
                        return Ok(None);
                    }
                }
            }
        }

        Ok(Some(diff_info))
    }

    fn get_last_applied_seqno(&self, shard_ident: &ShardIdent) -> Result<Option<u32>> {
        let snapshot = self.storage.internal_queue_storage().make_snapshot();
        snapshot.get_last_applied_diff_seqno(shard_ident)
    }

    fn get_commit_pointers(&self) -> Result<FastHashMap<ShardIdent, CommitPointerValue>> {
        self.storage
            .internal_queue_storage()
            .make_snapshot()
            .read_commit_pointers()
    }

    fn write_diff(
        &self,
        block_id_short: &BlockIdShort,
        statistics: &DiffStatistics,
        hash: HashBytes,
        diff: QueueDiffWithMessages<V>,
    ) -> Result<()> {
        let mut tx = self.storage.internal_queue_storage().begin_transaction();

        Self::add_messages(
            &mut tx,
            block_id_short.shard,
            &diff.partition_router,
            &diff.messages,
        )?;
        Self::add_statistics(&mut tx, statistics)?;
        Self::add_diff_tail(&mut tx, block_id_short, statistics.max_message());

        let src_router_partition = diff.partition_router.to_router_partitions_src();
        let dst_router_partition = diff.partition_router.to_router_partitions_dst();

        Self::add_diff_info(
            &mut tx,
            block_id_short,
            statistics,
            hash,
            diff.processed_to,
            src_router_partition,
            dst_router_partition,
        );

        let _histogram = HistogramGuard::begin("tycho_internal_queue_write_diff_time");

        tx.write()
    }

    fn clear_uncommitted(&self, partitions: &FastHashSet<QueuePartitionIdx>) -> Result<()> {
        let snapshot = self.storage.internal_queue_storage().make_snapshot();
        let pointers = snapshot.read_commit_pointers()?;
        let tx = self.storage.internal_queue_storage().begin_transaction();
        tx.clear_uncommitted(partitions, &pointers)?;
        tx.write()
    }
}

impl QueueStateStdImpl {
    /// write new messages to storage
    fn add_messages<V: InternalMessageValue>(
        internal_queue_tx: &mut InternalQueueTransaction,
        source: ShardIdent,
        partition_router: &PartitionRouter,
        messages: &BTreeMap<QueueKey, Arc<V>>,
    ) -> Result<()> {
        let _histogram = HistogramGuard::begin("tycho_internal_queue_apply_diff_add_messages_time");
        let mut buffer = Vec::new();
        for (internal_message_key, message) in messages {
            let destination = message.destination();
            let partition = partition_router.get_partition(Some(message.source()), destination);

            buffer.clear();
            message.serialize(&mut buffer);

            internal_queue_tx.insert_message(
                &ShardsInternalMessagesKey::new(partition, source, *internal_message_key),
                destination,
                &buffer,
            );
        }

        Ok(())
    }

    /// write new statistics to storage
    fn add_statistics(
        internal_queue_tx: &mut InternalQueueTransaction,
        diff_statistics: &DiffStatistics,
    ) -> Result<()> {
        let _histogram =
            HistogramGuard::begin("tycho_internal_queue_apply_diff_add_statistics_time");
        let shard_ident = diff_statistics.shard_ident();
        let max_message = diff_statistics.max_message();

        for (partition, values) in diff_statistics.iter() {
            for (addr, count) in values {
                let Some(dest) = RouterAddr::from_int_addr(addr) else {
                    anyhow::bail!("cannot add VarAddr to router statistics");
                };

                let key = StatKey {
                    shard_ident: *shard_ident,
                    partition: *partition,
                    max_message: *max_message,
                    dest,
                };

                internal_queue_tx.insert_statistics(&key, *count);
            }
            metrics::counter!(
                "tycho_internal_queue_apply_diff_add_statistics_accounts_count",
                "partition" => partition.to_string(),
            )
            .increment(values.len() as u64);
        }

        Ok(())
    }

    fn add_diff_tail(
        internal_queue_tx: &mut InternalQueueTransaction,
        block_id_short: &BlockIdShort,
        max_message: &QueueKey,
    ) {
        internal_queue_tx.insert_diff_tail(
            &DiffTailKey {
                shard_ident: block_id_short.shard,
                max_message: *max_message,
            },
            block_id_short.seqno.to_le_bytes().as_slice(),
        );
    }

    fn add_diff_info(
        internal_queue_tx: &mut InternalQueueTransaction,
        block_id_short: &BlockIdShort,
        diff_statistics: &DiffStatistics,
        hash: HashBytes,
        processed_to: ProcessedTo,
        router_partitions_src: RouterPartitions,
        router_partitions_dst: RouterPartitions,
    ) {
        let shard_messages_count = diff_statistics.shards_messages_count();

        let key = DiffInfoKey {
            shard_ident: block_id_short.shard,
            seqno: block_id_short.seqno,
        };

        let diff_info = DiffInfo {
            min_message: *diff_statistics.min_message(),
            max_message: *diff_statistics.max_message(),
            shards_messages_count: shard_messages_count.clone(),
            hash,
            processed_to,
            router_partitions_src,
            router_partitions_dst,
            seqno: block_id_short.seqno,
        };

        let serialized_diff_info = tl_proto::serialize(diff_info);

        internal_queue_tx.insert_diff_info(&key, &serialized_diff_info);
    }
}
