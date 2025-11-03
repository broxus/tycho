use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::ensure;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_types::cell::HashBytes;
use tycho_types::models::{IntAddr, ShardIdent, StdAddr};
use tycho_util::FastHashSet;

use crate::collator::messages_buffer::{
    FillMessageGroupResult, IncludeAllMessages, MessageGroup, MessagesBufferLimits,
};
use crate::collator::messages_reader::internals_reader::InternalsPartitionReader;
use crate::collator::messages_reader::state::ShardReaderState;
use crate::collator::messages_reader::state::internal::InternalsRangeReaderState;
use crate::internal_queue::iterator::QueueIterator;
use crate::internal_queue::types::message::InternalMessageValue;
use crate::internal_queue::types::ranges::{Bound, QueueShardBoundedRange};
use crate::queue_adapter::MessageQueueAdapter;
use crate::tracing_targets;
use crate::types::SaturatingAddAssign;
use crate::types::processed_upto::BlockSeqno;

#[derive(Debug, Default)]
pub struct InternalsRangeReaderInfo {
    pub last_to_lts: BTreeMap<ShardIdent, ShardReaderState>,
    pub last_range_block_seqno: BlockSeqno,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InternalsRangeReaderKind {
    Existing,
    Next,
    NewMessages,
}

pub struct InternalsRangeReader<V: InternalMessageValue> {
    pub partition_id: QueuePartitionIdx,
    pub for_shard_id: ShardIdent,
    pub seqno: BlockSeqno,
    pub kind: InternalsRangeReaderKind,
    /// Target limits for filling message group from the buffer
    pub buffer_limits: MessagesBufferLimits,
    pub iterator_opt: Option<Box<dyn QueueIterator<V>>>,
    pub initialized: bool,
}

impl<V: InternalMessageValue> InternalsRangeReader<V> {
    pub fn init(
        &mut self,
        reader_state: &mut InternalsRangeReaderState,
        mq_adapter: &Arc<dyn MessageQueueAdapter<V>>,
    ) -> anyhow::Result<()> {
        // do not init iterator if range is fully read
        if !reader_state.is_fully_read() {
            let mut ranges = Vec::with_capacity(reader_state.shards.len());

            for (shard_id, shard_reader_state) in &reader_state.shards {
                let shard_range_to = QueueKey::max_for_lt(shard_reader_state.to);
                ranges.push(QueueShardBoundedRange {
                    shard_ident: *shard_id,
                    from: Bound::Excluded(shard_reader_state.current_position),
                    to: Bound::Included(shard_range_to),
                });
            }

            let iterator =
                mq_adapter.create_iterator(self.for_shard_id, self.partition_id, ranges)?;

            self.iterator_opt = Some(iterator);
        }

        self.initialized = true;

        tracing::debug!(target: tracing_targets::COLLATOR,
            partition_id = %self.partition_id,
            seqno = self.seqno,
            fully_read = reader_state.is_fully_read(),
            "internals reader: initialized range reader",
        );

        Ok(())
    }

    pub fn collect_messages(
        &mut self,
        msg_group: &mut MessageGroup,
        prev_par_readers: &BTreeMap<QueuePartitionIdx, InternalsPartitionReader<V>>,
        prev_readers_states: &Vec<&InternalsRangeReaderState>,
        prev_msg_groups: &BTreeMap<QueuePartitionIdx, MessageGroup>,
        already_skipped_accounts: &mut FastHashSet<HashBytes>,
        reader_state: &mut InternalsRangeReaderState,
    ) -> CollectMessagesFromRangeReaderResult {
        let FillMessageGroupResult {
            collected_int_msgs,
            ops_count,
            ..
        } = reader_state.buffer.fill_message_group::<_, _>(
            msg_group,
            self.buffer_limits.slots_count,
            self.buffer_limits.slot_vert_size,
            already_skipped_accounts,
            |account_id| {
                let mut check_ops_count = 0;

                let dst_addr = IntAddr::from((self.for_shard_id.workchain() as i8, *account_id));

                // check by msg group from previous partition (e.g. from partition 0 when collecting from 1)
                for msg_group in prev_msg_groups.values() {
                    if msg_group.messages_count() > 0 {
                        check_ops_count.saturating_add_assign(1);
                        if msg_group.contains_account(account_id) {
                            return (true, check_ops_count);
                        }
                    }
                }

                // check by previous partitions
                for prev_par_reader in prev_par_readers.values() {
                    // check buffers in previous partition
                    for prev_par_range_reader in prev_par_reader.range_readers().values() {
                        let reader_state = prev_par_reader
                            .reader_state()
                            .ranges
                            .get(&prev_par_range_reader.seqno)
                            .unwrap();

                        if reader_state.buffer.msgs_count() > 0 {
                            check_ops_count.saturating_add_assign(1);
                            if reader_state.buffer.account_messages_count(account_id) > 0 {
                                return (true, check_ops_count);
                            }
                        }
                    }

                    // check stats in previous partition
                    check_ops_count.saturating_add_assign(1);

                    if let Some(remaning_msgs_stats) = &prev_par_reader.remaning_msgs_stats
                        && remaning_msgs_stats.statistics().contains_key(&dst_addr)
                    {
                        return (true, check_ops_count);
                    }
                }

                // check by previous ranges in current partition
                for reader_state in prev_readers_states {
                    // check buffer
                    if reader_state.buffer.msgs_count() > 0 {
                        check_ops_count.saturating_add_assign(1);
                        if reader_state.buffer.account_messages_count(account_id) > 0 {
                            return (true, check_ops_count);
                        }
                    }

                    // check stats
                    if !reader_state.is_fully_read() {
                        check_ops_count.saturating_add_assign(1);
                        if reader_state.contains_account_addr_in_remaning_msgs_stats(&dst_addr) {
                            return (true, check_ops_count);
                        }
                    }
                }

                (false, check_ops_count)
            },
            IncludeAllMessages,
        );

        CollectMessagesFromRangeReaderResult {
            collected_int_msgs,
            ops_count,
        }
    }
}

pub struct CollectMessagesFromRangeReaderResult {
    pub collected_int_msgs: Vec<QueueKey>,
    pub ops_count: u64,
}

pub fn partitions_have_intersecting_accounts<V: InternalMessageValue>(
    current: &InternalsPartitionReader<V>,
    next: &InternalsPartitionReader<V>,
) -> anyhow::Result<Option<IntAddr>> {
    ensure!(current.for_shard_id == next.for_shard_id);
    ensure!(current.partition_id.is_zero());
    ensure!(next.partition_id > current.partition_id);

    let current_stats = current.remaning_msgs_stats.as_ref();
    let next_stats = next.remaning_msgs_stats.as_ref();

    let (Some(current_stats), Some(next_stats)) = (current_stats, next_stats) else {
        return Ok(None);
    };

    let workchain = next.for_shard_id.workchain() as i8;

    // Check buffers in range readers
    for range_reader_seqno in next.range_readers.keys() {
        let state = next.state().ranges.get(&range_reader_seqno).unwrap();

        for (account_address, _) in state.buffer.iter() {
            let addr = IntAddr::Std(StdAddr::new(workchain, *account_address));
            if current_stats.contains(&addr) {
                return Ok(Some(addr));
            }
        }
    }

    // Check next_stats
    for item in next_stats.statistics() {
        let addr = item.key();
        if current_stats.contains(addr) {
            return Ok(Some(addr.clone()));
        }
    }

    Ok(None)
}
