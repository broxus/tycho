use std::collections::BTreeMap;
use std::sync::Arc;

use tycho_types::models::{IntAddr, ShardIdent};
use tycho_util_proc::Transactional;

use crate::collator::messages_buffer::MessagesBuffer;
use crate::collator::messages_reader::state::ShardReaderState;
use crate::internal_queue::types::stats::QueueStatistics;
use crate::types::ProcessedTo;
use crate::types::processed_upto::InternalsRangeStuff;

#[derive(Default, Transactional)]
pub struct InternalsRangeReaderState {
    /// Buffer to store messages from the next iterator
    /// for accounts that have messages in the previous iterator
    /// until all messages from previous iterator are not read
    #[tx(transactional)]
    pub buffer: MessagesBuffer,

    /// Statistics shows all messages in current range
    pub msgs_stats: Option<Arc<QueueStatistics>>,
    /// Statistics shows remaining not read messages from current range.
    /// We reduce initial statistics by the number of messages that were read.
    #[tx(transactional)]
    pub remaning_msgs_stats: Option<QueueStatistics>,
    /// Statistics shows read messages in current range
    #[tx(transactional)]
    pub read_stats: Option<QueueStatistics>,

    pub shards: BTreeMap<ShardIdent, ShardReaderState>,

    /// Skip offset before collecting messages from this range.
    /// Because we should collect from others.
    pub skip_offset: u32,
    /// How many times internal messages were collected from all ranges.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u32,

    #[tx(state)]
    pub tx: Option<InternalsRangeReaderStateTx>,
}

impl InternalsRangeReaderState {
    pub fn contains_account_addr_in_remaning_msgs_stats(&self, account_addr: &IntAddr) -> bool {
        match &self.remaning_msgs_stats {
            None => false,
            Some(remaning_msgs_stats) => {
                remaning_msgs_stats.statistics().contains_key(account_addr)
            }
        }
    }

    pub fn is_fully_read(&self) -> bool {
        self.shards.values().all(|s| s.is_fully_read())
    }

    pub fn from_range_info(range_info: &InternalsRangeStuff, processed_to: &ProcessedTo) -> Self {
        let mut res = Self {
            skip_offset: range_info.skip_offset,
            processed_offset: range_info.processed_offset,
            ..Default::default()
        };

        for (shard_id, shard_range_info) in &range_info.shards {
            let shard_processed_to = processed_to.get(shard_id).copied().unwrap_or_default();
            let reader_state =
                ShardReaderState::from_range_info(shard_range_info, shard_processed_to);
            res.shards.insert(*shard_id, reader_state);
        }

        res
    }
}
