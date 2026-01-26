use crate::collator::messages_reader::state::DisplayShardReaderState;
use crate::collator::messages_reader::state::int::partition_reader::InternalsPartitionReaderState;
use crate::collator::messages_reader::state::int::range_reader::InternalsRangeReaderState;
use crate::types::DebugIter;
use crate::types::processed_upto::{InternalsProcessedUptoStuff, InternalsRangeStuff};

pub mod partition_reader;
pub mod range_reader;
pub mod reader;

impl From<&InternalsPartitionReaderState> for InternalsProcessedUptoStuff {
    fn from(value: &InternalsPartitionReaderState) -> Self {
        Self {
            processed_to: value.processed_to.clone(),
            ranges: value.ranges().iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}

impl From<&InternalsRangeReaderState> for InternalsRangeStuff {
    fn from(value: &InternalsRangeReaderState) -> Self {
        Self {
            skip_offset: value.skip_offset,
            processed_offset: value.processed_offset,
            shards: value.shards.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}

pub struct DebugInternalsRangeReaderState<'a>(pub &'a InternalsRangeReaderState);

impl std::fmt::Debug for DebugInternalsRangeReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let read_stats_account_count = self
            .0
            .read_stats
            .as_ref()
            .map_or(0, |s| s.statistics().len());

        f.debug_struct("")
            .field("skip_offset", &self.0.skip_offset)
            .field("processed_offset", &self.0.processed_offset)
            .field("buffer.msgs_count", &self.0.buffer.msgs_count())
            .field(
                "remaning_msgs_stats.accounts_count",
                &self
                    .0
                    .remaning_msgs_stats
                    .as_ref()
                    .map(|s| s.statistics().len()),
            )
            .field("read_stats.accounts_count", &read_stats_account_count)
            .field(
                "shards",
                &DebugIter(
                    self.0
                        .shards
                        .iter()
                        .map(|(shard_id, r_s)| (shard_id, DisplayShardReaderState(r_s))),
                ),
            )
            .finish()
    }
}
