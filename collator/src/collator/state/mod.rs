use crate::collator::messages_reader::state::ShardReaderState;
use crate::collator::messages_reader::state::external::ExternalsPartitionRangeReaderState;
use crate::types::processed_upto::ShardRangeInfo;

pub struct DisplayRangeReaderStateByPartition<'a>(pub &'a ExternalsPartitionRangeReaderState);

impl std::fmt::Debug for DisplayRangeReaderStateByPartition<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for DisplayRangeReaderStateByPartition<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("skip_offset", &self.0.skip_offset)
            .field("processed_offset", &self.0.processed_offset)
            .field("buffer.msgs_count", &self.0.buffer.msgs_count())
            .finish()
    }
}

impl From<&ShardReaderState> for ShardRangeInfo {
    fn from(value: &ShardReaderState) -> Self {
        Self {
            from: value.from,
            to: value.to,
        }
    }
}
