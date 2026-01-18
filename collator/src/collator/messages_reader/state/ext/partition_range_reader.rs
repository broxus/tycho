use tycho_util_proc::Transactional;

use crate::collator::messages_buffer::{
    BufferFillStateByCount, BufferFillStateBySlots, MessagesBuffer, MessagesBufferLimits,
};
use crate::types::processed_upto::ExternalsRangeInfo;

#[derive(Transactional)]
pub struct ExternalsPartitionRangeReaderState {
    /// Buffer to store external messages
    /// before collect them to the next execution group
    #[tx(transactional)]
    pub buffer: MessagesBuffer,
    /// Skip offset before collecting messages from this range.
    /// Because we should collect from others.
    pub skip_offset: u32,
    /// How many times externals messages were collected from all ranges.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u32,
    /// Last chain time used to check externals expiration.
    /// If `next_chain_time` was not changed on collect,
    /// we can omit the expire check.
    pub last_expire_check_on_ct: Option<u64>,

    #[tx(state)]
    tx: Option<ExternalsPartitionRangeReaderStateTx>,
}

impl ExternalsPartitionRangeReaderState {
    pub fn new(
        buffer: MessagesBuffer,
        skip_offset: u32,
        processed_offset: u32,
        last_expire_check_on_ct: Option<u64>,
    ) -> Self {
        Self {
            buffer,
            skip_offset,
            processed_offset,
            last_expire_check_on_ct,
            tx: None,
        }
    }

    pub fn check_buffer_fill_state(
        &self,
        buffer_limits: &MessagesBufferLimits,
    ) -> (BufferFillStateByCount, BufferFillStateBySlots) {
        self.buffer.check_is_filled(buffer_limits)
    }
}

impl From<&ExternalsRangeInfo> for ExternalsPartitionRangeReaderState {
    fn from(value: &ExternalsRangeInfo) -> Self {
        Self {
            buffer: Default::default(),
            skip_offset: value.skip_offset,
            processed_offset: value.processed_offset,
            last_expire_check_on_ct: None,
            tx: Default::default(),
        }
    }
}
