use crate::collator::messages_reader::state::ext::ExternalKey;

#[derive(Debug, Default, Clone)]
pub struct ExternalsPartitionReaderState {
    /// The last processed external message from all ranges
    pub processed_to: ExternalKey,

    /// Actual current processed offset
    /// during the messages reading.
    /// Is incremented before collect.
    pub curr_processed_offset: u32,
}
