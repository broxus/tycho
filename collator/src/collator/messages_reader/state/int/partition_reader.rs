use std::collections::BTreeMap;

use tycho_util::transactional::btreemap::TransactionalBTreeMap;
use tycho_util::transactional::value::TransactionalValue;
use tycho_util_proc::Transactional;

use crate::collator::messages_reader::state;
use crate::collator::messages_reader::state::int::DebugInternalsRangeReaderState;
use crate::collator::messages_reader::state::int::range_reader::InternalsRangeReaderState;
use crate::types::processed_upto::{BlockSeqno, InternalsProcessedUptoStuff};
use crate::types::{DebugIter, ProcessedTo};

#[derive(Transactional, Default)]
pub struct InternalsPartitionReaderState {
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    pub ranges: TransactionalBTreeMap<BlockSeqno, InternalsRangeReaderState>,

    pub processed_to: TransactionalValue<ProcessedTo>,

    /// Actual current processed offset
    /// during the messages reading.
    /// Is incremented before collect.
    pub curr_processed_offset: TransactionalValue<u32>,
}

impl InternalsPartitionReaderState {
    pub fn with_prev_and_current<F, R>(
        &mut self,
        key: BlockSeqno,
        prev_keys: &[BlockSeqno],
        f: F,
    ) -> anyhow::Result<R>
    where
        F: for<'s> FnOnce(
            Vec<&'s InternalsRangeReaderState>,
            &'s mut InternalsRangeReaderState,
        ) -> anyhow::Result<R>,
    {
        state::with_prev_list_and_current(self.ranges.inner_mut(), key, prev_keys, f)
    }
}

impl From<&InternalsProcessedUptoStuff> for InternalsPartitionReaderState {
    fn from(value: &InternalsProcessedUptoStuff) -> Self {
        let ranges: BTreeMap<u32, InternalsRangeReaderState> = value
            .ranges
            .iter()
            .map(|(k, v)| {
                (
                    *k,
                    InternalsRangeReaderState::from_range_info(v, &value.processed_to),
                )
            })
            .collect();

        Self {
            curr_processed_offset: 0.into(),
            processed_to: value.processed_to.clone().into(),
            ranges: ranges.into(),
        }
    }
}

pub struct DebugInternalsPartitionReaderState<'a>(pub &'a InternalsPartitionReaderState);

impl std::fmt::Debug for DebugInternalsPartitionReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field(
                "ranges",
                &DebugIter(
                    self.0
                        .ranges
                        .iter()
                        .map(|(seqno, state)| (seqno, DebugInternalsRangeReaderState(state))),
                ),
            )
            .field("processed_to", &*self.0.processed_to)
            .field("curr_processed_offset", &*self.0.curr_processed_offset)
            .finish()
    }
}
