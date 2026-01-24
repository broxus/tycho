use std::collections::BTreeMap;

use tycho_util_proc::Transactional;

use crate::collator::messages_reader::state;
use crate::collator::messages_reader::state::int::range_reader::InternalsRangeReaderState;
use crate::types::ProcessedTo;
use crate::types::processed_upto::{BlockSeqno, InternalsProcessedUptoStuff};

#[derive(Transactional, Default)]
pub struct InternalsPartitionReaderState {
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    #[tx(collection)]
    ranges: BTreeMap<BlockSeqno, InternalsRangeReaderState>,

    pub processed_to: ProcessedTo,

    /// Actual current processed offset
    /// during the messages reading.
    /// Is incremented before collect.
    pub curr_processed_offset: u32,

    #[tx(state)]
    tx: Option<InternalsPartitionReaderStateTx>,
}

impl InternalsPartitionReaderState {
    pub fn ranges(&self) -> &BTreeMap<BlockSeqno, InternalsRangeReaderState> {
        &self.ranges
    }

    pub fn get_range_mut(&mut self, seqno: &BlockSeqno) -> Option<&mut InternalsRangeReaderState> {
        self.ranges.get_mut(seqno)
    }

    pub fn tx_insert_range(&mut self, seqno: BlockSeqno, state: InternalsRangeReaderState) {
        self.tx_insert_ranges(seqno, state);
    }

    pub fn tx_remove_range(&mut self, seqno: &BlockSeqno) -> bool {
        self.tx_remove_ranges(seqno)
    }

    pub fn tx_retain_ranges<F>(&mut self, mut f: F)
    where
        F: FnMut(&BlockSeqno, &mut InternalsRangeReaderState) -> bool,
    {
        let mut to_remove: Vec<BlockSeqno> = Vec::new();

        for (k, v) in self.ranges.iter_mut() {
            if !f(k, v) {
                to_remove.push(*k);
            }
        }

        for k in to_remove {
            self.tx_remove_ranges(&k);
        }
    }

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
        state::with_prev_list_and_current(&mut self.ranges, key, prev_keys, f)
    }
}

impl From<&InternalsProcessedUptoStuff> for InternalsPartitionReaderState {
    fn from(value: &InternalsProcessedUptoStuff) -> Self {
        Self {
            curr_processed_offset: 0,
            processed_to: value.processed_to.clone(),
            ranges: value
                .ranges
                .iter()
                .map(|(k, v)| {
                    (
                        *k,
                        InternalsRangeReaderState::from_range_info(v, &value.processed_to),
                    )
                })
                .collect(),
            tx: None,
        }
    }
}
