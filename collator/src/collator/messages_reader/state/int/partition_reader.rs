use std::collections::BTreeMap;

use tycho_util::transactional::btreemap::deep::TransactionalBTreeMapDeep;
use tycho_util::transactional::value::TransactionalValue;
use tycho_util_proc::Transactional;

use crate::collator::messages_reader::state;
use crate::collator::messages_reader::state::int::range_reader::InternalsRangeReaderState;
use crate::types::ProcessedTo;
use crate::types::processed_upto::{BlockSeqno, InternalsProcessedUptoStuff};

#[derive(Transactional, Default)]
pub struct InternalsPartitionReaderState {
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    pub ranges: TransactionalBTreeMapDeep<BlockSeqno, InternalsRangeReaderState>,

    pub processed_to: TransactionalValue<ProcessedTo>,

    /// Actual current processed offset
    /// during the messages reading.
    /// Is incremented before collect.
    pub curr_processed_offset: TransactionalValue<u32>,
}

impl InternalsPartitionReaderState {
    pub fn retain_ranges<F>(&mut self, mut f: F)
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
            self.ranges.remove(&k);
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
