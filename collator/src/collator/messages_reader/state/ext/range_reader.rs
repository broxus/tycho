use std::collections::BTreeMap;

use anyhow::Context;
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_util::transactional::btreemap::deep::TransactionalBTreeMapDeep;
use tycho_util::transactional::value::TransactionalValue;
use tycho_util_proc::Transactional;

use crate::collator::messages_reader::state::ext::ExternalsReaderRange;
use crate::collator::messages_reader::state::ext::partition_range_reader::ExternalsPartitionRangeReaderState;
use crate::collator::state::DisplayRangeReaderStateByPartition;
use crate::types::DebugIter;

#[derive(Transactional)]
pub struct ExternalsRangeReaderState {
    /// Range info
    pub range: TransactionalValue<ExternalsReaderRange>,

    /// Partition related externals range reader state
    pub by_partitions:
        TransactionalBTreeMapDeep<QueuePartitionIdx, ExternalsPartitionRangeReaderState>,

    #[tx(skip)]
    pub fully_read: bool,
}

impl ExternalsRangeReaderState {
    pub fn new(
        range: ExternalsReaderRange,
        by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState>,
    ) -> Self {
        Self {
            range: TransactionalValue::new(range),
            by_partitions: by_partitions.into(),
            fully_read: false,
        }
    }

    pub fn get_state_by_partition_mut(
        &mut self,
        par_id: QueuePartitionIdx,
    ) -> anyhow::Result<&mut ExternalsPartitionRangeReaderState> {
        self.by_partitions.get_mut(&par_id).with_context(|| {
            format!("externals range reader state not exists for partition {par_id}")
        })
    }

    pub fn get_state_by_partition(
        &self,
        par_id: QueuePartitionIdx,
    ) -> anyhow::Result<&ExternalsPartitionRangeReaderState> {
        self.by_partitions.get(&par_id).with_context(|| {
            format!("externals range reader state not exists for partition {par_id}")
        })
    }
}

pub struct DebugExternalsRangeReaderState<'a>(pub &'a ExternalsRangeReaderState);

impl std::fmt::Debug for DebugExternalsRangeReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("range", &*self.0.range)
            .field(
                "by_partitions",
                &DebugIter(
                    self.0
                        .by_partitions
                        .iter()
                        .map(|(par_id, par)| (par_id, DisplayRangeReaderStateByPartition(par))),
                ),
            )
            .finish()
    }
}
