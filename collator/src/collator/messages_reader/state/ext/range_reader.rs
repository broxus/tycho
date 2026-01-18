use std::collections::BTreeMap;

use anyhow::Context;
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_util_proc::Transactional;

use crate::collator::messages_reader::state::ext::ExternalsReaderRange;
use crate::collator::messages_reader::state::ext::partition_range_reader::ExternalsPartitionRangeReaderState;
use crate::collator::state::DisplayRangeReaderStateByPartition;
use crate::types::DebugIter;

#[derive(Transactional)]
pub struct ExternalsRangeReaderState {
    /// Range info
    pub range: ExternalsReaderRange,

    /// Partition related externals range reader state
    #[tx(collection)]
    by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState>,

    #[tx(skip)]
    pub fully_read: bool,

    #[tx(state)]
    tx: Option<ExternalsRangeReaderStateTx>,
}

impl ExternalsRangeReaderState {
    pub fn new(
        range: ExternalsReaderRange,
        by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState>,
    ) -> Self {
        Self {
            range,
            by_partitions,
            fully_read: false,
            tx: None,
        }
    }

    pub fn get_state_by_partition_mut<T: Into<QueuePartitionIdx>>(
        &mut self,
        par_id: T,
    ) -> anyhow::Result<&mut ExternalsPartitionRangeReaderState> {
        let par_id = par_id.into();
        self.by_partitions.get_mut(&par_id).with_context(|| {
            format!("externals range reader state not exists for partition {par_id}")
        })
    }

    pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
        &self,
        par_id: T,
    ) -> anyhow::Result<&ExternalsPartitionRangeReaderState> {
        let par_id = par_id.into();
        self.by_partitions.get(&par_id).with_context(|| {
            format!("externals range reader state not exists for partition {par_id}")
        })
    }

    pub fn partitions(&self) -> &BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState> {
        &self.by_partitions
    }

    pub fn partitions_mut(
        &mut self,
    ) -> &mut BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState> {
        &mut self.by_partitions
    }

    pub fn insert_partition(
        &mut self,
        par_id: QueuePartitionIdx,
        state: ExternalsPartitionRangeReaderState,
    ) {
        self.tx_insert_by_partitions(par_id, state);
    }

    pub fn remove_partition(&mut self, par_id: QueuePartitionIdx) {
        self.tx_remove_by_partitions(&par_id);
    }
}

pub struct DebugExternalsRangeReaderState<'a>(pub &'a ExternalsRangeReaderState);

impl std::fmt::Debug for DebugExternalsRangeReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("range", &self.0.range)
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
