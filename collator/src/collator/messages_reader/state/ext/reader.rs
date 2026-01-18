use std::collections::BTreeMap;

use anyhow::Context;
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_util_proc::Transactional;

use crate::collator::messages_reader::state::ext::partition_reader::ExternalsPartitionReaderState;
use crate::collator::messages_reader::state::ext::range_reader::ExternalsRangeReaderState;
use crate::types::processed_upto::BlockSeqno;

#[derive(Default, Transactional)]
pub struct ExternalsReaderState {
    /// We fully read each externals range
    /// because we unable to get remaining messages info
    /// in any other way.
    /// We need this for not to get messages for account `A` from range `2`
    /// when we still have messages for account `A` in range `1`.
    ///
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    #[tx(collection)]
    pub ranges: BTreeMap<BlockSeqno, ExternalsRangeReaderState>,

    /// Partition related externals reader state
    pub by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionReaderState>,

    /// last read to anchor chain time
    pub last_read_to_anchor_chain_time: Option<u64>,

    #[tx(state)]
    tx: Option<ExternalsReaderStateTx>,
}

impl ExternalsReaderState {
    pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
        &self,
        par_id: T,
    ) -> anyhow::Result<&ExternalsPartitionReaderState> {
        let par_id = par_id.into();
        self.by_partitions
            .get(&par_id)
            .with_context(|| format!("externals reader state not exists for partition {par_id}"))
    }

    pub fn insert_range(
        &mut self,
        seqno: BlockSeqno,
        state: ExternalsRangeReaderState,
    ) -> Option<ExternalsRangeReaderState> {
        self.tx_insert_ranges(seqno, state);
        None
    }

    pub fn ranges(&self) -> &BTreeMap<BlockSeqno, ExternalsRangeReaderState> {
        &self.ranges
    }

    pub fn ranges_mut(&mut self) -> &mut BTreeMap<BlockSeqno, ExternalsRangeReaderState> {
        &mut self.ranges
    }

    pub fn retain_ranges<F>(&mut self, mut f: F)
    where
        F: FnMut(&BlockSeqno, &mut ExternalsRangeReaderState) -> bool,
    {
        if self.tx.is_none() {
            self.ranges.retain(|k, v| f(k, v));
            return;
        }

        let mut to_remove: Vec<BlockSeqno> = Vec::new();

        for (k, v) in self.ranges.iter_mut() {
            if !f(k, v) {
                to_remove.push(*k);
            }
        }

        for k in to_remove {
            assert!(
                self.tx_remove_ranges(&k),
                "range {k} should exist for removal"
            );
        }
    }
}
