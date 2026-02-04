use std::collections::BTreeMap;

use anyhow::Context;
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_util::transactional::btreemap::TransactionalBTreeMap;
use tycho_util::transactional::value::TransactionalValue;
use tycho_util_proc::Transactional;

use crate::collator::messages_reader::state::ext::partition_reader::ExternalsPartitionReaderState;
use crate::collator::messages_reader::state::ext::range_reader::ExternalsRangeReaderState;
use crate::types::processed_upto::BlockSeqno;

#[derive(Transactional, Default)]
pub struct ExternalsReaderState {
    /// We fully read each externals range
    /// because we unable to get remaining messages info
    /// in any other way.
    /// We need this for not to get messages for account `A` from range `2`
    /// when we still have messages for account `A` in range `1`.
    ///
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    pub ranges: TransactionalBTreeMap<BlockSeqno, ExternalsRangeReaderState>,

    /// Partition related externals reader state
    pub by_partitions:
        TransactionalValue<BTreeMap<QueuePartitionIdx, ExternalsPartitionReaderState>>,

    /// last read to anchor chain time
    pub last_read_to_anchor_chain_time: TransactionalValue<Option<u64>>,
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
}
