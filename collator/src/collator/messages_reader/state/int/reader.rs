use tycho_block_util::queue::QueuePartitionIdx;
use tycho_util::FastHashMap;
use tycho_util::transactional::hashmap::TransactionalHashMap;
use tycho_util::transactional::option::TransactionalOption;
use tycho_util_proc::Transactional;

use crate::collator::messages_reader::state::int::partition_reader::InternalsPartitionReaderState;
use crate::collator::statistics::cumulative::CumulativeStatistics;
use crate::types::ProcessedTo;

#[derive(Transactional, Default)]
pub struct InternalsReaderState {
    pub partitions: TransactionalHashMap<QueuePartitionIdx, InternalsPartitionReaderState>,
    pub cumulative_statistics: TransactionalOption<CumulativeStatistics>,
}

impl InternalsReaderState {
    pub fn new(
        partitions: FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState>,
        cumulative_statistics: Option<CumulativeStatistics>,
    ) -> Self {
        Self {
            partitions: partitions.into(),
            cumulative_statistics: cumulative_statistics.into(),
        }
    }

    pub fn get_min_processed_to_by_shards(&self) -> ProcessedTo {
        let mut shards_processed_to = ProcessedTo::default();
        for par_s in self.partitions.values() {
            for (shard_id, key) in &*par_s.processed_to {
                shards_processed_to
                    .entry(*shard_id)
                    .and_modify(|min_key| *min_key = std::cmp::min(*min_key, *key))
                    .or_insert(*key);
            }
        }
        shards_processed_to
    }

    pub fn ensure_partition(&mut self, par_id: QueuePartitionIdx) {
        if !self.partitions.contains_key(&par_id) {
            self.partitions.insert(par_id, Default::default());
        }
    }
}
