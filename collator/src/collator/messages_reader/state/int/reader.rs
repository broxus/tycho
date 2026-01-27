use tycho_block_util::queue::QueuePartitionIdx;
use tycho_util::FastHashMap;
use tycho_util_proc::Transactional;

use crate::collator::messages_reader::state::int::partition_reader::InternalsPartitionReaderState;
use crate::collator::statistics::cumulative::CumulativeStatistics;
use crate::types::ProcessedTo;

#[derive(Transactional, Default)]
pub struct InternalsReaderState {
    #[tx(collection)]
    partitions: FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState>,

    #[tx(transactional)]
    cumulative_statistics: Option<CumulativeStatistics>,

    #[tx(state)]
    tx: Option<InternalsReaderStateTx>,
}

impl InternalsReaderState {
    pub fn new(
        partitions: FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState>,
        cumulative_statistics: Option<CumulativeStatistics>,
    ) -> Self {
        Self {
            partitions,
            cumulative_statistics,
            tx: None,
        }
    }

    pub fn get_min_processed_to_by_shards(&self) -> ProcessedTo {
        let mut shards_processed_to = ProcessedTo::default();
        for par_s in self.partitions.values() {
            for (shard_id, key) in &par_s.processed_to {
                shards_processed_to
                    .entry(*shard_id)
                    .and_modify(|min_key| *min_key = std::cmp::min(*min_key, *key))
                    .or_insert(*key);
            }
        }
        shards_processed_to
    }

    pub fn partitions(&self) -> &FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState> {
        &self.partitions
    }

    pub fn cumulative_statistics(&self) -> &Option<CumulativeStatistics> {
        &self.cumulative_statistics
    }

    pub fn set_cumulative_statistics(&mut self, stats: Option<CumulativeStatistics>) {
        self.tx_set_cumulative_statistics(stats);
    }

    pub fn tx_cumulative_statistics_mut(&mut self) -> &mut Option<CumulativeStatistics> {
        &mut self.cumulative_statistics
    }

    #[cfg(test)]
    pub fn tx_partitions_mut(
        &mut self,
    ) -> &mut FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState> {
        &mut self.partitions
    }

    pub fn ensure_partition(&mut self, par_id: QueuePartitionIdx) {
        if !self.partitions.contains_key(&par_id) {
            self.tx_insert_partitions(par_id, Default::default());
        }
    }

    pub fn tx_partitions_and_cumulative_stats_mut(
        &mut self,
    ) -> (
        &mut FastHashMap<QueuePartitionIdx, InternalsPartitionReaderState>,
        &mut Option<CumulativeStatistics>,
    ) {
        (&mut self.partitions, &mut self.cumulative_statistics)
    }
}
