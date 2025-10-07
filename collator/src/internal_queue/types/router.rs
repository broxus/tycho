use std::collections::{BTreeMap, BTreeSet};

use tycho_block_util::queue::{QueuePartitionIdx, RouterAddr, RouterPartitions};
use tycho_types::models::IntAddr;
use tycho_util::FastHashMap;

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct PartitionRouter {
    src: FastHashMap<RouterAddr, QueuePartitionIdx>,
    dst: FastHashMap<RouterAddr, QueuePartitionIdx>,
    partitions_stats: FastHashMap<QueuePartitionIdx, usize>,
}

impl PartitionRouter {
    pub fn with_partitions(src: &RouterPartitions, dst: &RouterPartitions) -> Self {
        let mut partitions_stats = FastHashMap::default();
        let mut convert = |partitions: &RouterPartitions| {
            let mut result =
                FastHashMap::with_capacity_and_hasher(partitions.len(), Default::default());

            for (partition, accounts) in partitions {
                partitions_stats
                    .entry(*partition)
                    .and_modify(|count| *count += accounts.len())
                    .or_insert(accounts.len());
                result.extend(accounts.iter().map(|account| (*account, *partition)));
            }

            result
        };

        Self {
            src: convert(src),
            dst: convert(dst),
            partitions_stats,
        }
    }

    /// Returns the partition for the given source and destination addresses.
    /// If the partition is not found, returns the default partition.
    pub fn get_partition(
        &self,
        src_addr: Option<&IntAddr>,
        dest_addr: &IntAddr,
    ) -> QueuePartitionIdx {
        RouterAddr::from_int_addr(dest_addr)
            .and_then(|dst| self.dst.get(&dst))
            .or_else(|| {
                src_addr
                    .and_then(RouterAddr::from_int_addr)
                    .and_then(|src| self.src.get(&src))
            })
            .copied()
            .unwrap_or_default()
    }

    /// Inserts the address into the inbound router.
    pub fn insert_src(
        &mut self,
        addr: &IntAddr,
        partition: QueuePartitionIdx,
    ) -> anyhow::Result<()> {
        if partition == QueuePartitionIdx::MIN {
            anyhow::bail!("attempt to insert address into default priority partition");
        }
        let Some(addr) = RouterAddr::from_int_addr(addr) else {
            anyhow::bail!("attempt to insert a VarAddr into a priority partition");
        };
        if self.src.insert(addr, partition).is_none() {
            self.partitions_stats
                .entry(partition)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
        Ok(())
    }

    /// Inserts the address into the outbound router.
    pub fn insert_dst(
        &mut self,
        addr: &IntAddr,
        partition: QueuePartitionIdx,
    ) -> anyhow::Result<()> {
        if partition == QueuePartitionIdx::MIN {
            anyhow::bail!("attempt to insert address into default priority partition");
        }
        let Some(addr) = RouterAddr::from_int_addr(addr) else {
            anyhow::bail!("attempt to insert a VarAddr into a priority partition");
        };
        if self.dst.insert(addr, partition).is_none() {
            self.partitions_stats
                .entry(partition)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
        Ok(())
    }

    pub fn partitions_stats(&self) -> &FastHashMap<QueuePartitionIdx, usize> {
        &self.partitions_stats
    }

    pub fn to_router_partitions_src(&self) -> RouterPartitions {
        let mut result = BTreeMap::new();
        for (addr, partition) in &self.src {
            result
                .entry(*partition)
                .or_insert(BTreeSet::new())
                .insert(*addr);
        }
        result
    }

    pub fn to_router_partitions_dst(&self) -> RouterPartitions {
        let mut result = BTreeMap::new();
        for (addr, partition) in &self.dst {
            result
                .entry(*partition)
                .or_insert(BTreeSet::new())
                .insert(*addr);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use tycho_block_util::queue::RouterAddr;
    use tycho_types::cell::HashBytes;
    use tycho_util::FastHashSet;

    use super::*;

    #[test]
    fn test_partition_router_from_btreemap() {
        let addr1 = RouterAddr {
            workchain: 0,
            account: HashBytes([0x01; 32]),
        };
        let addr2 = RouterAddr {
            workchain: 0,
            account: HashBytes([0x02; 32]),
        };
        let addr3 = RouterAddr {
            workchain: 1,
            account: HashBytes([0x03; 32]),
        };
        let addr4 = RouterAddr {
            workchain: 1,
            account: HashBytes([0x04; 32]),
        };

        let mut dest_map = BTreeMap::new();
        dest_map.insert(QueuePartitionIdx(1), BTreeSet::from([addr1, addr2]));
        dest_map.insert(QueuePartitionIdx(2), BTreeSet::from([addr3]));

        let mut src_map = BTreeMap::new();
        src_map.insert(QueuePartitionIdx(10), BTreeSet::from([addr4]));

        let partition_router = PartitionRouter::with_partitions(&src_map, &dest_map);

        {
            let expected_partitions = FastHashSet::from_iter([1, 2, 10].map(QueuePartitionIdx));
            for par_id in partition_router.partitions_stats().keys() {
                assert!(expected_partitions.contains(par_id));
            }
        }

        {
            // Dest
            let dest_router = &partition_router.dst;
            // addr1 и addr2 -> partition 1
            assert_eq!(*dest_router.get(&addr1).unwrap(), QueuePartitionIdx(1));
            assert_eq!(*dest_router.get(&addr2).unwrap(), QueuePartitionIdx(1));
            // addr3 -> partition 2
            assert_eq!(*dest_router.get(&addr3).unwrap(), QueuePartitionIdx(2));

            // Src
            let src_router = &partition_router.src;
            // addr4 -> partition 10
            assert_eq!(*src_router.get(&addr4).unwrap(), QueuePartitionIdx(10));
        }
    }
}
