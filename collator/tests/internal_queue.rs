use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use everscale_types::cell::{Cell, HashBytes, Lazy};
use everscale_types::models::{
    AccountStatus, BlockId, BlockIdShort, ComputePhase, ComputePhaseSkipReason, CurrencyCollection,
    HashUpdate, IntAddr, IntMsgInfo, IntermediateAddr, MsgEnvelope, MsgInfo, OrdinaryTxInfo,
    OutMsg, OutMsgDescr, OutMsgNew, OutMsgQueueUpdates, OwnedMessage, ShardIdent,
    SkippedComputePhase, StdAddr, Transaction, TxInfo,
};
use everscale_types::num::Tokens;
use tycho_block_util::queue::{QueueDiff, QueueDiffStuff, QueueKey, QueuePartitionIdx, RouterAddr};
use tycho_collator::internal_queue::queue::{
    Queue, QueueConfig, QueueFactory, QueueFactoryStdImpl, QueueImpl,
};
use tycho_collator::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use tycho_collator::internal_queue::state::storage::{QueueStateImplFactory, QueueStateStdImpl};
use tycho_collator::internal_queue::types::{
    DiffStatistics, DiffZone, EnqueuedMessage, InternalMessageValue, PartitionRouter,
    QueueDiffWithMessages, QueueShardRange,
};
use tycho_collator::storage::snapshot::{AccountStatistics, InternalQueueSnapshot};
use tycho_collator::storage::InternalQueueStorage;
use tycho_storage::StorageContext;
use tycho_util::FastHashSet;

#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredObject {
    key: u64,
    dest: IntAddr,
    src: IntAddr,
}

impl Ord for StoredObject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}
impl PartialOrd for StoredObject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl InternalMessageValue for StoredObject {
    fn deserialize(bytes: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let key = u64::from_le_bytes(bytes[..8].try_into()?);
        let workchain = bytes[8] as i8;
        let addr = HashBytes::from_slice(&bytes[9..]);
        let dest = IntAddr::Std(StdAddr::new(workchain, addr));
        Ok(StoredObject {
            key,
            dest,
            src: Default::default(),
        })
    }

    fn serialize(&self, buffer: &mut Vec<u8>)
    where
        Self: Sized,
    {
        buffer.extend_from_slice(&self.key.to_le_bytes());
        match &self.dest {
            IntAddr::Std(addr) => {
                buffer.push(addr.workchain as u8);
                buffer.extend_from_slice(&addr.address.0);
            }
            IntAddr::Var(_) => unreachable!(),
        }
    }

    fn source(&self) -> &IntAddr {
        &self.src
    }

    fn destination(&self) -> &IntAddr {
        &self.dest
    }

    fn key(&self) -> QueueKey {
        QueueKey {
            lt: self.key,
            hash: HashBytes::default(),
        }
    }

    fn info(&self) -> &IntMsgInfo {
        unreachable!("method is not used in internal queue tests")
    }

    fn cell(&self) -> &Cell {
        unreachable!("method is not used in internal queue tests")
    }
}

fn create_stored_object(key: u64, dest_addr: RouterAddr) -> anyhow::Result<Arc<StoredObject>> {
    let dest = dest_addr.to_int_addr();
    Ok(Arc::new(StoredObject {
        key,
        dest,
        src: Default::default(),
    }))
}

fn test_statistics_check_statistics(
    queue: &QueueImpl<QueueStateStdImpl, StoredObject>,
    dest_1_low_priority: RouterAddr,
    dest_2_low_priority: RouterAddr,
    dest_3_normal_priority: RouterAddr,
) -> anyhow::Result<()> {
    // check two diff statistics
    // there are 30000 messages in low partition, 2000 message in normal partition

    let mut statistics_low_priority_partition = AccountStatistics::default();
    queue.load_diff_statistics(
        QueuePartitionIdx(1),
        &QueueShardRange {
            shard_ident: ShardIdent::new_full(0),
            from: QueueKey {
                lt: 1,
                hash: HashBytes::default(),
            },
            to: QueueKey {
                lt: 36000,
                hash: HashBytes::default(),
            }
            .next_value(),
        },
        &mut statistics_low_priority_partition,
    )?;

    let addr_1_stat = statistics_low_priority_partition
        .get(&dest_1_low_priority.to_int_addr())
        .unwrap();

    let addr_2_stat = statistics_low_priority_partition
        .get(&dest_2_low_priority.to_int_addr())
        .unwrap();

    assert_eq!(*addr_1_stat, 20000);
    assert_eq!(*addr_2_stat, 10000);

    let mut statistics_normal_priority_partition = AccountStatistics::default();

    queue.load_diff_statistics(
        QueuePartitionIdx(0),
        &QueueShardRange {
            shard_ident: ShardIdent::new_full(0),
            from: QueueKey {
                lt: 1,
                hash: HashBytes::default(),
            },
            to: QueueKey {
                lt: 36000,
                hash: HashBytes::default(),
            }
            .next_value(),
        },
        &mut statistics_normal_priority_partition,
    )?;

    let addr_3_stat = statistics_normal_priority_partition
        .get(&dest_3_normal_priority.to_int_addr())
        .unwrap();
    assert_eq!(*addr_3_stat, 2000);

    let mut statistics_low_priority_partition = AccountStatistics::default();
    // check first diff, there are 15000 messages in low partition
    queue.load_diff_statistics(
        QueuePartitionIdx(1),
        &QueueShardRange {
            shard_ident: ShardIdent::new_full(0),
            from: QueueKey {
                lt: 1,
                hash: HashBytes::default(),
            },
            to: QueueKey {
                lt: 16000,
                hash: HashBytes::default(),
            }
            .next_value(),
        },
        &mut statistics_low_priority_partition,
    )?;

    let addr_1_stat = statistics_low_priority_partition
        .get(&dest_1_low_priority.to_int_addr())
        .unwrap();
    let addr_2_stat = statistics_low_priority_partition
        .get(&dest_2_low_priority.to_int_addr())
        .unwrap();

    assert_eq!(*addr_1_stat, 10000);
    assert_eq!(*addr_2_stat, 5000);

    // check second diff, there are 15000 messages in low partition
    let mut statistics_low_priority_partition = AccountStatistics::default();

    queue.load_diff_statistics(
        QueuePartitionIdx(1),
        &QueueShardRange {
            shard_ident: ShardIdent::new_full(0),
            from: QueueKey {
                lt: 20000,
                hash: HashBytes::default(),
            },
            to: QueueKey {
                lt: 36000,
                hash: HashBytes::default(),
            }
            .next_value(),
        },
        &mut statistics_low_priority_partition,
    )?;

    let addr_1_stat = statistics_low_priority_partition
        .get(&dest_1_low_priority.to_int_addr())
        .unwrap();
    let addr_2_stat = statistics_low_priority_partition
        .get(&dest_2_low_priority.to_int_addr())
        .unwrap();

    assert_eq!(*addr_1_stat, 10000);
    assert_eq!(*addr_2_stat, 5000);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue() -> anyhow::Result<()> {
    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;

    let queue_factory = QueueFactoryStdImpl {
        state: QueueStateImplFactory::new(ctx)?,
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let queue: QueueImpl<QueueStateStdImpl, StoredObject> = queue_factory.create()?;

    let mc_block = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 10,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };

    // create first block with queue diff
    let block1 = BlockId {
        shard: ShardIdent::new_full(0),
        seqno: 0,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };

    let mc_block2 = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 11,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };

    let mut diff = QueueDiffWithMessages::new();

    let mut partition_router = PartitionRouter::default();

    let dest_1_low_priority = RouterAddr::from(StdAddr::new(-1, HashBytes::from([1; 32])));
    let dest_2_low_priority = RouterAddr::from(StdAddr::new(-1, HashBytes::from([2; 32])));

    // low priority
    for i in 1..=10000 {
        let stored_object = create_stored_object(i, dest_1_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert_dst(&stored_object.dest, QueuePartitionIdx(1))?;
    }

    for i in 10001..=15000 {
        let stored_object = create_stored_object(i, dest_2_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert_dst(&stored_object.dest, QueuePartitionIdx(1))?;
    }

    let dest_3_normal_priority = RouterAddr::from(StdAddr::new(0, HashBytes::from([3; 32])));

    // normal priority
    for i in 15001..=16000 {
        let stored_object = create_stored_object(i, dest_3_normal_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    assert_eq!(
        partition_router.get_partition(
            Some(&Default::default()),
            &dest_1_low_priority.to_int_addr()
        ),
        QueuePartitionIdx(1)
    );
    assert_eq!(
        partition_router.get_partition(
            Some(&Default::default()),
            &dest_2_low_priority.to_int_addr()
        ),
        QueuePartitionIdx(1)
    );
    assert_eq!(
        partition_router.get_partition(
            Some(&Default::default()),
            &dest_3_normal_priority.to_int_addr()
        ),
        QueuePartitionIdx::default()
    );

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router,
    };

    let diff_statistics = DiffStatistics::from_diff(
        &diff_with_messages,
        block1.shard,
        diff_with_messages
            .min_message()
            .cloned()
            .unwrap_or_default(),
        diff_with_messages
            .max_message()
            .cloned()
            .unwrap_or_default(),
    );
    assert_eq!(diff_with_messages.messages.len(), 16000);

    // check low priority statistics
    diff_statistics
        .iter()
        .filter(|(partition, _)| partition == &&1)
        .for_each(|(_partition, statistics)| {
            assert_eq!(statistics.iter().count(), 2);

            let addr1_count = statistics.get(&dest_1_low_priority.to_int_addr()).unwrap();
            assert_eq!(*addr1_count, 10000);

            let addr2_count = statistics.get(&dest_2_low_priority.to_int_addr()).unwrap();
            assert_eq!(*addr2_count, 5000);
        });

    // check normal priority statistics
    diff_statistics
        .iter()
        .filter(|(partition, _)| partition == &&QueuePartitionIdx::default())
        .for_each(|(_partition, statistics)| {
            assert_eq!(statistics.iter().count(), 1);

            let addr3_count = statistics
                .get(&dest_3_normal_priority.to_int_addr())
                .unwrap();
            assert_eq!(*addr3_count, 1000);
        });

    queue.apply_diff(
        diff_with_messages,
        block1.as_short_id(),
        &HashBytes::from([1; 32]),
        diff_statistics,
        Some(DiffZone::Both),
    )?;
    // end block 1 diff

    // create second block with queue diff
    let block2 = BlockId {
        shard: ShardIdent::new_full(0),
        seqno: 1,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };

    let block4 = BlockId {
        shard: ShardIdent::new_full(0),
        seqno: 4,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };
    let mut diff = QueueDiffWithMessages::new();

    let mut partition_router = PartitionRouter::default();

    let dest_1_low_priority = RouterAddr::from(StdAddr::new(-1, HashBytes::from([1; 32])));
    let dest_2_low_priority = RouterAddr::from(StdAddr::new(-1, HashBytes::from([2; 32])));

    // low priority
    for i in 20001..=30000 {
        let stored_object = create_stored_object(i, dest_1_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert_dst(&stored_object.dest, QueuePartitionIdx(1))?;
    }

    for i in 30001..=35000 {
        let stored_object = create_stored_object(i, dest_2_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert_dst(&stored_object.dest, QueuePartitionIdx(1))?;
    }

    let dest_3_normal_priority = RouterAddr::from(StdAddr::new(0, HashBytes::from([3; 32])));

    // normal priority
    for i in 35001..=36000 {
        let stored_object = create_stored_object(i, dest_3_normal_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    assert_eq!(
        partition_router.get_partition(
            Some(&IntAddr::default()),
            &dest_1_low_priority.to_int_addr()
        ),
        QueuePartitionIdx(1)
    );
    assert_eq!(
        partition_router.get_partition(
            Some(&IntAddr::default()),
            &dest_2_low_priority.to_int_addr()
        ),
        QueuePartitionIdx(1)
    );
    assert_eq!(
        partition_router.get_partition(
            Some(&IntAddr::default()),
            &dest_3_normal_priority.to_int_addr()
        ),
        QueuePartitionIdx::default()
    );

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router,
    };

    let diff_statistics = DiffStatistics::from_diff(
        &diff_with_messages,
        block2.shard,
        diff_with_messages
            .min_message()
            .cloned()
            .unwrap_or_default(),
        diff_with_messages
            .max_message()
            .cloned()
            .unwrap_or_default(),
    );
    assert_eq!(diff_with_messages.messages.len(), 16000);

    // check low priority statistics
    diff_statistics
        .iter()
        .filter(|(partition, _)| partition == &&1)
        .for_each(|(_partition, statistics)| {
            assert_eq!(statistics.iter().count(), 2);

            let addr1_count = statistics.get(&dest_1_low_priority.to_int_addr()).unwrap();
            assert_eq!(*addr1_count, 10000);

            let addr2_count = statistics.get(&dest_2_low_priority.to_int_addr()).unwrap();
            assert_eq!(*addr2_count, 5000);
        });

    // check normal priority statistics
    diff_statistics
        .iter()
        .filter(|(partition, _)| partition == &&QueuePartitionIdx::default())
        .for_each(|(_partition, statistics)| {
            assert_eq!(statistics.iter().count(), 1);

            let addr3_count = statistics
                .get(&dest_3_normal_priority.to_int_addr())
                .unwrap();
            assert_eq!(*addr3_count, 1000);
        });

    let mut partitions = FastHashSet::default();

    partitions.insert(QueuePartitionIdx(1));
    partitions.insert(QueuePartitionIdx(0));

    queue.apply_diff(
        diff_with_messages.clone(),
        block2.as_short_id(),
        &HashBytes::from([1; 32]),
        diff_statistics.clone(),
        Some(DiffZone::Both),
    )?;

    // should return error because sequence number is not correct
    let res = queue.apply_diff(
        diff_with_messages.clone(),
        block4.as_short_id(),
        &HashBytes::from([1; 32]),
        diff_statistics.clone(),
        Some(DiffZone::Both),
    );

    assert!(res.is_err());
    // should return error because sequence number is not correct
    let res = queue.apply_diff(
        diff_with_messages,
        block4.as_short_id(),
        &HashBytes::from([1; 32]),
        diff_statistics,
        Some(DiffZone::Uncommitted),
    );
    assert!(res.is_err());

    let mc_diff_with_messages = QueueDiffWithMessages {
        messages: BTreeMap::new(),
        processed_to: BTreeMap::new(),
        partition_router: Default::default(),
    };

    let mc_diff_statistics = DiffStatistics::from_diff(
        &mc_diff_with_messages,
        mc_block.shard,
        mc_diff_with_messages
            .min_message()
            .cloned()
            .unwrap_or_default(),
        mc_diff_with_messages
            .max_message()
            .cloned()
            .unwrap_or_default(),
    );

    queue.apply_diff(
        mc_diff_with_messages,
        mc_block.as_short_id(),
        &HashBytes::from([1; 32]),
        mc_diff_statistics,
        Some(DiffZone::Both),
    )?;

    // end block 2 diff

    test_statistics_check_statistics(
        &queue,
        dest_1_low_priority,
        dest_2_low_priority,
        dest_3_normal_priority,
    )?;

    queue.commit_diff(&[(mc_block, true), (block1, true)], &partitions)?;
    test_statistics_check_statistics(
        &queue,
        dest_1_low_priority,
        dest_2_low_priority,
        dest_3_normal_priority,
    )?;

    // test iterator
    // test first diff iterator
    let mut ranges = Vec::new();

    let queue_range = QueueShardRange {
        shard_ident: ShardIdent::new_full(0),
        from: QueueKey {
            lt: 10000,
            hash: HashBytes::default(),
        }
        .next_value(),
        to: QueueKey {
            lt: 15500,
            hash: HashBytes::default(),
        }
        .next_value(),
    };

    ranges.push(queue_range);

    let iterators = queue.iterator(QueuePartitionIdx(1), &ranges, ShardIdent::MASTERCHAIN)?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while iterator_manager.next()?.is_some() {
        read_count += 1;
    }
    assert_eq!(read_count, 5000);

    let iterators = queue.iterator(
        QueuePartitionIdx::default(),
        &ranges,
        ShardIdent::new_full(0),
    )?;
    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while iterator_manager.next()?.is_some() {
        read_count += 1;
    }

    assert_eq!(read_count, 500);

    // check two diff iterator
    let mut ranges = Vec::new();

    let queue_range = QueueShardRange {
        shard_ident: ShardIdent::new_full(0),
        from: QueueKey {
            lt: 0,
            hash: HashBytes::default(),
        }
        .next_value(),
        to: QueueKey {
            lt: 36000,
            hash: HashBytes::default(),
        }
        .next_value(),
    };

    ranges.push(queue_range);

    let iterators = queue.iterator(QueuePartitionIdx(1), &ranges, ShardIdent::MASTERCHAIN)?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while iterator_manager.next()?.is_some() {
        read_count += 1;
    }
    assert_eq!(read_count, 30000);

    let iterators = queue.iterator(
        QueuePartitionIdx::default(),
        &ranges,
        ShardIdent::new_full(0),
    )?;
    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while iterator_manager.next()?.is_some() {
        read_count += 1;
    }

    assert_eq!(read_count, 2000);

    // test commit all diffs and check statistics

    let mc2_diff_with_messages = QueueDiffWithMessages {
        messages: BTreeMap::new(),
        processed_to: BTreeMap::new(),
        partition_router: Default::default(),
    };

    let mc2_diff_statistics = DiffStatistics::from_diff(
        &mc2_diff_with_messages,
        mc_block2.shard,
        mc2_diff_with_messages
            .min_message()
            .cloned()
            .unwrap_or_default(),
        mc2_diff_with_messages
            .max_message()
            .cloned()
            .unwrap_or_default(),
    );

    queue.apply_diff(
        mc2_diff_with_messages,
        mc_block2.as_short_id(),
        &HashBytes::from([2; 32]),
        mc2_diff_statistics,
        Some(DiffZone::Both),
    )?;

    queue.commit_diff(&[(mc_block2, true), (block2, true)], &partitions)?;
    test_statistics_check_statistics(
        &queue,
        dest_1_low_priority,
        dest_2_low_priority,
        dest_3_normal_priority,
    )?;
    queue.clear_uncommitted_state(&FastHashSet::from_iter([0, 1].map(QueuePartitionIdx)), &[])?;
    test_statistics_check_statistics(
        &queue,
        dest_1_low_priority,
        dest_2_low_priority,
        dest_3_normal_priority,
    )?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_iteration_from_two_shards() -> anyhow::Result<()> {
    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;

    let queue_factory = QueueFactoryStdImpl {
        state: QueueStateImplFactory::new(ctx)?,
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let queue: QueueImpl<QueueStateStdImpl, StoredObject> = queue_factory.create()?;

    // create first block with queue diff
    let block1 = BlockIdShort {
        shard: ShardIdent::new_full(0),
        seqno: 0,
    };
    let mut diff = QueueDiffWithMessages::new();

    let mut partition_router = PartitionRouter::default();

    let dest_1_low_priority = RouterAddr::from(StdAddr::new(-1, HashBytes::from([1; 32])));
    let dest_2_low_normal_priority = RouterAddr::from(StdAddr::new(-1, HashBytes::from([2; 32])));

    // low priority
    for i in 1..=10000 {
        let stored_object = create_stored_object(i, dest_1_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert_dst(&stored_object.dest, QueuePartitionIdx(1))?;
    }

    for i in 20001..=30000 {
        let stored_object = create_stored_object(i, dest_1_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert_dst(&stored_object.dest, QueuePartitionIdx(1))?;
    }

    for i in 100000..=150000 {
        let stored_object = create_stored_object(i, dest_2_low_normal_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router,
    };

    let diff_statistics = DiffStatistics::from_diff(
        &diff_with_messages,
        block1.shard,
        diff_with_messages
            .min_message()
            .cloned()
            .unwrap_or_default(),
        diff_with_messages
            .max_message()
            .cloned()
            .unwrap_or_default(),
    );

    queue.apply_diff(
        diff_with_messages,
        block1,
        &HashBytes::from([1; 32]),
        diff_statistics,
        Some(DiffZone::Both),
    )?;
    // end block 1 diff

    // create second block with queue diff
    let block2 = BlockIdShort {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 0,
    };
    let mut diff = QueueDiffWithMessages::new();

    let mut partition_router = PartitionRouter::default();

    let dest_1_low_priority = RouterAddr::from(StdAddr::new(-1, HashBytes::from([1; 32])));
    let dest_2_low_normal_priority = RouterAddr::from(StdAddr::new(-1, HashBytes::from([2; 32])));

    // low priority
    for i in 10001..=20000 {
        let stored_object = create_stored_object(i, dest_1_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert_dst(&stored_object.dest, QueuePartitionIdx(1))?;
    }

    for i in 200000..=250000 {
        let stored_object = create_stored_object(i, dest_2_low_normal_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router,
    };

    let diff_statistics = DiffStatistics::from_diff(
        &diff_with_messages,
        block2.shard,
        diff_with_messages
            .min_message()
            .cloned()
            .unwrap_or_default(),
        diff_with_messages
            .max_message()
            .cloned()
            .unwrap_or_default(),
    );

    queue.apply_diff(
        diff_with_messages,
        block2,
        &HashBytes::from([1; 32]),
        diff_statistics,
        Some(DiffZone::Both),
    )?;
    // end block 2 diff

    // test iterator
    let mut ranges = Vec::new();

    let queue_range1 = QueueShardRange {
        shard_ident: ShardIdent::new_full(0),
        from: QueueKey {
            lt: 0,
            hash: HashBytes::default(),
        }
        .next_value(),
        to: QueueKey {
            lt: 30000,
            hash: HashBytes::default(),
        }
        .next_value(),
    };

    let queue_range2 = QueueShardRange {
        shard_ident: ShardIdent::MASTERCHAIN,
        from: QueueKey {
            lt: 10000,
            hash: HashBytes::default(),
        }
        .next_value(),
        to: QueueKey {
            lt: 20000,
            hash: HashBytes::default(),
        }
        .next_value(),
    };

    ranges.push(queue_range1);
    ranges.push(queue_range2);

    let stat_range1 = QueueShardRange {
        shard_ident: ShardIdent::new_full(0),
        from: QueueKey {
            lt: 1,
            hash: HashBytes::default(),
        }
        .next_value(),
        to: QueueKey {
            lt: 150000,
            hash: HashBytes::default(),
        }
        .next_value(),
    };

    let stat_range2 = QueueShardRange {
        shard_ident: ShardIdent::MASTERCHAIN,
        from: QueueKey {
            lt: 1,
            hash: HashBytes::default(),
        }
        .next_value(),
        to: QueueKey {
            lt: 250000,
            hash: HashBytes::default(),
        }
        .next_value(),
    };
    let r = vec![stat_range1, stat_range2];
    let mut statistics = AccountStatistics::default();

    for partition in &[1, 0] {
        for range in &r {
            queue.load_diff_statistics(QueuePartitionIdx(*partition), range, &mut statistics)?;
        }
    }
    assert_eq!(statistics.len(), 2);

    let stat = statistics
        .get(&dest_1_low_priority.to_int_addr())
        .cloned()
        .unwrap_or_default();
    assert_eq!(stat, 30000);

    let iterators = queue.iterator(QueuePartitionIdx(1), &ranges, ShardIdent::MASTERCHAIN)?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while let Some(message) = iterator_manager.next()? {
        if message.message.key <= 10000 || message.message.key >= 20001 {
            assert_eq!(message.source, block1.shard);
        } else {
            assert_eq!(message.source, block2.shard);
        }
        read_count += 1;
        // check sequence
        assert_eq!(message.message.key, read_count);
    }
    assert_eq!(read_count, 30000);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_clear() -> anyhow::Result<()> {
    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;

    let queue_factory = QueueFactoryStdImpl {
        state: QueueStateImplFactory::new(ctx)?,
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let queue: QueueImpl<QueueStateStdImpl, StoredObject> = queue_factory.create()?;
    let block = BlockIdShort {
        shard: ShardIdent::new_full(0),
        seqno: 0,
    };
    let mut diff = QueueDiffWithMessages::new();

    let stored_objects = vec![create_stored_object(1, RouterAddr {
        workchain: 1,
        account: HashBytes::from([1; 32]),
    })?];

    for stored_object in &stored_objects {
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router: Default::default(),
    };

    let statistics = DiffStatistics::from_diff(
        &diff_with_messages,
        block.shard,
        diff_with_messages
            .min_message()
            .cloned()
            .unwrap_or_default(),
        diff_with_messages
            .max_message()
            .cloned()
            .unwrap_or_default(),
    );

    queue.apply_diff(
        diff_with_messages,
        block,
        &HashBytes::from([1; 32]),
        statistics,
        Some(DiffZone::Both),
    )?;

    let mut ranges = Vec::new();

    let queue_range = QueueShardRange {
        shard_ident: ShardIdent::new_full(0),
        from: QueueKey {
            lt: 1,
            hash: HashBytes::default(),
        },
        to: QueueKey {
            lt: 4,
            hash: HashBytes::default(),
        }
        .next_value(),
    };

    ranges.push(queue_range);

    let partition = QueuePartitionIdx::default();
    let iterators = queue.iterator(partition, &ranges, ShardIdent::new_full(1))?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let msg = iterator_manager.next()?;
    println!("iterator next msg before clean = {msg:?}");
    assert!(msg.is_some());

    queue.clear_uncommitted_state(&FastHashSet::from_iter([0, 1].map(QueuePartitionIdx)), &[
        ShardIdent::MASTERCHAIN,
        ShardIdent::new_full(0),
    ])?;

    let iterators = queue.iterator(partition, &ranges, ShardIdent::new_full(1))?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let msg = iterator_manager.next()?;
    println!("iterator next msg after clean = {msg:?}");
    assert!(msg.is_none());

    Ok(())
}

#[test]
fn test_router() -> anyhow::Result<()> {
    let mut partition_router = PartitionRouter::default();

    let dest_1 = RouterAddr {
        workchain: 0,
        account: HashBytes::from([1; 32]),
    };
    let src_1 = RouterAddr {
        workchain: 0,
        account: HashBytes::from([2; 32]),
    };

    partition_router.insert_dst(&dest_1.to_int_addr(), QueuePartitionIdx(1))?;
    partition_router.insert_src(&src_1.to_int_addr().clone(), QueuePartitionIdx(2))?;

    let res = partition_router.get_partition(Some(&IntAddr::default()), &dest_1.to_int_addr());
    assert_eq!(res, 1);

    let res = partition_router.get_partition(Some(&src_1.to_int_addr()), &IntAddr::default());
    assert_eq!(res, 2);

    let res = partition_router.get_partition(Some(&src_1.to_int_addr()), &dest_1.to_int_addr());
    assert_eq!(res, 1);

    let res = partition_router.get_partition(Some(&IntAddr::default()), &IntAddr::default());
    assert_eq!(res, 0);

    Ok(())
}

#[test]
fn test_queue_diff_with_messages_from_queue_diff_stuff() -> anyhow::Result<()> {
    let mut out_msg = OutMsgDescr::default();

    let transaction = Transaction {
        account: HashBytes::ZERO,
        lt: 0x01,
        prev_trans_hash: HashBytes::ZERO,
        prev_trans_lt: 0x00,
        now: 0,
        out_msg_count: Default::default(),
        orig_status: AccountStatus::Active,
        end_status: AccountStatus::Active,
        in_msg: None,
        out_msgs: Default::default(),
        total_fees: Default::default(),
        state_update: Lazy::new(&HashUpdate {
            old: HashBytes::ZERO,
            new: HashBytes::ZERO,
        })
        .unwrap(),
        info: Lazy::new(&TxInfo::Ordinary(OrdinaryTxInfo {
            credit_first: true,
            storage_phase: None,
            credit_phase: None,
            compute_phase: ComputePhase::Skipped(SkippedComputePhase {
                reason: ComputePhaseSkipReason::NoState,
            }),
            action_phase: None,
            aborted: false,
            bounce_phase: None,
            destroyed: false,
        }))
        .unwrap(),
    };

    let message1 = Lazy::new(&OwnedMessage {
        info: MsgInfo::Int(IntMsgInfo {
            created_lt: 0x01,
            ..Default::default()
        }),
        init: None,
        body: Default::default(),
        layout: None,
    })
    .unwrap();
    let message1_hash = *message1.inner().repr_hash();
    out_msg
        .add(
            message1_hash,
            CurrencyCollection::ZERO,
            OutMsg::New(OutMsgNew {
                out_msg_envelope: create_dump_msg_envelope(message1),
                transaction: Lazy::new(&transaction).unwrap(),
            }),
        )
        .unwrap();
    let message2 = Lazy::new(&OwnedMessage {
        info: MsgInfo::Int(IntMsgInfo {
            created_lt: 0x02,
            ..Default::default()
        }),
        init: None,
        body: Default::default(),
        layout: None,
    })
    .unwrap();
    let message2_hash = *message2.inner().repr_hash();
    out_msg
        .add(
            message2_hash,
            CurrencyCollection::ZERO,
            OutMsg::New(OutMsgNew {
                out_msg_envelope: create_dump_msg_envelope(message2),
                transaction: Lazy::new(&transaction).unwrap(),
            }),
        )
        .unwrap();
    let message3 = Lazy::new(&OwnedMessage {
        info: MsgInfo::Int(IntMsgInfo {
            created_lt: 0x03,
            ..Default::default()
        }),
        init: None,
        body: Default::default(),
        layout: None,
    })
    .unwrap();
    let message3_hash = *message3.inner().repr_hash();
    out_msg
        .add(
            message3_hash,
            CurrencyCollection::ZERO,
            OutMsg::New(OutMsgNew {
                out_msg_envelope: create_dump_msg_envelope(message3),
                transaction: Lazy::new(&transaction).unwrap(),
            }),
        )
        .unwrap();

    let mut messages = vec![message1_hash, message2_hash, message3_hash];
    messages.sort_unstable();

    let addr1 = RouterAddr::from(StdAddr::new(0, HashBytes::ZERO));
    let addr2 = RouterAddr::from(StdAddr::new(0, HashBytes::ZERO));
    let addr3 = RouterAddr::from(StdAddr::new(0, HashBytes::ZERO));

    let diff = QueueDiff {
        hash: HashBytes::ZERO,
        prev_hash: HashBytes::from([0x33; 32]),
        shard_ident: ShardIdent::MASTERCHAIN,
        seqno: 123,
        processed_to: BTreeMap::from([
            (ShardIdent::MASTERCHAIN, QueueKey {
                lt: 1,
                hash: message1_hash,
            }),
            (ShardIdent::BASECHAIN, QueueKey {
                lt: 2,
                hash: message2_hash,
            }),
        ]),
        min_message: QueueKey {
            lt: 1,
            hash: message1_hash,
        },
        max_message: QueueKey {
            lt: 2,
            hash: message2_hash,
        },
        messages,
        router_partitions_src: Default::default(),
        router_partitions_dst: BTreeMap::from([
            (QueuePartitionIdx(1), BTreeSet::from([addr1])),
            (QueuePartitionIdx(2), BTreeSet::from([addr2])),
            (QueuePartitionIdx(3), BTreeSet::from([addr3])),
        ]),
    };

    let data = tl_proto::serialize(&diff);

    let block_id = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 123,
        root_hash: HashBytes::ZERO,
        file_hash: HashBytes::ZERO,
    };

    let queue_diff_stuff = QueueDiffStuff::deserialize(&block_id, &data).unwrap();

    let diff_with_messages = QueueDiffWithMessages::from_queue_diff(&queue_diff_stuff, &out_msg)?;

    assert_eq!(diff_with_messages.processed_to, diff.processed_to,);
    assert_eq!(
        diff_with_messages.partition_router,
        PartitionRouter::with_partitions(&diff.router_partitions_src, &diff.router_partitions_dst),
    );

    assert_eq!(
        diff_with_messages
            .messages
            .into_keys()
            .map(|key| key.hash)
            .collect::<Vec<_>>(),
        vec![message1_hash, message2_hash, message3_hash,]
    );

    Ok(())
}

fn create_dump_msg_envelope(message: Lazy<OwnedMessage>) -> Lazy<MsgEnvelope> {
    Lazy::new(&MsgEnvelope {
        cur_addr: IntermediateAddr::FULL_DEST_SAME_WORKCHAIN,
        next_addr: IntermediateAddr::FULL_DEST_SAME_WORKCHAIN,
        fwd_fee_remaining: Tokens::ONE,
        message,
    })
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_tail_and_diff_info() -> anyhow::Result<()> {
    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;

    let queue_factory = QueueFactoryStdImpl {
        state: QueueStateImplFactory::new(ctx)?,
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let queue: QueueImpl<QueueStateStdImpl, StoredObject> = queue_factory.create()?;

    let block_mc1 = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 1,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };

    let block_mc2 = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 2,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };
    let mut diff_mc1 = QueueDiffWithMessages::new();
    let mut diff_mc2 = QueueDiffWithMessages::new();

    let stored_objects = [
        create_stored_object(1, RouterAddr {
            workchain: -1,
            account: HashBytes::from([1; 32]),
        })?,
        create_stored_object(2, RouterAddr {
            workchain: -1,
            account: HashBytes::from([2; 32]),
        })?,
        create_stored_object(3, RouterAddr {
            workchain: 0,
            account: HashBytes::from([3; 32]),
        })?,
        create_stored_object(4, RouterAddr {
            workchain: -1,
            account: HashBytes::from([4; 32]),
        })?,
    ];

    for stored_object in &stored_objects[..2] {
        diff_mc1
            .messages
            .insert(stored_object.key(), stored_object.clone());
    }

    for stored_object in &stored_objects[2..] {
        diff_mc2
            .messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let end_key_mc1 = *diff_mc1.messages.iter().last().unwrap().0;

    let statistics_mc1 = DiffStatistics::from_diff(
        &diff_mc1,
        block_mc1.shard,
        diff_mc1.min_message().cloned().unwrap_or_default(),
        diff_mc1.max_message().cloned().unwrap_or_default(),
    );
    let statistics_mc2 = DiffStatistics::from_diff(
        &diff_mc2,
        block_mc2.shard,
        diff_mc2.min_message().cloned().unwrap_or_default(),
        diff_mc2.max_message().cloned().unwrap_or_default(),
    );

    let mut partitions = FastHashSet::default();

    partitions.insert(QueuePartitionIdx(1));
    partitions.insert(QueuePartitionIdx(0));
    // apply two diffs
    queue.apply_diff(
        diff_mc1,
        block_mc1.as_short_id(),
        &HashBytes::from([1; 32]),
        statistics_mc1,
        Some(DiffZone::Both),
    )?;

    queue.apply_diff(
        diff_mc2,
        block_mc2.as_short_id(),
        &HashBytes::from([2; 32]),
        statistics_mc2,
        Some(DiffZone::Both),
    )?;

    // -- test case 1
    let diff_len_mc = queue.get_diffs_tail_len(&ShardIdent::MASTERCHAIN, &QueueKey::MIN);
    // length 2 in uncommitted state
    assert_eq!(diff_len_mc, 2);

    // first diff has only one message with lt=1
    let diff_info_mc1 = queue
        .get_diff_info(
            &ShardIdent::MASTERCHAIN,
            block_mc1.seqno,
            DiffZone::Uncommitted,
        )?
        .unwrap();
    assert_eq!(diff_info_mc1.max_message, QueueKey::min_for_lt(2));

    // second diff has three messages with lt=2,3,4
    let diff_info_mc2 = queue
        .get_diff_info(
            &ShardIdent::MASTERCHAIN,
            block_mc2.seqno,
            DiffZone::Uncommitted,
        )?
        .unwrap();
    assert_eq!(diff_info_mc2.max_message, QueueKey::min_for_lt(4));

    // should be none because it is not committed
    let res = queue.get_diff_info(
        &ShardIdent::MASTERCHAIN,
        block_mc1.seqno,
        DiffZone::Committed,
    )?;
    assert!(res.is_none());

    // should be some because it is in uncommitted zone
    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, block_mc1.seqno, DiffZone::Both)?;
    assert!(res.is_some());

    // -- test case 2
    // commit first diff
    queue.commit_diff(&[(block_mc1, true)], &partitions)?;

    let diff_len_mc = queue.get_diffs_tail_len(&ShardIdent::MASTERCHAIN, &QueueKey::MIN);
    // one diff moved to committed state. one diff left in uncommitted state
    // uncommitted: 1; committed: 1
    assert_eq!(diff_len_mc, 2);

    // first diff has only one message with lt=1
    let diff_info_mc1 = queue
        .get_diff_info(
            &ShardIdent::MASTERCHAIN,
            block_mc1.seqno,
            DiffZone::Committed,
        )?
        .unwrap();

    assert_eq!(diff_info_mc1.max_message, QueueKey::min_for_lt(2));

    // should be some because it is committed
    let res = queue.get_diff_info(
        &ShardIdent::MASTERCHAIN,
        block_mc1.seqno,
        DiffZone::Uncommitted,
    )?;
    assert!(res.is_none());

    // should be some because it is in committed zone
    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, block_mc1.seqno, DiffZone::Both)?;
    assert!(res.is_some());

    // second diff has three messages with lt=2,3,4
    let diff_info_mc2 = queue
        .get_diff_info(
            &ShardIdent::MASTERCHAIN,
            block_mc2.seqno,
            DiffZone::Uncommitted,
        )?
        .unwrap();
    assert_eq!(diff_info_mc2.max_message, QueueKey::min_for_lt(4));
    // -- test case 3
    // exclude committed diff by range
    let diff_len_mc = queue.get_diffs_tail_len(&ShardIdent::MASTERCHAIN, &end_key_mc1.next_value());
    // uncommitted: 1; committed: 0 (1)
    assert_eq!(diff_len_mc, 1);

    // -- test case 4
    // clear uncommitted state with second diff
    queue.clear_uncommitted_state(&FastHashSet::from_iter([0, 1].map(QueuePartitionIdx)), &[])?;

    let diff_len_mc = queue.get_diffs_tail_len(&ShardIdent::MASTERCHAIN, &QueueKey::MIN);
    // uncommitted: 0; committed: 1
    assert_eq!(diff_len_mc, 1);

    // first diff has only one message with lt=1
    let diff_info_mc1 = queue
        .get_diff_info(
            &ShardIdent::MASTERCHAIN,
            block_mc1.seqno,
            DiffZone::Committed,
        )?
        .unwrap();
    assert_eq!(diff_info_mc1.max_message, QueueKey::min_for_lt(2));

    // second diff removed because it was located in uncommitted state
    let diff_info_mc2 =
        queue.get_diff_info(&ShardIdent::MASTERCHAIN, block_mc2.seqno, DiffZone::Both)?;
    assert!(diff_info_mc2.is_none());

    // -- test case 5
    // exclude committed diff by range
    let diff_len_mc = queue.get_diffs_tail_len(&ShardIdent::MASTERCHAIN, &end_key_mc1.next_value());
    // uncommitted: 0; committed: 0 (1)
    assert_eq!(diff_len_mc, 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_version() -> anyhow::Result<()> {
    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;

    let queue_factory = QueueFactoryStdImpl {
        state: QueueStateImplFactory::new(ctx)?,
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let mut partitions = FastHashSet::default();

    partitions.insert(QueuePartitionIdx(1));
    partitions.insert(QueuePartitionIdx(0));

    let queue: QueueImpl<QueueStateStdImpl, StoredObject> = queue_factory.create()?;

    let block_mc1 = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 1,
        root_hash: HashBytes::from([11; 32]),
        file_hash: HashBytes::from([12; 32]),
    };

    let block_mc2 = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 2,

        root_hash: HashBytes::from([1; 32]),
        file_hash: HashBytes::from([2; 32]),
    };

    let mut diff_mc1 = QueueDiffWithMessages::new();
    let mut diff_mc2 = QueueDiffWithMessages::new();

    let stored_objects = [create_stored_object(1, RouterAddr {
        workchain: -1,
        account: HashBytes::from([1; 32]),
    })?];

    if let Some(stored_object) = stored_objects.first() {
        diff_mc1
            .messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let stored_objects = [create_stored_object(2, RouterAddr {
        workchain: -1,
        account: HashBytes::from([1; 32]),
    })?];

    for stored_object in &stored_objects {
        diff_mc2
            .messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let statistics_mc1 = DiffStatistics::from_diff(
        &diff_mc1,
        block_mc1.shard,
        diff_mc1.min_message().cloned().unwrap_or_default(),
        diff_mc1.max_message().cloned().unwrap_or_default(),
    );

    let statistics_mc2 = DiffStatistics::from_diff(
        &diff_mc2,
        block_mc2.shard,
        diff_mc2.min_message().cloned().unwrap_or_default(),
        diff_mc2.max_message().cloned().unwrap_or_default(),
    );

    let version = queue.get_last_committed_mc_block_id()?;
    assert_eq!(version, None);

    queue.apply_diff(
        diff_mc1,
        block_mc1.as_short_id(),
        &HashBytes::from([1; 32]),
        statistics_mc1,
        Some(DiffZone::Both),
    )?;

    let version = queue.get_last_committed_mc_block_id()?;
    assert_eq!(version, None);

    queue.commit_diff(&[(block_mc1, true)], &partitions)?;

    let version = queue.get_last_committed_mc_block_id()?;
    assert_eq!(version, Some(block_mc1));

    queue.apply_diff(
        diff_mc2,
        block_mc2.as_short_id(),
        &HashBytes::from([2; 32]),
        statistics_mc2,
        Some(DiffZone::Committed),
    )?;
    queue.commit_diff(&[(block_mc2, true)], &partitions)?;

    let version = queue.get_last_committed_mc_block_id()?;
    assert_eq!(version, Some(block_mc2));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_commit_wrong_sequence() -> anyhow::Result<()> {
    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;

    let queue_factory = QueueFactoryStdImpl {
        state: QueueStateImplFactory::new(ctx)?,
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let queue: QueueImpl<QueueStateStdImpl, StoredObject> = queue_factory.create()?;

    // create first block with queue diff
    let block1 = BlockId {
        shard: ShardIdent::new_full(0),
        seqno: 1,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };

    let mc_block1 = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 3,
        root_hash: HashBytes::from([1; 32]),
        file_hash: HashBytes::from([2; 32]),
    };

    let block2 = BlockId {
        shard: ShardIdent::new_full(0),
        seqno: 2,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };

    let mc_block2 = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 4,
        root_hash: HashBytes::from([3; 32]),
        file_hash: HashBytes::from([4; 32]),
    };

    let blocks = vec![block1, mc_block1, block2, mc_block2];

    let low_priority = RouterAddr::from(StdAddr::new(-1, HashBytes::from([2; 32])));

    // apply four diffs
    for block in blocks {
        let mut diff = QueueDiffWithMessages::new();

        let mut partition_router = PartitionRouter::default();

        for i in (block.seqno - 1) * 100 + 1..=100 * block.seqno {
            let stored_object = create_stored_object(i.into(), low_priority)?;
            diff.messages
                .insert(stored_object.key(), stored_object.clone());
            partition_router.insert_dst(&stored_object.dest, QueuePartitionIdx(1))?;
        }

        let diff_with_messages = QueueDiffWithMessages {
            messages: diff.messages,
            processed_to: diff.processed_to,
            partition_router,
        };

        let diff_statistics = DiffStatistics::from_diff(
            &diff_with_messages,
            block.shard,
            diff_with_messages
                .min_message()
                .cloned()
                .unwrap_or_default(),
            diff_with_messages
                .max_message()
                .cloned()
                .unwrap_or_default(),
        );

        queue.apply_diff(
            diff_with_messages,
            block.as_short_id(),
            &HashBytes::from([block.seqno as u8; 32]),
            diff_statistics,
            Some(DiffZone::Uncommitted),
        )?;
    }

    // test iterator
    let mut ranges = Vec::new();

    let queue_range1 = QueueShardRange {
        shard_ident: ShardIdent::new_full(0),
        from: QueueKey {
            lt: 0,
            hash: HashBytes::default(),
        }
        .next_value(),
        to: QueueKey {
            lt: 200,
            hash: HashBytes::default(),
        }
        .next_value(),
    };

    let queue_range2 = QueueShardRange {
        shard_ident: ShardIdent::MASTERCHAIN,
        from: QueueKey {
            lt: 200,
            hash: HashBytes::default(),
        }
        .next_value(),
        to: QueueKey {
            lt: 400,
            hash: HashBytes::default(),
        }
        .next_value(),
    };

    ranges.push(queue_range1);
    ranges.push(queue_range2);

    let stat_range1 = QueueShardRange {
        shard_ident: ShardIdent::new_full(0),
        from: QueueKey {
            lt: 1,
            hash: HashBytes::default(),
        },
        to: QueueKey {
            lt: 100,
            hash: [2; 32].into(),
        }
        .next_value(),
    };

    let stat_range2 = QueueShardRange {
        shard_ident: ShardIdent::MASTERCHAIN,
        from: QueueKey {
            lt: 201,
            hash: HashBytes::default(),
        },
        to: QueueKey {
            lt: 400,
            hash: [4; 32].into(),
        }
        .next_value(),
    };

    let mut statistics = AccountStatistics::default();

    queue.load_diff_statistics(QueuePartitionIdx(1), &stat_range1, &mut statistics)?;
    queue.load_diff_statistics(QueuePartitionIdx(1), &stat_range2, &mut statistics)?;

    let stat = statistics
        .get(&low_priority.to_int_addr())
        .cloned()
        .unwrap_or_default();

    assert_eq!(stat, 300);

    let iterators = queue.iterator(QueuePartitionIdx(1), &ranges, ShardIdent::MASTERCHAIN)?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while let Some(message) = iterator_manager.next()? {
        if (1..100).contains(&message.message.key) {
            assert_eq!(message.source, block1.shard);
        }

        if (101..200).contains(&message.message.key) {
            assert_eq!(message.source, block2.shard);
        }

        if (201..300).contains(&message.message.key) {
            assert_eq!(message.source, mc_block1.shard);
        }

        if (301..400).contains(&message.message.key) {
            assert_eq!(message.source, mc_block2.shard);
        }

        read_count += 1;
        // check sequence
        assert_eq!(message.message.key, read_count);
    }
    assert_eq!(read_count, 400);

    let res = queue.get_diff_info(&ShardIdent::new_full(0), 1, DiffZone::Committed)?;
    assert!(res.is_none());
    let res = queue.get_diff_info(&ShardIdent::new_full(0), 2, DiffZone::Committed)?;
    assert!(res.is_none());
    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, 3, DiffZone::Committed)?;
    assert!(res.is_none());
    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, 4, DiffZone::Committed)?;
    assert!(res.is_none());

    let res = queue.get_diff_info(&ShardIdent::new_full(0), 1, DiffZone::Uncommitted)?;
    assert!(res.is_some());
    let res = queue.get_diff_info(&ShardIdent::new_full(0), 2, DiffZone::Uncommitted)?;
    assert!(res.is_some());
    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, 3, DiffZone::Uncommitted)?;
    assert!(res.is_some());
    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, 4, DiffZone::Uncommitted)?;
    assert!(res.is_some());

    // commit second mc block
    // first mc block will be committed too

    queue.commit_diff(
        &[(mc_block2, true), (block2, true)],
        &FastHashSet::from_iter([0, 1].map(QueuePartitionIdx)),
    )?;

    let res = queue.get_diff_info(&ShardIdent::new_full(0), 1, DiffZone::Committed)?;
    assert!(res.is_some());

    let res = queue.get_diff_info(&ShardIdent::new_full(0), 2, DiffZone::Committed)?;
    assert!(res.is_some());

    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, 3, DiffZone::Committed)?;
    assert!(res.is_some());

    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, 4, DiffZone::Committed)?;
    assert!(res.is_some());

    let res = queue.get_diff_info(&ShardIdent::new_full(0), 1, DiffZone::Uncommitted)?;
    assert!(res.is_none());
    let res = queue.get_diff_info(&ShardIdent::new_full(0), 2, DiffZone::Uncommitted)?;
    assert!(res.is_none());
    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, 3, DiffZone::Uncommitted)?;
    assert!(res.is_none());
    let res = queue.get_diff_info(&ShardIdent::MASTERCHAIN, 4, DiffZone::Uncommitted)?;
    assert!(res.is_none());

    Ok(())
}

async fn prepare_data_from_prepared_persistent_state(
    file_path: &str,
    block_id_str: &str,
    diff_hash_str: &str,
    tail_len: u32,
    storage: &InternalQueueStorage,
) -> anyhow::Result<(BlockId, OutMsgQueueUpdates)> {
    let diff_hash = HashBytes::from_str(diff_hash_str)?;
    let top_update = OutMsgQueueUpdates {
        diff_hash,
        tail_len,
    };

    let block_id = BlockId::from_str(block_id_str)?;

    let full_path = std::path::Path::new(file_path);
    let db = tycho_storage::fs::Dir::new_readonly(full_path.parent().unwrap());
    let file = db.file(full_path.file_name().unwrap()).read(true).open()?;

    storage
        .import_from_file(&top_update, file, block_id)
        .await?;

    Ok((block_id, top_update))
}

#[tokio::test]
async fn test_import_persistent_state() -> anyhow::Result<()> {
    // init storage
    let (ctx, _tmp_dir) = StorageContext::new_temp().await?;

    // init queue
    let queue_factory = QueueFactoryStdImpl {
        state: QueueStateImplFactory::new(ctx)?,
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };
    let storage = &queue_factory.state.storage;

    let queue: QueueImpl<QueueStateStdImpl, EnqueuedMessage> = queue_factory.create()?;

    // import shard persistent state
    let (shard_block_id, shard_top_update) = prepare_data_from_prepared_persistent_state(
        "../test/data/internals/persistent_state/shard.bin",
        // block_id_str
        "0:8000000000000000:160:f54b2c09be8c873670374271571ea70aeacbfbdbef9ac45252451cb035f11a33:0097ee79d8551854a353dd80991c17dafdaaa933abfcb012f93437f3a25c0365",
        // diff_hash_str
        "794fe9ce31b7919d2b6ca9ae0f97501f4017382a273b8f3ee512dcfcb5b14482",
        // tail_len
        3,
        storage,
    ).await?;

    // should be none because it mc block state is not imported
    let last_committed_block = queue.get_last_committed_mc_block_id()?;
    assert!(last_committed_block.is_none());

    // import mc persistent state
    let (mc_block_id, mc_top_update) = prepare_data_from_prepared_persistent_state(
        "../test/data/internals/persistent_state/master.bin",
        // block_id_str
        "-1:8000000000000000:90:9e6e3bdbefda3a64a9dab50387f64a8c30b33a04b6fd638a8058e1344bf8a9d1:60ac6a64e15fb70a1c5629f0bb38890f3fd45aa7ba77e4b847cbf0f48d5d5818",
        // diff_hash_str
        "5795ca5d1b8d0c1ce96394eef851bde718ff2e14fd2840098ed2bb7640f278c5",
        // tail_len
        1,
        storage,
    ).await?;

    let snapshot = storage.make_snapshot();

    // check shard queue
    check_imported_queue(&queue, &shard_block_id, &shard_top_update, &snapshot)?;
    // check mc queue
    check_imported_queue(&queue, &mc_block_id, &mc_top_update, &snapshot)?;

    // common checks
    let last_committed_pointer = snapshot.read_commit_pointers()?;
    assert_eq!(last_committed_pointer.len(), 2);

    let last_committed_block = queue.get_last_committed_mc_block_id()?.unwrap();
    assert_eq!(last_committed_block, mc_block_id);

    Ok(())
}

fn check_imported_queue(
    queue: &QueueImpl<QueueStateStdImpl, EnqueuedMessage>,
    block_id: &BlockId,
    top_update: &OutMsgQueueUpdates,
    snapshot: &InternalQueueSnapshot,
) -> anyhow::Result<()> {
    let diff_tail = queue.get_diffs_tail_len(&block_id.shard, &QueueKey::MIN);
    assert_eq!(diff_tail, top_update.tail_len);

    let last_applied_block_seqno = snapshot
        .get_last_applied_diff_seqno(&block_id.shard)?
        .unwrap();
    assert_eq!(last_applied_block_seqno, block_id.seqno);

    let separated_stats = snapshot.collect_separated_stats_in_range_for_partitions(
        &block_id.shard,
        &FastHashSet::from_iter([0, 1].map(QueuePartitionIdx)),
        &QueueKey::MIN,
        &QueueKey::MAX,
    )?;

    let mut total_stats = AccountStatistics::default();
    for stat in separated_stats.values() {
        for acc_stat in stat.values() {
            for (key, value) in acc_stat {
                total_stats
                    .entry(key.clone())
                    .and_modify(|v| *v += value)
                    .or_insert(*value);
            }
        }
    }

    let mut stat = AccountStatistics::default();
    snapshot.collect_stats_in_range(
        &block_id.shard,
        QueuePartitionIdx(0),
        &QueueKey::MIN,
        &QueueKey::MAX,
        &mut stat,
    )?;

    assert_eq!(stat, total_stats);

    let mut total_messages_by_stat = 0;
    for s in stat.values() {
        total_messages_by_stat += s;
    }

    // if tail_len > 1, then we should have at least one message when tail len == 1 its not guaranteed that diff has messages
    if top_update.tail_len > 1 {
        assert!(total_messages_by_stat > 0);
    }

    let iter_range = vec![QueueShardRange {
        shard_ident: block_id.shard,
        from: QueueKey::MIN.next_value(),
        to: QueueKey::MAX.next_value(),
    }];

    let mut read_messages = 0;

    let mut iterator =
        queue.iterator(QueuePartitionIdx(0), &iter_range, ShardIdent::MASTERCHAIN)?;
    while iterator.next()?.is_some() {
        read_messages += 1;
    }

    let mut iterator = queue.iterator(QueuePartitionIdx(0), &iter_range, ShardIdent::BASECHAIN)?;

    while iterator.next()?.is_some() {
        read_messages += 1;
    }

    assert_eq!(read_messages, total_messages_by_stat);

    let last_committed_pointer = snapshot.read_commit_pointers()?;
    let pointer = last_committed_pointer.get(&block_id.shard).unwrap();
    assert_eq!(pointer.seqno, block_id.seqno);

    Ok(())
}
