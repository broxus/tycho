use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use everscale_types::cell::{Cell, CellSliceRange, HashBytes};
use everscale_types::models::{
    AccountStatus, BlockId, BlockIdShort, ComputePhase, ComputePhaseSkipReason, CurrencyCollection,
    HashUpdate, IntAddr, IntMsgInfo, IntermediateAddr, Lazy, MsgEnvelope, MsgInfo, OrdinaryTxInfo,
    OutMsg, OutMsgDescr, OutMsgNew, OwnedMessage, ShardIdent, SkippedComputePhase, StdAddr,
    Transaction, TxInfo,
};
use everscale_types::num::Tokens;
use tycho_block_util::queue::{DestAddr, QueueDiff, QueueDiffStuff, QueueKey, QueuePartition};
use tycho_collator::internal_queue::queue::{
    Queue, QueueConfig, QueueFactory, QueueFactoryStdImpl, QueueImpl,
};
use tycho_collator::internal_queue::state::commited_state::{
    CommittedStateImplFactory, CommittedStateStdImpl,
};
use tycho_collator::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use tycho_collator::internal_queue::state::uncommitted_state::{
    UncommittedStateImplFactory, UncommittedStateStdImpl,
};
use tycho_collator::internal_queue::types::{
    DiffStatistics, InternalMessageValue, PartitionRouter, QueueDiffWithMessages, QueueShardRange,
};
use tycho_storage::Storage;

#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredObject {
    key: u64,
    dest: IntAddr,
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
        let key = u64::from_be_bytes(bytes[..8].try_into()?);
        let workchain = i8::from_be_bytes(bytes[8].to_be_bytes());
        let addr = HashBytes::from_slice(&bytes[9..]);
        let dest = IntAddr::Std(StdAddr::new(workchain, addr));
        Ok(StoredObject { key, dest })
    }

    fn serialize(&self) -> anyhow::Result<Vec<u8>>
    where
        Self: Sized,
    {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.key.to_be_bytes());
        match &self.dest {
            IntAddr::Std(addr) => {
                bytes.extend_from_slice(&addr.workchain.to_be_bytes());
                bytes.extend_from_slice(&addr.address.0);
            }
            IntAddr::Var(_) => return Err(anyhow!("Unsupported address type")),
        }
        Ok(bytes)
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
}

fn create_stored_object(key: u64, dest_addr: DestAddr) -> anyhow::Result<Arc<StoredObject>> {
    let dest = dest_addr.to_int_addr();
    Ok(Arc::new(StoredObject { key, dest }))
}

fn test_statistics_check_statistics(
    queue: &QueueImpl<UncommittedStateStdImpl, CommittedStateStdImpl, StoredObject>,
    dest_1_low_priority: DestAddr,
    dest_2_low_priority: DestAddr,
    dest_3_normal_priority: DestAddr,
) -> anyhow::Result<()> {
    // check two diff statistics
    let statistics_low_priority_partition =
        queue.load_statistics(QueuePartition::LowPriority, vec![QueueShardRange {
            shard_ident: ShardIdent::new_full(0),
            from: QueueKey {
                lt: 1,
                hash: HashBytes::default(),
            },
            to: QueueKey {
                lt: 36000,
                hash: HashBytes::default(),
            },
        }])?;

    let addr_1_stat = statistics_low_priority_partition
        .statistics()
        .get(&dest_1_low_priority.to_int_addr())
        .unwrap();
    let addr_2_stat = statistics_low_priority_partition
        .statistics()
        .get(&dest_2_low_priority.to_int_addr())
        .unwrap();

    assert_eq!(*addr_1_stat, 20000);
    assert_eq!(*addr_2_stat, 10000);

    let statistics_normal_priority_partition =
        queue.load_statistics(QueuePartition::NormalPriority, vec![QueueShardRange {
            shard_ident: ShardIdent::new_full(0),
            from: QueueKey {
                lt: 1,
                hash: HashBytes::default(),
            },
            to: QueueKey {
                lt: 36000,
                hash: HashBytes::default(),
            },
        }])?;

    let addr_3_stat = statistics_normal_priority_partition
        .statistics()
        .get(&dest_3_normal_priority.to_int_addr())
        .unwrap();
    assert_eq!(*addr_3_stat, 2000);

    // check first diff
    let statistics_low_priority_partition =
        queue.load_statistics(QueuePartition::LowPriority, vec![QueueShardRange {
            shard_ident: ShardIdent::new_full(0),
            from: QueueKey {
                lt: 1,
                hash: HashBytes::default(),
            },
            to: QueueKey {
                lt: 16000,
                hash: HashBytes::default(),
            },
        }])?;

    let addr_1_stat = statistics_low_priority_partition
        .statistics()
        .get(&dest_1_low_priority.to_int_addr())
        .unwrap();
    let addr_2_stat = statistics_low_priority_partition
        .statistics()
        .get(&dest_2_low_priority.to_int_addr())
        .unwrap();

    assert_eq!(*addr_1_stat, 10000);
    assert_eq!(*addr_2_stat, 5000);

    // check second diff
    let statistics_low_priority_partition =
        queue.load_statistics(QueuePartition::LowPriority, vec![QueueShardRange {
            shard_ident: ShardIdent::new_full(0),
            from: QueueKey {
                lt: 20000,
                hash: HashBytes::default(),
            },
            to: QueueKey {
                lt: 36000,
                hash: HashBytes::default(),
            },
        }])?;

    let addr_1_stat = statistics_low_priority_partition
        .statistics()
        .get(&dest_1_low_priority.to_int_addr())
        .unwrap();
    let addr_2_stat = statistics_low_priority_partition
        .statistics()
        .get(&dest_2_low_priority.to_int_addr())
        .unwrap();

    assert_eq!(*addr_1_stat, 10000);
    assert_eq!(*addr_2_stat, 5000);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue() -> anyhow::Result<()> {
    let (storage, _tmp_dir) = Storage::new_temp().await?;

    let queue_factory = QueueFactoryStdImpl {
        uncommitted_state_factory: UncommittedStateImplFactory {
            storage: storage.clone(),
        },
        committed_state_factory: CommittedStateImplFactory {
            storage: storage.clone(),
        },
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let queue: QueueImpl<UncommittedStateStdImpl, CommittedStateStdImpl, StoredObject> =
        queue_factory.create();

    // create first block with queue diff
    let block1 = BlockIdShort {
        shard: ShardIdent::new_full(0),
        seqno: 0,
    };
    let mut diff = QueueDiffWithMessages::new();

    let mut partition_router = PartitionRouter::default();

    let dest_1_low_priority =
        DestAddr::try_from(IntAddr::Std(StdAddr::new(-1, HashBytes::from([1; 32]))))?;
    let dest_2_low_priority =
        DestAddr::try_from(IntAddr::Std(StdAddr::new(-1, HashBytes::from([2; 32]))))?;

    // low priority
    for i in 1..=10000 {
        let stored_object = create_stored_object(i, dest_1_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert(stored_object.dest.clone(), QueuePartition::LowPriority)?;
    }

    for i in 10001..=15000 {
        let stored_object = create_stored_object(i, dest_2_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert(stored_object.dest.clone(), QueuePartition::LowPriority)?;
    }

    let dest_3_normal_priority =
        DestAddr::try_from(IntAddr::Std(StdAddr::new(0, HashBytes::from([3; 32]))))?;

    // normal priority
    for i in 15001..=16000 {
        let stored_object = create_stored_object(i, dest_3_normal_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    assert_eq!(
        partition_router.get_partition(&dest_1_low_priority.to_int_addr()),
        QueuePartition::LowPriority
    );
    assert_eq!(
        partition_router.get_partition(&dest_2_low_priority.to_int_addr()),
        QueuePartition::LowPriority
    );
    assert_eq!(
        partition_router.get_partition(&dest_3_normal_priority.to_int_addr()),
        QueuePartition::NormalPriority
    );

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router,
    };

    let diff_statistics: DiffStatistics = (&diff_with_messages, block1.shard).into();
    assert_eq!(diff_with_messages.messages.len(), 16000);

    // check low priority statistics
    diff_statistics
        .iter()
        .filter(|(partition, _)| partition == &&QueuePartition::LowPriority)
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
        .filter(|(partition, _)| partition == &&QueuePartition::NormalPriority)
        .for_each(|(_partition, statistics)| {
            assert_eq!(statistics.iter().count(), 1);

            let addr3_count = statistics
                .get(&dest_3_normal_priority.to_int_addr())
                .unwrap();
            assert_eq!(*addr3_count, 1000);
        });

    queue.apply_diff(
        diff_with_messages,
        block1,
        &HashBytes::from([1; 32]),
        diff_statistics,
    )?;
    // end block 1 diff

    // create second block with queue diff
    let block2 = BlockIdShort {
        shard: ShardIdent::new_full(0),
        seqno: 1,
    };
    let mut diff = QueueDiffWithMessages::new();

    let mut partition_router = PartitionRouter::default();

    let dest_1_low_priority =
        DestAddr::try_from(IntAddr::Std(StdAddr::new(-1, HashBytes::from([1; 32]))))?;
    let dest_2_low_priority =
        DestAddr::try_from(IntAddr::Std(StdAddr::new(-1, HashBytes::from([2; 32]))))?;

    // low priority
    for i in 20001..=30000 {
        let stored_object = create_stored_object(i, dest_1_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert(stored_object.dest.clone(), QueuePartition::LowPriority)?;
    }

    for i in 30001..=35000 {
        let stored_object = create_stored_object(i, dest_2_low_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
        partition_router.insert(stored_object.dest.clone(), QueuePartition::LowPriority)?;
    }

    let dest_3_normal_priority =
        DestAddr::try_from(IntAddr::Std(StdAddr::new(0, HashBytes::from([3; 32]))))?;

    // normal priority
    for i in 35001..=36000 {
        let stored_object = create_stored_object(i, dest_3_normal_priority)?;
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    assert_eq!(
        partition_router.get_partition(&dest_1_low_priority.to_int_addr()),
        QueuePartition::LowPriority
    );
    assert_eq!(
        partition_router.get_partition(&dest_2_low_priority.to_int_addr()),
        QueuePartition::LowPriority
    );
    assert_eq!(
        partition_router.get_partition(&dest_3_normal_priority.to_int_addr()),
        QueuePartition::NormalPriority
    );

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router,
    };

    let diff_statistics: DiffStatistics = (&diff_with_messages, block2.shard).into();
    assert_eq!(diff_with_messages.messages.len(), 16000);

    // check low priority statistics
    diff_statistics
        .iter()
        .filter(|(partition, _)| partition == &&QueuePartition::LowPriority)
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
        .filter(|(partition, _)| partition == &&QueuePartition::NormalPriority)
        .for_each(|(_partition, statistics)| {
            assert_eq!(statistics.iter().count(), 1);

            let addr3_count = statistics
                .get(&dest_3_normal_priority.to_int_addr())
                .unwrap();
            assert_eq!(*addr3_count, 1000);
        });

    queue.apply_diff(
        diff_with_messages,
        block2,
        &HashBytes::from([1; 32]),
        diff_statistics,
    )?;

    // end block 2 diff

    test_statistics_check_statistics(
        &queue,
        dest_1_low_priority,
        dest_2_low_priority,
        dest_3_normal_priority,
    )?;

    queue.commit_diff(&vec![(block1, true)])?;
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
            lt: 0,
            hash: HashBytes::default(),
        },
        to: QueueKey {
            lt: 16000,
            hash: HashBytes::default(),
        },
    };

    ranges.push(queue_range);

    let iterators = queue.iterator(
        QueuePartition::LowPriority,
        ranges.clone(),
        ShardIdent::new_full(-1),
    )?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while let Some(_) = iterator_manager.next()? {
        read_count += 1;
    }
    assert_eq!(read_count, 15000);

    let iterators = queue.iterator(
        QueuePartition::NormalPriority,
        ranges,
        ShardIdent::new_full(0),
    )?;
    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while let Some(_) = iterator_manager.next()? {
        read_count += 1;
    }

    assert_eq!(read_count, 1000);

    // check two diff iterator
    let mut ranges = Vec::new();

    let queue_range = QueueShardRange {
        shard_ident: ShardIdent::new_full(0),
        from: QueueKey {
            lt: 0,
            hash: HashBytes::default(),
        },
        to: QueueKey {
            lt: 36000,
            hash: HashBytes::default(),
        },
    };

    ranges.push(queue_range);

    let iterators = queue.iterator(
        QueuePartition::LowPriority,
        ranges.clone(),
        ShardIdent::new_full(-1),
    )?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while let Some(_) = iterator_manager.next()? {
        read_count += 1;
    }
    assert_eq!(read_count, 30000);

    let iterators = queue.iterator(
        QueuePartition::NormalPriority,
        ranges,
        ShardIdent::new_full(0),
    )?;
    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    let mut read_count = 0;
    while let Some(_) = iterator_manager.next()? {
        read_count += 1;
    }

    assert_eq!(read_count, 2000);

    // test commit all diffs and check statistics
    queue.commit_diff(&vec![(block2, true)])?;
    test_statistics_check_statistics(
        &queue,
        dest_1_low_priority,
        dest_2_low_priority,
        dest_3_normal_priority,
    )?;
    queue.clear_uncommitted_state()?;
    test_statistics_check_statistics(
        &queue,
        dest_1_low_priority,
        dest_2_low_priority,
        dest_3_normal_priority,
    )?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_clear() -> anyhow::Result<()> {
    let (storage, _tmp_dir) = Storage::new_temp().await?;

    let queue_factory = QueueFactoryStdImpl {
        uncommitted_state_factory: UncommittedStateImplFactory {
            storage: storage.clone(),
        },
        committed_state_factory: CommittedStateImplFactory { storage },
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let queue: QueueImpl<UncommittedStateStdImpl, CommittedStateStdImpl, StoredObject> =
        queue_factory.create();
    let block = BlockIdShort {
        shard: ShardIdent::new_full(0),
        seqno: 0,
    };
    let mut diff = QueueDiffWithMessages::new();

    let stored_objects = vec![create_stored_object(1, DestAddr {
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

    let statistics = (&diff_with_messages, block.shard).into();

    queue.apply_diff(
        diff_with_messages,
        block,
        &HashBytes::from([1; 32]),
        statistics,
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
        },
    };

    ranges.push(queue_range);

    let partition = QueuePartition::NormalPriority;
    let iterators = queue.iterator(partition, ranges.clone(), ShardIdent::new_full(1))?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    assert!(iterator_manager.next().ok().is_some());

    queue.clear_uncommitted_state()?;

    let iterators = queue.iterator(partition, ranges.clone(), ShardIdent::new_full(1))?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    assert!(iterator_manager.next()?.is_none());

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

    let message_body = Cell::default();

    let message1 = Lazy::new(&OwnedMessage {
        info: MsgInfo::Int(IntMsgInfo {
            created_lt: 0x01,
            ..Default::default()
        }),
        init: None,
        body: (message_body.clone(), CellSliceRange::default()),
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
        body: (message_body.clone(), CellSliceRange::default()),
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
        body: (message_body.clone(), CellSliceRange::default()),
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

    let mut partition_router = BTreeMap::default();

    partition_router.insert(
        QueuePartition::LowPriority,
        BTreeSet::from_iter(vec![
            DestAddr::try_from(IntAddr::Std(StdAddr::new(0, HashBytes::ZERO)))?,
            DestAddr::try_from(IntAddr::Std(StdAddr::new(1, HashBytes::ZERO)))?,
            DestAddr::try_from(IntAddr::Std(StdAddr::new(2, HashBytes::ZERO)))?,
        ]),
    );

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
        partition_router,
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
        PartitionRouter::from(diff.partition_router),
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
async fn test_queue_tail() -> anyhow::Result<()> {
    let (storage, _tmp_dir) = Storage::new_temp().await?;

    let queue_factory = QueueFactoryStdImpl {
        uncommitted_state_factory: UncommittedStateImplFactory {
            storage: storage.clone(),
        },
        committed_state_factory: CommittedStateImplFactory { storage },
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let queue: QueueImpl<UncommittedStateStdImpl, CommittedStateStdImpl, StoredObject> =
        queue_factory.create();
    let block_mc1 = BlockIdShort {
        shard: ShardIdent::new_full(-1),
        seqno: 0,
    };

    let block_mc2 = BlockIdShort {
        shard: ShardIdent::new_full(-1),
        seqno: 1,
    };
    let mut diff_mc1 = QueueDiffWithMessages::new();
    let mut diff_mc2 = QueueDiffWithMessages::new();

    let stored_objects = vec![
        create_stored_object(1, DestAddr {
            workchain: -1,
            account: HashBytes::from([1; 32]),
        })?,
        create_stored_object(2, DestAddr {
            workchain: -1,
            account: HashBytes::from([2; 32]),
        })?,
        create_stored_object(3, DestAddr {
            workchain: 0,
            account: HashBytes::from([3; 32]),
        })?,
        create_stored_object(4, DestAddr {
            workchain: -1,
            account: HashBytes::from([4; 32]),
        })?,
    ];

    if let Some(stored_object) = stored_objects.first() {
        diff_mc1
            .messages
            .insert(stored_object.key(), stored_object.clone());
    }

    for stored_object in &stored_objects {
        diff_mc2
            .messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let end_key_mc1 = *diff_mc1.messages.iter().last().unwrap().0;

    let statistics_mc1 = (&diff_mc1, block_mc1.shard).into();
    let statistics_mc2 = (&diff_mc2, block_mc2.shard).into();

    // apply two diffs
    queue.apply_diff(
        diff_mc1,
        block_mc1,
        &HashBytes::from([1; 32]),
        statistics_mc1,
    )?;
    queue.apply_diff(
        diff_mc2,
        block_mc2,
        &HashBytes::from([2; 32]),
        statistics_mc2,
    )?;

    let diff_len_mc = queue.get_diffs_count_by_shard(&ShardIdent::new_full(-1));

    assert_eq!(diff_len_mc, 2);

    // commit first diff
    queue.commit_diff(&[(block_mc1, true)])?;
    let diff_len_mc = queue.get_diffs_count_by_shard(&ShardIdent::new_full(-1));

    assert_eq!(diff_len_mc, 2);

    // trim first diff
    queue.trim_diffs(&ShardIdent::new_full(-1), &end_key_mc1)?;
    let diff_len_mc = queue.get_diffs_count_by_shard(&ShardIdent::new_full(-1));
    assert_eq!(diff_len_mc, 1);

    // clear uncommitted state with second diff
    queue.clear_uncommitted_state()?;
    let diff_len_mc = queue.get_diffs_count_by_shard(&ShardIdent::new_full(-1));
    assert_eq!(diff_len_mc, 0);

    Ok(())
}
