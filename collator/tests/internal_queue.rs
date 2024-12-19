use std::collections::BTreeMap;
use std::str::FromStr;
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
use tycho_block_util::queue::{QueueDiff, QueueDiffStuff, QueueKey, QueuePartition};
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
    InternalMessageValue, QueueDiffWithMessages, QueueRange, QueueShardRange,
};
use tycho_collator::test_utils::prepare_test_storage;
use tycho_util::FastHashMap;

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

fn create_stored_object(key: u64, dest_str: &str) -> anyhow::Result<Arc<StoredObject>> {
    let dest = IntAddr::Std(StdAddr::from_str(dest_str)?);
    Ok(Arc::new(StoredObject { key, dest }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue() -> anyhow::Result<()> {
    let (storage, _tmp_dir) = prepare_test_storage().await.unwrap();

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

    let stored_objects = vec![
        create_stored_object(
            1,
            "-1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            2,
            "-1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            3,
            "0:7d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            4,
            "-1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            5,
            "-1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
    ];

    for stored_object in &stored_objects {
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let mut partition_router = FastHashMap::default();

    for stored_object in &stored_objects {
        partition_router.insert(stored_object.dest.clone(), QueuePartition::LowPriority);
    }

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router,
    };

    let statistics = (diff_with_messages.clone(), block.shard).into();

    queue.apply_diff(
        diff_with_messages,
        block,
        &HashBytes::from([1; 32]),
        statistics,
    )?;

    let top_blocks = vec![(block, true)];

    queue.commit_diff(&top_blocks)?;

    let block2 = BlockIdShort {
        shard: ShardIdent::new_full(1),
        seqno: 1,
    };

    let mut diff = QueueDiffWithMessages::new();

    let stored_objects2 = vec![
        create_stored_object(
            1,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            2,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            3,
            "0:7d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            4,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            5,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
    ];

    for stored_object in &stored_objects2 {
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let top_blocks = vec![(block2, true)];

    let mut partition_router = FastHashMap::default();

    for stored_object in &stored_objects2 {
        partition_router.insert(stored_object.dest.clone(), QueuePartition::LowPriority);
    }

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router,
    };

    let statistics = (diff_with_messages.clone(), block.shard).into();

    queue.apply_diff(
        diff_with_messages,
        block2,
        &HashBytes::from([0; 32]),
        statistics,
    )?;
    queue.commit_diff(&top_blocks)?;

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

    let partition = QueuePartition::LowPriority;
    let iterators = queue.iterator(partition, ranges, ShardIdent::new_full(-1))?;

    let mut iterator_manager = StatesIteratorsManager::new(iterators);
    iterator_manager.next().ok();
    let loaded_stored_object = iterator_manager.next();

    let loaded_stored_object = loaded_stored_object.unwrap().unwrap();
    assert_eq!(stored_objects[3], loaded_stored_object.message);

    let current_position = iterator_manager.current_position();
    let mut expected_position = FastHashMap::default();
    expected_position.insert(ShardIdent::new_full(0), QueueKey {
        lt: 4,
        hash: HashBytes::default(),
    });

    assert_eq!(expected_position, current_position);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_clear() -> anyhow::Result<()> {
    let (storage, _tmp_dir) = prepare_test_storage().await.unwrap();

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

    let stored_objects = vec![create_stored_object(
        1,
        "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
    )?];

    for stored_object in &stored_objects {
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router: Default::default(),
    };

    let statistics = (diff_with_messages.clone(), block.shard).into();

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_statistics() -> anyhow::Result<()> {
    let (storage, _tmp_dir) = prepare_test_storage().await.unwrap();

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

    let stored_objects = vec![create_stored_object(
        1,
        "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
    )?];

    for stored_object in &stored_objects {
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let start_key = *diff.messages.iter().next().unwrap().0;
    let end_key = *diff.messages.iter().last().unwrap().0;
    let diff_with_messages = QueueDiffWithMessages {
        messages: diff.messages,
        processed_to: diff.processed_to,
        partition_router: Default::default(),
    };

    let statistics = (diff_with_messages.clone(), block.shard).into();

    queue.apply_diff(
        diff_with_messages,
        block,
        &HashBytes::from([1; 32]),
        statistics,
    )?;

    let range = QueueRange {
        shard_ident: ShardIdent::new_full(0),
        partition: QueuePartition::NormalPriority,
        from: start_key,
        to: end_key,
    };
    let stat = queue.load_statistics(range)?;

    assert_eq!(stat.show().len(), 1);

    Ok(())
}

#[cfg(FALSE)]
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

#[cfg(FALSE)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_tail() -> anyhow::Result<()> {
    let (storage, _tmp_dir) = prepare_test_storage().await.unwrap();

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
        create_stored_object(
            1,
            "-1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            2,
            "-1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            3,
            "0:7d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            4,
            "-1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
        create_stored_object(
            5,
            "-1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )?,
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
    let end_key_mc2 = *diff_mc2.messages.iter().last().unwrap().0;

    // apply two diffs
    queue.apply_diff(diff_mc1, block_mc1, &HashBytes::from([1; 32]), end_key_mc1)?;
    queue.apply_diff(diff_mc2, block_mc2, &HashBytes::from([2; 32]), end_key_mc2)?;

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
