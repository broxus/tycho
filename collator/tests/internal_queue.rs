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
use tycho_block_util::queue::{QueueDiff, QueueDiffStuff, QueueKey};
use tycho_collator::internal_queue::queue::{
    Queue, QueueConfig, QueueFactory, QueueFactoryStdImpl, QueueImpl,
};
use tycho_collator::internal_queue::state::persistent_state::{
    PersistentStateImplFactory, PersistentStateStdImpl,
};
use tycho_collator::internal_queue::state::session_state::{
    SessionStateImplFactory, SessionStateStdImpl,
};
use tycho_collator::internal_queue::state::states_iterators_manager::StatesIteratorsManager;
use tycho_collator::internal_queue::types::{InternalMessageValue, QueueDiffWithMessages};
use tycho_collator::test_utils::prepare_test_storage;
use tycho_util::FastHashMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd)]
struct StoredObject {
    key: u64,
    dest: IntAddr,
}

impl Ord for StoredObject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
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
            _ => return Err(anyhow!("Unsupported address type")),
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

async fn create_stored_object(key: u64, dest_str: &str) -> anyhow::Result<Arc<StoredObject>> {
    let dest = IntAddr::Std(StdAddr::from_str(dest_str)?);
    Ok(Arc::new(StoredObject { key, dest }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue() -> anyhow::Result<()> {
    let storage = prepare_test_storage().await?;

    let queue_factory = QueueFactoryStdImpl {
        session_state_factory: SessionStateImplFactory {
            storage: storage.clone(),
        },
        persistent_state_factory: PersistentStateImplFactory { storage },
        config: QueueConfig {
            gc_interval: Duration::from_secs(1),
        },
    };

    let queue: QueueImpl<SessionStateStdImpl, PersistentStateStdImpl, StoredObject> =
        queue_factory.create();
    let block = BlockIdShort {
        shard: ShardIdent::new_full(0),
        seqno: 0,
    };
    let mut diff = QueueDiffWithMessages::new();

    let stored_objects = vec![
        create_stored_object(
            1,
            "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            2,
            "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            3,
            "0:7d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            4,
            "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            5,
            "1:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
    ];

    for stored_object in &stored_objects {
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    queue
        .apply_diff(diff, block, &HashBytes::from([1; 32]))
        .await?;

    let mut top_blocks = vec![];

    top_blocks.push((block, true));

    queue.commit_diff(top_blocks).await?;

    let block2 = BlockIdShort {
        shard: ShardIdent::new_full(1),
        seqno: 1,
    };

    let mut diff = QueueDiffWithMessages::new();

    let stored_objects2 = vec![
        create_stored_object(
            1,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            2,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            3,
            "0:7d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            4,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
        create_stored_object(
            5,
            "0:6d6e566da0b322193d90020ff65b9b9e91582c953ed587ffd281d8344a7d5732",
        )
        .await?,
    ];

    for stored_object in &stored_objects2 {
        diff.messages
            .insert(stored_object.key(), stored_object.clone());
    }

    let mut top_blocks = vec![];

    top_blocks.push((block2, true));

    queue
        .apply_diff(diff, block2, &HashBytes::from([0; 32]))
        .await?;
    queue.commit_diff(top_blocks).await?;

    let mut ranges = FastHashMap::default();
    ranges.insert(
        ShardIdent::new_full(0),
        (
            QueueKey {
                lt: 1,
                hash: HashBytes::default(),
            },
            QueueKey {
                lt: 4,
                hash: HashBytes::default(),
            },
        ),
    );

    let iterators = queue.iterator(&ranges, ShardIdent::new_full(1)).await;

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
        processed_upto: BTreeMap::from([
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

    let diff_with_messages = QueueDiffWithMessages::from_queue_diff(queue_diff_stuff, &out_msg)?;

    assert_eq!(
        diff_with_messages.processed_upto,
        diff.processed_upto
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    );

    assert_eq!(
        diff_with_messages
            .messages
            .into_iter()
            .map(|(key, _)| key.hash)
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
