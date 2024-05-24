use std::str::FromStr;
use std::sync::Arc;

use everscale_types::cell::{Cell, HashBytes};
use everscale_types::models::{BlockIdShort, IntAddr, IntMsgInfo, MsgInfo, ShardIdent, StdAddr};
use tycho_collator::internal_queue::persistent::persistent_state::{
    PersistentStateConfig, PersistentStateImplFactory, PersistentStateStdImpl,
};
use tycho_collator::internal_queue::queue::{
    QueueConfig, QueueFactory, QueueFactoryStdImpl, QueueImpl,
};
use tycho_collator::internal_queue::session::session_state::{
    SessionStateImplFactory, SessionStateStdImpl,
};
use tycho_collator::internal_queue::snapshot::IterRange;
use tycho_collator::internal_queue::types::EnqueuedMessage;
use tycho_collator::queue_adapter::{MessageQueueAdapter, MessageQueueAdapterStdImpl};
use tycho_collator::utils::shard::SplitMergeAction;

fn init_queue() -> QueueImpl<SessionStateStdImpl, PersistentStateStdImpl> {
    let config = QueueConfig {
        persistent_state_config: PersistentStateConfig {
            database_url: "db_url".to_string(),
        },
    };

    let session_state_factory = SessionStateImplFactory::new(vec![]);
    let persistent_state_factory = PersistentStateImplFactory::new(config.persistent_state_config);

    let queue_factory = QueueFactoryStdImpl {
        session_state_factory,
        persistent_state_factory,
    };

    let queue = queue_factory.create();
    queue
}

#[tokio::test]
// 1. init iterator
// 2. add messages to iterator
// 3. create diff
// 4. apply diff
// 5. create snapshot
// 6. read message from another shard (intershard message delivery)
async fn intershard_message_delivery_test() {
    let queue = init_queue();

    let shard_id_1 = ShardIdent::new_full(-1);
    let shard_id_2 = ShardIdent::new_full(0);

    let adapter = MessageQueueAdapterStdImpl::new(queue);

    let action_1 = SplitMergeAction::Add(shard_id_1);
    let action_2 = SplitMergeAction::Add(shard_id_2);

    adapter
        .update_shards(vec![action_1, action_2])
        .await
        .unwrap();

    let iter_range_from = vec![
        IterRange {
            shard_id: shard_id_1,
            lt: 0,
        },
        IterRange {
            shard_id: shard_id_2,
            lt: 0,
        },
    ];

    let iter_range_to = vec![
        IterRange {
            shard_id: shard_id_1,
            lt: u64::MAX,
        },
        IterRange {
            shard_id: shard_id_2,
            lt: u64::MAX,
        },
    ];

    let mut iterator = adapter
        .create_iterator(shard_id_1, iter_range_from.clone(), iter_range_to.clone())
        .await
        .unwrap();

    let mut int_message = IntMsgInfo::default();
    int_message.created_lt = 100;
    int_message.dst = IntAddr::Std(StdAddr::new(
        0,
        HashBytes::from_str("67a092821012f7a197ab3a221c36086e2fde95df021cb9839641dbcd0bed9fa8")
            .unwrap(),
    ));
    let message = MsgInfo::Int(int_message.clone());
    let cell: Cell = Default::default();
    let messages = vec![(message, cell.clone())];
    adapter
        .add_messages_to_iterator(&mut iterator, messages)
        .unwrap();

    let enqueued_message = EnqueuedMessage::from((int_message, cell));

    let processed_messages = vec![(shard_id_1, enqueued_message.key())];

    adapter
        .commit_messages_to_iterator(&mut iterator, processed_messages)
        .unwrap();

    let block_id = BlockIdShort::default();
    let diff = Arc::new(iterator.take_diff(block_id));

    assert_eq!(diff.id, block_id);
    assert_eq!(diff.messages.len(), 1);

    adapter.apply_diff(diff).await.unwrap();

    let mut iterator = adapter
        .create_iterator(shard_id_2, iter_range_from.clone(), iter_range_to.clone())
        .await
        .unwrap();

    let peek_message = iterator.peek();

    assert!(peek_message.is_some());

    let mut iterator = adapter
        .create_iterator(shard_id_1, iter_range_from.clone(), iter_range_to.clone())
        .await
        .unwrap();

    let peek_message = iterator.peek();

    assert!(peek_message.is_none());
}
