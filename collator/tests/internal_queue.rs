use std::str::FromStr;
use std::sync::Arc;

use everscale_types::cell::{CellBuilder, CellSlice, HashBytes};
use everscale_types::models::{
    BaseMessage, BlockIdShort, IntAddr, IntMsgInfo, MsgInfo, ShardIdent, StdAddr,
};
use tycho_collator::internal_queue::queue::{
    QueueConfig, QueueFactory, QueueFactoryStdImpl, QueueImpl,
};
use tycho_collator::internal_queue::state::persistent::persistent_state::{
    PersistentStateConfig, PersistentStateImplFactory, PersistentStateStdImpl,
};
use tycho_collator::internal_queue::state::session::session_state::{
    SessionStateImplFactory, SessionStateStdImpl,
};
use tycho_collator::internal_queue::types::EnqueuedMessage;
use tycho_collator::queue_adapter::{MessageQueueAdapter, MessageQueueAdapterStdImpl};
use tycho_collator::utils::shard::SplitMergeAction;
use tycho_storage::Storage;
use tycho_util::FastHashMap;

fn init_queue() -> QueueImpl<SessionStateStdImpl, PersistentStateStdImpl> {
    let (storage, _) = Storage::new_temp().unwrap();

    let config = QueueConfig {
        persistent_state_config: PersistentStateConfig { storage },
    };

    let session_state_factory = SessionStateImplFactory::new(vec![]);
    let persistent_state_factory =
        PersistentStateImplFactory::new(config.persistent_state_config.storage);

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
async fn intershard_message_delivery_test() -> anyhow::Result<()> {
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

    let mut from_ranges = FastHashMap::default();
    let mut to_ranges = FastHashMap::default();

    from_ranges.insert(shard_id_1, 0);
    from_ranges.insert(shard_id_2, 0);

    to_ranges.insert(shard_id_1, u64::MAX);
    to_ranges.insert(shard_id_2, u64::MAX);

    let mut iterator = adapter
        .create_iterator(shard_id_1, from_ranges.clone(), to_ranges.clone())
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

    let msg = BaseMessage {
        info: message.clone(),
        init: None,
        body: CellSlice::default(),
        layout: None,
    };
    let cell = CellBuilder::build_from(msg)?;

    let messages = vec![(message, cell.clone())];
    adapter
        .add_messages_to_iterator(&mut iterator, messages)
        .unwrap();

    let enqueued_message = EnqueuedMessage::from((int_message.clone(), cell));

    let processed_messages = vec![(shard_id_1, enqueued_message.key())];

    adapter
        .commit_messages_to_iterator(&mut iterator, processed_messages)
        .unwrap();

    let block_id = BlockIdShort::default();
    let diff = Arc::new(iterator.take_diff());

    // assert_eq!(diff.id, block_id);
    assert_eq!(diff.messages.len(), 1);

    adapter.apply_diff(diff, block_id).await.unwrap();

    let mut iterator = adapter
        .create_iterator(shard_id_2, from_ranges.clone(), to_ranges.clone())
        .await
        .unwrap();

    let peek_message = iterator.peek(true)?;

    assert!(peek_message.is_some());

    let next_message = iterator.next(true)?;

    assert!(next_message.is_some());

    let peek_message = iterator.peek(true)?;

    assert!(peek_message.is_none());

    let mut iterator = adapter
        .create_iterator(shard_id_1, from_ranges.clone(), to_ranges.clone())
        .await
        .unwrap();

    let peek_message = iterator.peek(true)?;

    assert!(peek_message.is_none());

    adapter.commit_diff(&block_id).await?;

    // peek message from persistent state
    // let mut iterator = adapter
    //     .create_iterator(shard_id_2, from_ranges.clone(), to_ranges.clone())
    //     .await
    //     .unwrap();
    //
    // let peek_message = iterator.peek(false)?;
    // assert!(peek_message.is_some());
    //
    // let next_message = iterator.next(false)?;
    //
    // assert!(next_message.is_some());
    // // println!("{:?}", peek_message.unwrap().message_with_source.message.hash);
    // let next_message = next_message.unwrap();
    // println!("{:?}", next_message.message_with_source.message.info.dst);
    // assert_eq!(
    //     next_message.message_with_source.message.info.dst,
    //     int_message.dst
    // );
    // assert_eq!(next_message.message_with_source.shard_id, shard_id_1);
    //
    // let next_message = iterator.next(false)?;
    // assert!(next_message.is_none());

    Ok(())
}
