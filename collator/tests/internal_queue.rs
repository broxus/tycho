// use std::str::FromStr;
// use std::sync::Arc;
//
// use anyhow::anyhow;
// use everscale_types::cell::{Cell, CellBuilder, CellSlice, HashBytes};
// use everscale_types::models::{
//     BaseMessage, BlockIdShort, IntAddr, IntMsgInfo, MsgInfo, ShardIdent, StdAddr,
// };
// use tycho_collator::internal_queue::queue::{
//     QueueConfig, QueueFactory, QueueFactoryStdImpl, QueueImpl,
// };
// use tycho_collator::internal_queue::state::persistent::persistent_state::{
//     PersistentStateConfig, PersistentStateImplFactory, PersistentStateStdImpl,
// };
// use tycho_collator::internal_queue::state::session::session_state::{
//     SessionStateImplFactory, SessionStateStdImpl,
// };
// use tycho_collator::internal_queue::types::{EnqueuedMessage, InternalMessageKey};
// use tycho_collator::queue_adapter::{MessageQueueAdapter, MessageQueueAdapterStdImpl};
// use tycho_collator::utils::shard::SplitMergeAction;
// use tycho_storage::Storage;
// use tycho_util::FastHashMap;
//
// fn init_queue() -> QueueImpl<SessionStateStdImpl, PersistentStateStdImpl> {
//     let (storage, _) = Storage::new_temp().unwrap();
//
//     let config = QueueConfig {
//         persistent_state_config: PersistentStateConfig { storage },
//     };
//
//     let session_state_factory = SessionStateImplFactory::new(vec![]);
//     let persistent_state_factory =
//         PersistentStateImplFactory::new(config.persistent_state_config.storage);
//
//     let queue_factory = QueueFactoryStdImpl {
//         session_state_factory,
//         persistent_state_factory,
//     };
//
//     let queue = queue_factory.create();
//     queue
// }
//
// #[tokio::test]
// // 1. init iterator
// // 2. add messages to iterator
// // 3. create diff
// // 4. apply diff
// // 5. create snapshot
// // 6. read message from another shard (intershard message delivery)
// async fn intershard_message_delivery_test() -> anyhow::Result<()> {
//     let queue = init_queue();
//
//     let shard_id_1 = ShardIdent::new_full(-1);
//     let shard_id_2 = ShardIdent::new_full(0);
//
//     let adapter = MessageQueueAdapterStdImpl::new(queue);
//
//     let action_1 = SplitMergeAction::Add(shard_id_1);
//     let action_2 = SplitMergeAction::Add(shard_id_2);
//
//     adapter
//         .update_shards(vec![action_1, action_2])
//         .await
//         .unwrap();
//
//     let mut from_ranges = FastHashMap::default();
//     let mut to_ranges = FastHashMap::default();
//
//     from_ranges.insert(shard_id_1, InternalMessageKey::default());
//     from_ranges.insert(shard_id_2, InternalMessageKey::default());
//
//     to_ranges.insert(shard_id_1, InternalMessageKey::MAX);
//     to_ranges.insert(shard_id_2, InternalMessageKey::MAX);
//
//     let mut iterator = adapter
//         .create_iterator(shard_id_1, from_ranges.clone(), to_ranges.clone())
//         .await
//         .unwrap();
//
//     let mut int_message = IntMsgInfo::default();
//     int_message.created_lt = 100;
//     int_message.dst = IntAddr::Std(StdAddr::new(
//         0,
//         HashBytes::from_str("67a092821012f7a197ab3a221c36086e2fde95df021cb9839641dbcd0bed9fa8")
//             .unwrap(),
//     ));
//     let int_message = int_message;
//     let message = MsgInfo::Int(int_message.clone());
//
//     let msg = BaseMessage {
//         info: message.clone(),
//         init: None,
//         body: CellSlice::default(),
//         layout: None,
//     };
//     let cell = CellBuilder::build_from(msg)?;
//
//     let messages = vec![(message, cell.clone())];
//     adapter
//         .add_message_to_iterator(&mut iterator, (int_message.clone(), cell.clone()))
//         .unwrap();
//
//     let enqueued_message = EnqueuedMessage::from((int_message.clone(), cell));
//
//     let processed_messages = vec![(shard_id_1, enqueued_message.key())];
//
//     adapter
//         .commit_messages_to_iterator(&mut iterator, processed_messages)
//         .unwrap();
//
//     let block_id = BlockIdShort::default();
//     let diff = Arc::new(iterator.take_diff());
//
//     // assert_eq!(diff.id, block_id);
//     assert_eq!(diff.messages.len(), 1);
//
//     adapter.apply_diff(diff, block_id).await.unwrap();
//
//     let mut iterator = adapter
//         .create_iterator(shard_id_2, from_ranges.clone(), to_ranges.clone())
//         .await
//         .unwrap();
//
//     let peek_message = iterator.peek(true)?;
//     assert!(peek_message.is_some());
//
//     let next_message = iterator.next(true)?;
//     assert!(next_message.is_some());
//
//     let peek_message = iterator.peek(true)?;
//     assert!(peek_message.is_none());
//
//     let mut iterator = adapter
//         .create_iterator(shard_id_1, from_ranges.clone(), to_ranges.clone())
//         .await
//         .unwrap();
//
//     let peek_message = iterator.peek(true)?;
//
//     assert!(peek_message.is_none());
//
//     adapter.commit_diff(&block_id).await?;
//
//     // peek message from persistent state
//     let mut iterator = adapter
//         .create_iterator(shard_id_2, from_ranges.clone(), to_ranges.clone())
//         .await
//         .unwrap();
//
//     let peek_message = iterator.peek(false)?;
//     assert!(peek_message.is_some());
//
//     let next_message = iterator.next(false)?;
//     assert!(next_message.is_some());
//
//     let next_message = next_message.unwrap();
//     println!("{:?}", next_message.message_with_source.message.info.dst);
//
//     assert_eq!(
//         next_message.message_with_source.message.info.dst,
//         int_message.dst
//     );
//     assert_eq!(next_message.message_with_source.shard_id, shard_id_1);
//
//     let next_message = iterator.next(false)?;
//     assert!(next_message.is_none());
//
//     Ok(())
// }
//
// fn create_message(lt: u64, hash: &str) -> (Cell, IntMsgInfo) {
//     let mut int_message = IntMsgInfo::default();
//     int_message.created_lt = lt;
//     int_message.dst = IntAddr::Std(StdAddr::new(0, HashBytes::from_str(hash).unwrap()));
//     let int_message = int_message;
//     let message = MsgInfo::Int(int_message.clone());
//
//     let base_message = BaseMessage {
//         info: message.clone(),
//         init: None,
//         body: CellSlice::default(),
//         layout: None,
//     };
//
//     let cell = CellBuilder::build_from(base_message).unwrap();
//
//     (cell, int_message)
// }
//
// #[tokio::test]
// async fn persistent_test() -> anyhow::Result<()> {
//     let queue = init_queue();
//
//     let shard_id_1 = ShardIdent::new_full(-1);
//     let shard_id_2 = ShardIdent::new_full(0);
//
//     let adapter = MessageQueueAdapterStdImpl::new(queue);
//
//     let action_1 = SplitMergeAction::Add(shard_id_1);
//     let action_2 = SplitMergeAction::Add(shard_id_2);
//
//     let message_1 = create_message(
//         100,
//         "67a092821012f7a197ab3a221c36086e2fde95df021cb9839641dbcd0bed9fa8",
//     );
//     let message_2 = create_message(
//         101,
//         "67a092821012f7a197ab3a221c36086e2fde95df021cb9839641dbcd0bed9fa9",
//     );
//     let message_3 = create_message(
//         102,
//         "67a092821012f7a197ab3a221c36086e2fde95df021cb9839641dbcd0bed9f11",
//     );
//     let message_4 = create_message(
//         103,
//         "67a092821012f7a197ab3a221c36086e2fde95df021cb9839641dbcd0bed9f12",
//     );
//
//     adapter.update_shards(vec![action_1, action_2]).await?;
//
//     let mut from_ranges = FastHashMap::default();
//     let mut to_ranges = FastHashMap::default();
//
//     from_ranges.insert(shard_id_1, InternalMessageKey::default());
//     from_ranges.insert(shard_id_2, InternalMessageKey::default());
//
//     to_ranges.insert(shard_id_1, InternalMessageKey::MAX);
//     to_ranges.insert(shard_id_2, InternalMessageKey::MAX);
//
//     let mut iterator = adapter
//         .create_iterator(shard_id_1, from_ranges.clone(), to_ranges.clone())
//         .await
//         .unwrap();
//
//     // let messages = vec![(message_1.0, message_1.1)];
//     adapter
//         .add_message_to_iterator(&mut iterator, (message_1.1.clone(), message_1.0.clone()))
//         .unwrap();
//
//     adapter
//         .add_message_to_iterator(&mut iterator, (message_3.1.clone(), message_3.0.clone()))
//         .unwrap();
//
//     adapter
//         .add_message_to_iterator(&mut iterator, (message_2.1.clone(), message_2.0.clone()))
//         .unwrap();
//
//     let enqueued_message1 = EnqueuedMessage::from((message_1.1.clone(), message_1.0.clone()));
//     let enqueued_message3 = EnqueuedMessage::from((message_3.1.clone(), message_3.0.clone()));
//     let enqueued_message2 = EnqueuedMessage::from((message_2.1.clone(), message_2.0.clone()));
//
//     let processed_messages1 = vec![(shard_id_1, enqueued_message1.key())];
//     let processed_messages3 = vec![(shard_id_1, enqueued_message3.key())];
//     let processed_messages2 = vec![(shard_id_1, enqueued_message2.key())];
//
//     adapter
//         .commit_messages_to_iterator(&mut iterator, processed_messages1)
//         .unwrap();
//
//     let block_id = BlockIdShort::default();
//     let diff = Arc::new(iterator.take_diff());
//
//     // assert_eq!(diff.id, block_id);
//     assert_eq!(diff.messages.len(), 3);
//
//     assert_eq!(
//         diff.processed_upto
//             .get(&ShardIdent::new_full(-1))
//             .unwrap()
//             .lt,
//         100
//     );
//
//     adapter.apply_diff(diff, block_id).await.unwrap();
//     adapter.commit_diff(&block_id).await?;
//
//     let mut iterator = adapter
//         .create_iterator(shard_id_2, from_ranges.clone(), to_ranges.clone())
//         .await
//         .unwrap();
//
//     let message_1_from_iter = iterator.next(false)?.unwrap();
//     assert_eq!(
//         message_1_from_iter
//             .message_with_source
//             .message
//             .info
//             .created_lt,
//         message_2.1.created_lt
//     );
//
//     let message_2_from_iter = iterator.next(false)?.unwrap();
//     assert_eq!(
//         message_2_from_iter
//             .message_with_source
//             .message
//             .info
//             .created_lt,
//         message_3.1.created_lt
//     );
//
//     let message_3_from_iter = iterator.next(false)?;
//     assert!(message_3_from_iter.is_none());
//
//     // assert_eq!(message_1_from_iter.message_with_source.message.info.created_lt, message_1.1.created_lt);
//
//     Ok(())
// }
