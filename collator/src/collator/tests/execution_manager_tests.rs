use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_types::cell::{CellBuilder, HashBytes};
use everscale_types::dict::Dict;
use everscale_types::models::{
    BlockId, BlockIdShort, BlockchainConfig, CurrencyCollection, ExternalsProcessedUpto,
    ShardDescription, ShardHashes, ShardIdent, ShardStateUnsplit, ValidatorInfo,
};
use tycho_block_util::queue::QueueKey;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_util::FastHashMap;

use super::ExecutionManager;
use crate::collator::do_collate::tests::{build_stub_collation_data, fill_test_anchors_cache};
use crate::collator::execution_manager::GetNextMessageGroupMode;
use crate::collator::mq_iterator_adapter::QueueIteratorAdapter;
use crate::collator::types::{AnchorsCache, MessagesBuffer, PrevData, WorkingState};
use crate::internal_queue::iterator::{IterItem, QueueIterator};
use crate::internal_queue::types::{
    EnqueuedMessage, InternalMessageValue, QueueDiffWithMessages, QueueFullDiff,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::test_utils::try_init_test_tracing;
use crate::types::{InternalsProcessedUptoStuff, McData, ProcessedUptoInfoStuff};

#[derive(Default)]
struct QueueIteratorTestImpl<V: InternalMessageValue> {
    _phantom_data: PhantomData<V>,
}

#[allow(clippy::unimplemented)]
impl<V: InternalMessageValue> QueueIterator<V> for QueueIteratorTestImpl<V> {
    fn next(&mut self, _with_new: bool) -> Result<Option<IterItem<V>>> {
        Ok(None)
    }

    fn next_new(&mut self) -> Result<Option<IterItem<V>>> {
        unimplemented!()
    }

    fn process_new_messages(&mut self) -> Result<Option<IterItem<V>>> {
        unimplemented!()
    }

    fn has_new_messages_for_current_shard(&self) -> bool {
        unimplemented!()
    }

    fn current_position(&self) -> FastHashMap<ShardIdent, QueueKey> {
        unimplemented!()
    }

    fn add_message(&mut self, _message: V) -> Result<()> {
        unimplemented!()
    }

    fn set_new_messages_from_full_diff(&mut self, _full_diff: QueueFullDiff<V>) {
        unimplemented!()
    }

    fn extract_full_diff(&mut self) -> QueueFullDiff<V> {
        unimplemented!()
    }

    fn take_diff(&self) -> QueueDiffWithMessages<V> {
        unimplemented!()
    }

    fn commit(&mut self, _messages: Vec<(ShardIdent, QueueKey)>) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct MessageQueueAdapterTestImpl<V: InternalMessageValue> {
    _phantom_data: PhantomData<V>,
}

#[async_trait]
#[allow(clippy::unimplemented)]
impl<V: InternalMessageValue + Default> MessageQueueAdapter<V> for MessageQueueAdapterTestImpl<V> {
    async fn create_iterator(
        &self,
        _for_shard_id: ShardIdent,
        _shards_from: FastHashMap<ShardIdent, QueueKey>,
        _shards_to: FastHashMap<ShardIdent, QueueKey>,
    ) -> Result<Box<dyn QueueIterator<V>>> {
        Ok(Box::new(QueueIteratorTestImpl::default()))
    }

    async fn apply_diff(
        &self,
        _diff: QueueDiffWithMessages<V>,
        _block_id_short: BlockIdShort,
        _diff_hash: &HashBytes,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn commit_diff(&self, _mc_top_blocks: Vec<(BlockIdShort, bool)>) -> Result<()> {
        unimplemented!()
    }

    fn add_message_to_iterator(
        &self,
        _iterator: &mut Box<dyn QueueIterator<V>>,
        _message: V,
    ) -> Result<()> {
        unimplemented!()
    }

    fn commit_messages_to_iterator(
        &self,
        _iterator: &mut Box<dyn QueueIterator<V>>,
        _messages: Vec<(ShardIdent, QueueKey)>,
    ) -> Result<()> {
        unimplemented!()
    }

    fn clear_session_state(&self) -> Result<()> {
        unimplemented!()
    }
}

fn gen_stub_working_state(
    next_block_id_short: BlockIdShort,
    prev_block_info: (BlockIdShort, (u64, u64)),
    mc_block_info: (BlockIdShort, (u64, u64), (u64, u64)),
    top_shard_block_info: (BlockIdShort, (u64, u64)),
) -> Box<WorkingState> {
    let msgs_buffer = MessagesBuffer::new(next_block_id_short.shard, 3, 2);

    let prev_block_id = BlockId {
        shard: prev_block_info.0.shard,
        seqno: prev_block_info.0.seqno,
        root_hash: HashBytes::default(),
        file_hash: HashBytes::default(),
    };

    let tracker = MinRefMcStateTracker::new();
    let shard_state = ShardStateUnsplit {
        shard_ident: prev_block_id.shard,
        seqno: prev_block_id.seqno,
        gen_lt: prev_block_info.1 .1,
        ..Default::default()
    };
    let shard_state_root = CellBuilder::build_from(&shard_state).unwrap();
    let prev_state_stuff = ShardStateStuff::from_state_and_root(
        &prev_block_id,
        Box::new(shard_state),
        shard_state_root,
        &tracker,
    )
    .unwrap();
    let (prev_shard_data, usage_tree) =
        PrevData::build(vec![prev_state_stuff], vec![HashBytes::default()]).unwrap();

    let shard_descr = ShardDescription {
        seqno: top_shard_block_info.0.seqno,
        reg_mc_seqno: 1,
        start_lt: top_shard_block_info.1 .0,
        end_lt: top_shard_block_info.1 .1,
        root_hash: HashBytes::default(),
        file_hash: HashBytes::default(),
        before_split: false,
        before_merge: false,
        want_split: false,
        nx_cc_updated: false,
        want_merge: false,
        next_catchain_seqno: 1,
        ext_processed_to_anchor_id: 1,
        top_sc_block_updated: true,
        min_ref_mc_seqno: 1,
        gen_utime: 6,
        split_merge_at: None,
        fees_collected: CurrencyCollection::default(),
        funds_created: CurrencyCollection::default(),
        copyleft_rewards: Dict::default(),
        proof_chain: None,
    };
    let shards_info = [(ShardIdent::new_full(0), shard_descr)];

    let working_state = WorkingState {
        next_block_id_short,
        mc_data: Arc::new(McData {
            global_id: 0,
            block_id: BlockId {
                shard: mc_block_info.0.shard,
                seqno: mc_block_info.0.seqno,
                root_hash: HashBytes::default(),
                file_hash: HashBytes::default(),
            },
            prev_key_block_seqno: 0,
            gen_lt: mc_block_info.1 .1,
            gen_chain_time: 6944,
            libraries: Dict::default(),
            total_validator_fees: CurrencyCollection::default(),
            global_balance: CurrencyCollection::default(),
            shards: ShardHashes::from_shards(shards_info.iter().map(|(k, v)| (k, v))).unwrap(),
            config: BlockchainConfig::new_empty(HashBytes([0x55; 32])),
            validator_info: ValidatorInfo {
                validator_list_hash_short: 0,
                catchain_seqno: 1,
                nx_cc_updated: false,
            },
            processed_upto: ProcessedUptoInfoStuff {
                internals: [
                    (ShardIdent::new_full(-1), InternalsProcessedUptoStuff {
                        processed_to_msg: (mc_block_info.2 .1, HashBytes::default()).into(),
                        read_to_msg: (mc_block_info.2 .1, HashBytes::default()).into(),
                    }),
                    (ShardIdent::new_full(0), InternalsProcessedUptoStuff {
                        processed_to_msg: (top_shard_block_info.1 .1, HashBytes::default()).into(),
                        read_to_msg: (top_shard_block_info.1 .1, HashBytes::default()).into(),
                    }),
                ]
                .into(),
                externals: Some(ExternalsProcessedUpto {
                    processed_to: (4, 4),
                    read_to: (4, 4),
                }),
                processed_offset: 0,
            },
            ref_mc_state_handle: prev_shard_data.ref_mc_state_handle().clone(),
        }),
        wu_used_from_last_anchor: 0,
        prev_shard_data: Some(prev_shard_data),
        usage_tree: Some(usage_tree),
        has_unprocessed_messages: Some(true),
        msgs_buffer: Some(msgs_buffer),
    };

    Box::new(working_state)
}

#[tokio::test]
async fn test_refill_msgs_buffer_with_only_externals() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let mc_shard_id = ShardIdent::new_full(-1);
    let shard_id = ShardIdent::new_full(0);
    let mut exec_manager = ExecutionManager::new(shard_id, 20);

    let mut anchors_cache = AnchorsCache::default();
    fill_test_anchors_cache(&mut anchors_cache, shard_id);

    let mc_block_info = (
        BlockIdShort {
            shard: shard_id,
            seqno: 1,
        },
        (3001, 3020),
        (0, 0),
    );
    let top_shard_block_info = (
        BlockIdShort {
            shard: shard_id,
            seqno: 2,
        },
        (2001, 2041),
    );

    // let prev_block_info = top_shard_block_info;
    // let prev_block_upto = ((0, 0), (1001, 1052));
    // let next_block_info = (
    //     BlockIdShort {
    //         shard: shard_id,
    //         seqno: 3,
    //     },
    //     (4001, 4073),
    // );

    let prev_block_info = (
        BlockIdShort {
            shard: shard_id,
            seqno: 3,
        },
        (4001, 4073),
    );
    let prev_block_upto = ((3001, 3020), (2001, 2041));
    let next_block_info = (
        BlockIdShort {
            shard: shard_id,
            seqno: 4,
        },
        (5001, 5035),
    );

    let mut collation_data = build_stub_collation_data(next_block_info.0, &anchors_cache, 0);
    let working_state = gen_stub_working_state(
        next_block_info.0,
        prev_block_info,
        mc_block_info,
        top_shard_block_info,
    );
    let (working_state, mut msgs_buffer) = working_state.take_msgs_buffer();

    let mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>> =
        Arc::new(MessageQueueAdapterTestImpl::default());
    let mut mq_iterator_adapter = QueueIteratorAdapter::new(
        shard_id,
        mq_adapter,
        msgs_buffer.current_iterator_positions.take().unwrap(),
    );

    // ===================================
    // Set ProcessedUpto like we read and processed all internals,
    // read externals but have not processed them all
    collation_data.processed_upto = ProcessedUptoInfoStuff {
        internals: [
            (ShardIdent::new_full(-1), InternalsProcessedUptoStuff {
                processed_to_msg: (prev_block_upto.0 .1, HashBytes::default()).into(),
                read_to_msg: (prev_block_upto.0 .1, HashBytes::default()).into(),
            }),
            (ShardIdent::new_full(0), InternalsProcessedUptoStuff {
                processed_to_msg: (prev_block_upto.1 .1, HashBytes::default()).into(),
                read_to_msg: (prev_block_upto.1 .1, HashBytes::default()).into(),
            }),
        ]
        .into(),
        externals: Some(ExternalsProcessedUpto {
            processed_to: (4, 4),
            read_to: (16, 3),
        }),
        processed_offset: 2,
    };

    let prev_processed_offset = collation_data.processed_upto.processed_offset;
    assert!(!msgs_buffer.has_pending_messages());
    assert!(prev_processed_offset > 0);

    while msgs_buffer.message_groups_offset() < prev_processed_offset {
        let msg_group = exec_manager
            .get_next_message_group(
                &mut msgs_buffer,
                &mut anchors_cache,
                &mut collation_data,
                &mut mq_iterator_adapter,
                &QueueKey::MIN,
                &working_state,
                GetNextMessageGroupMode::Refill,
            )
            .await
            .unwrap();
        if msg_group.is_none() {
            break;
        }
    }

    println!(
        "after refill collation_data.processed_upto = {:?}",
        collation_data.processed_upto
    );

    assert_eq!(msgs_buffer.message_groups_offset(), prev_processed_offset);
    assert_eq!(
        collation_data.processed_upto.processed_offset,
        prev_processed_offset
    );

    let (last_imported_anchor_id, _) = anchors_cache.get_last_imported_anchor_id_and_ct().unwrap();
    assert_eq!(last_imported_anchor_id, 40);

    assert_eq!(msgs_buffer.current_ext_reader_position.unwrap(), (16, 3));
    let processed_upto_externals = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(processed_upto_externals.processed_to, (4, 4));
    assert_eq!(processed_upto_externals.read_to, (16, 3));

    assert_eq!(msgs_buffer.message_groups.int_messages_count(), 0);
    assert_eq!(msgs_buffer.message_groups.ext_messages_count(), 1);

    let processed_upto_internals = &collation_data.processed_upto.internals;
    let mc_upto_int = processed_upto_internals.get(&mc_shard_id).unwrap();
    assert_eq!(
        mc_upto_int.processed_to_msg,
        (prev_block_upto.0 .1, HashBytes::default()).into()
    );
    let sc_upto_int = processed_upto_internals.get(&shard_id).unwrap();
    assert_eq!(
        sc_upto_int.read_to_msg,
        (prev_block_upto.1 .1, HashBytes::default()).into()
    );

    // ===================================
    // And finish reading all messages
    loop {
        let msg_group = exec_manager
            .get_next_message_group(
                &mut msgs_buffer,
                &mut anchors_cache,
                &mut collation_data,
                &mut mq_iterator_adapter,
                &QueueKey::MIN,
                &working_state,
                GetNextMessageGroupMode::Continue,
            )
            .await
            .unwrap();
        if msg_group.is_none() {
            break;
        }
    }

    println!(
        "after reading finished collation_data.processed_upto = {:?}",
        collation_data.processed_upto
    );

    assert_eq!(msgs_buffer.message_groups_offset(), 0);
    assert_eq!(collation_data.processed_upto.processed_offset, 0);

    assert_eq!(msgs_buffer.current_ext_reader_position.unwrap(), (40, 0));
    let processed_upto_externals = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(processed_upto_externals.processed_to, (40, 0));
    assert_eq!(processed_upto_externals.read_to, (40, 0));

    assert_eq!(msgs_buffer.message_groups.int_messages_count(), 0);
    assert_eq!(msgs_buffer.message_groups.ext_messages_count(), 0);
}
