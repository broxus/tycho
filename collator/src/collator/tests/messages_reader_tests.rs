use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::cell::{Cell, CellBuilder, CellFamily, HashBytes, Store};
use everscale_types::dict::Dict;
use everscale_types::models::{
    BlockId, BlockIdShort, ExtInMsgInfo, IntAddr, IntMsgInfo, Message, MsgInfo,
    MsgsExecutionParams, ShardIdent, StdAddr,
};
use indexmap::IndexMap;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use tycho_block_util::queue::{QueueDiffStuff, QueueKey};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use super::{
    CumulativeStatsCalcParams, FinalizedMessagesReader, GetNextMessageGroupMode, MessagesReader,
    MessagesReaderContext, ReaderState,
};
use crate::collator::messages_buffer::MessageGroup;
use crate::collator::types::{AnchorsCache, ParsedMessage};
use crate::internal_queue::types::{
    DiffStatistics, DiffZone, EnqueuedMessage, InternalMessageValue,
};
use crate::mempool::{ExternalMessage, MempoolAnchor, MempoolAnchorId};
use crate::queue_adapter::MessageQueueAdapter;
use crate::test_utils::{create_test_queue_adapter, try_init_test_tracing};
use crate::types::processed_upto::{
    BlockSeqno, Lt, ProcessedUptoInfoExtension, ProcessedUptoInfoStuff,
};
use crate::types::{DebugDisplay, ProcessedToByPartitions};

const DEX_PAIR_USDC_NATIVE: u8 = 10;
const DEX_PAIR_NATIVE_ETH: u8 = 11;
const DEX_PAIR_NATIVE_BTC: u8 = 12;
const DEX_PAIR_USDC_USDT: u8 = 13;
const DEX_PAIR_USDT_BNB: u8 = 14;

#[tokio::test]
async fn test_refill_messages() -> Result<()> {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let sc_shard_id = ShardIdent::new_full(0);

    //--------------
    // SET UP ADDRESSES
    //--------------

    // one-to-many address
    let one_to_many_address = IntAddr::Std(StdAddr::new(0, HashBytes([255; 32])));

    // non existed account address
    let non_existed_account_address = IntAddr::Std(StdAddr::new(0, HashBytes([254; 32])));

    // dex pairs addresses
    let dex_pairs: BTreeMap<u8, IntAddr> = [
        (
            DEX_PAIR_USDC_NATIVE,
            IntAddr::Std(StdAddr::new(0, HashBytes([DEX_PAIR_USDC_NATIVE; 32]))),
        ), // USDC/NATIVE
        (
            DEX_PAIR_NATIVE_ETH,
            IntAddr::Std(StdAddr::new(0, HashBytes([DEX_PAIR_NATIVE_ETH; 32]))),
        ), // NATIVE/ETH
        (
            DEX_PAIR_NATIVE_BTC,
            IntAddr::Std(StdAddr::new(0, HashBytes([DEX_PAIR_NATIVE_BTC; 32]))),
        ), // NATIVE/BTC
        (
            DEX_PAIR_USDC_USDT,
            IntAddr::Std(StdAddr::new(0, HashBytes([DEX_PAIR_USDC_USDT; 32]))),
        ), // USDC/USDT
        (
            DEX_PAIR_USDT_BNB,
            IntAddr::Std(StdAddr::new(0, HashBytes([DEX_PAIR_USDT_BNB; 32]))),
        ), // USDT/BNB
    ]
    .into_iter()
    .collect();

    // dex wallets addresses
    let mut dex_wallets = BTreeMap::<u8, IntAddr>::new();
    for i in 30..50 {
        dex_wallets.insert(i, IntAddr::Std(StdAddr::new(0, HashBytes([i; 32]))));
    }

    // transfers wallets addresses
    let mut transfers_wallets = BTreeMap::<u8, IntAddr>::new();
    for i in 100..120 {
        transfers_wallets.insert(i, IntAddr::Std(StdAddr::new(0, HashBytes([i; 32]))));
    }

    //--------------
    // SET UP MESSAGES EXEC PARAMS
    //--------------
    let mut group_slots_fractions = Dict::<u16, u8>::new();
    group_slots_fractions.set(0, 80)?;
    group_slots_fractions.set(1, 10)?;
    let msgs_exec_params = Arc::new(MsgsExecutionParams {
        buffer_limit: 30,
        group_limit: 5,
        group_vert_size: 2,
        externals_expire_timeout: 10,
        open_ranges_limit: 3,
        par_0_ext_msgs_count_limit: 10,
        // isolated partition logic executed when partition 0 messages count limit is <= 50
        par_0_int_msgs_count_limit: 50,
        group_slots_fractions,
        range_messages_limit: 20,
    });

    const DEFAULT_BLOCK_EXEC_COUNT_LIMIT: usize = 20;

    //--------------
    // INIT PROCESSED UPTO
    //--------------
    let processed_upto = ProcessedUptoInfoStuff::default();

    //--------------
    // INIT TEST ADAPTER
    //--------------
    // test messages factory and executor
    let msgs_factory =
        TestMessageFactory::new(dex_pairs, |info, cell| EnqueuedMessage { info, cell });

    // queue adapter
    let (primary_mq_adapter, _primary_tmp_dir) = create_test_queue_adapter().await?;
    let (secondary_mq_adapter, _secondary_tmp_dir) = create_test_queue_adapter().await?;

    // test shard collator
    let sc_collator = TestCollator {
        msgs_exec_params: msgs_exec_params.clone(),

        shard_id: sc_shard_id,
        block_seqno: 0,

        last_block_gen_lt: 0,
        curr_lt: 0,

        last_queue_diff_hash: HashBytes::default(),

        last_mc_top_shards_blocks_info: vec![],

        primary_working_state: Some(TestWorkingState {
            anchors_cache: AnchorsCache::default(),
            reader_state: ReaderState::new(&processed_upto),
        }),

        primary_mq_adapter: primary_mq_adapter.clone(),
        secondary_mq_adapter: secondary_mq_adapter.clone(),
    };

    // test master collator
    let mc_collator = TestCollator {
        msgs_exec_params: msgs_exec_params.clone(),

        shard_id: ShardIdent::MASTERCHAIN,
        block_seqno: 0,

        last_block_gen_lt: 0,
        curr_lt: 0,

        last_queue_diff_hash: HashBytes::default(),

        last_mc_top_shards_blocks_info: vec![],

        primary_working_state: Some(TestWorkingState {
            anchors_cache: AnchorsCache::default(),
            reader_state: ReaderState::new(&processed_upto),
        }),

        primary_mq_adapter,
        secondary_mq_adapter,
    };

    // test adapter
    let mut test_adapter = TestAdapter {
        msgs_factory,
        mc_collator,
        sc_collator,
    };

    //--------------
    // TEST CASE 001: EXTERNALS TO NON EXISTED ACCOUNTS
    //--------------
    tracing::trace!("TEST CASE 001: EXTERNALS TO NON EXISTED ACCOUNTS");

    let messages = test_adapter
        .msgs_factory
        .create_messages_to_non_existed_account(&non_existed_account_address, 1)?;
    test_adapter.import_anchor_with_messages(messages);
    // collate block and check refill
    test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;

    //--------------
    // TEST CASE 002:
    //  * we have some existing internals in queue
    //  * we have many imported externals
    //  * existing internals finished, all read externals finished, we moved to ExternalsAndNew stage
    //  * small part of externals produces new messages with the same dst as externals, so we read all new messages each step
    //  * finally we have some externals and new messages in buffers
    //--------------
    tracing::trace!("TEST CASE 002");

    // process anchor with 14 transfer externals, this will produce 14 internals
    let transfer_messages = test_adapter
        .msgs_factory
        .create_transfer_messages(&transfers_wallets, 14)?;
    test_adapter.import_anchor_with_messages(transfer_messages);
    // 20 of 28 messages will be executed, 8 new messages will be added to queue
    test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;

    // process anchor with 40 externals (5 transfer and 35 dummy)
    let mut transfer_messages = test_adapter
        .msgs_factory
        .create_transfer_messages(&transfers_wallets, 5)?;
    let target_accounts: Vec<_> = transfer_messages
        .iter()
        .map(|m| m.info.dst.clone())
        .collect();
    let mut messages = test_adapter
        .msgs_factory
        .create_dummy_messages(&target_accounts, 35)?;
    messages.append(&mut transfer_messages);
    test_adapter.import_anchor_with_messages(messages);
    // 8 existing internals collected first, then will be collected already read externals,
    // then will be collecting externals and new messages
    test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;

    // and process empty anchor to check refill
    test_adapter.import_anchor_with_messages(vec![]);
    test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;

    while let TestCollateResult {
        has_unprocessed_messages: true,
    } = test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?
    {
        // process all messages
    }

    //--------------
    // TEST CASE 003: REFILL EXTERNALS WITH 2+ OPEN RANGES
    //  * we have some existing internals in queue
    //  * we have some imported anchors with externals
    //  * existing internals finished, all read externals finished, we moved to ExternalsAndNew stage
    //  * we read externals minimum 2 times, not all externals read and we proceed to next block
    //  * we open one more externals range but not fully read them
    //  * on refill we should fully read first externals range and only then proceed to the next
    //--------------
    tracing::trace!("TEST CASE 003: REFILL EXTERNALS WITH 2+ OPEN RANGES");

    // process anchor with transfer externals amount > block limit
    let transfer_messages = test_adapter
        .msgs_factory
        .create_transfer_messages(&transfers_wallets, 25)?;
    test_adapter.import_anchor_with_messages(transfer_messages);
    // some unprocessed new messages will be added to queue
    test_adapter.test_collate_shards(40)?;

    // process anchor with lot of externals (transfer < dummy)
    let mut transfer_messages = test_adapter
        .msgs_factory
        .create_transfer_messages(&transfers_wallets, 30)?;
    let target_accounts: Vec<_> = transfer_messages
        .iter()
        .map(|m| m.info.dst.clone())
        .collect();
    let mut messages = test_adapter
        .msgs_factory
        .create_dummy_messages(&target_accounts, 70)?;
    // let mut messages =
    //     test_adapter.create_messages_to_non_existed_account(&non_existed_account_address, 90)?;
    messages.append(&mut transfer_messages);
    test_adapter.import_anchor_with_messages(messages);
    // will start read new anchor, then all existing internals will be collected,
    // then will be collected previously read externals,
    // then will read and collect externals and new messages minimum 2 times
    test_adapter.test_collate_shards(60)?;

    // will open new externals range, read some externals
    // will collect all existing internals
    // then will start to collect previously read externals
    // finally we will have 2 externals open ranges
    test_adapter.import_anchor_with_messages(vec![]);
    test_adapter.test_collate_shards(40)?;

    // we should correctly read externals in 2 ranges on refill
    test_adapter.import_anchor_with_messages(vec![]);
    test_adapter.test_collate_shards(40)?;

    while let TestCollateResult {
        has_unprocessed_messages: true,
    } = test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?
    {
        // process all messages
    }

    //--------------
    // TEST CASE 004: START one-to-many AND PROCESS SOME BLOCKS
    //--------------
    tracing::trace!("TEST CASE 004:  START one-to-many AND PROCESS SOME BLOCKS");

    // import anchor with one-to-many start message
    let messages = test_adapter
        .msgs_factory
        .create_one_to_many_start_message(one_to_many_address.clone())?;
    test_adapter.import_anchor_with_messages(messages);

    test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;

    for _ in 0..3 {
        test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;
    }

    //--------------
    // TEST CASE 005: RUN SWAPS
    //--------------
    tracing::trace!("TEST CASE 005: RUN SWAPS");

    for _ in 0..10 {
        let messages = test_adapter
            .msgs_factory
            .create_swap_messages(&dex_wallets, 10)?;
        test_adapter.import_anchor_with_messages(messages);
        test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;
    }

    //--------------
    // TEST CASE 006: RUN SWAPS AND TRANSFERS
    //--------------
    tracing::trace!("TEST CASE 006: RUN SWAPS AND TRANSFERS");

    for _ in 0..20 {
        // we create such amout of externals per anchor so that they do not fit one buffer limit
        let target_total_msgs_count = (msgs_exec_params.buffer_limit + 10) as usize;
        let target_swap_msgs_count = target_total_msgs_count / 3;
        let target_transfer_msgs_count = target_total_msgs_count - target_swap_msgs_count;
        let mut messages = test_adapter
            .msgs_factory
            .create_swap_messages(&dex_wallets, target_swap_msgs_count)?;
        let mut transfer_messages = test_adapter
            .msgs_factory
            .create_transfer_messages(&transfers_wallets, target_transfer_msgs_count)?;
        messages.append(&mut transfer_messages);
        test_adapter.import_anchor_with_messages(messages);
        test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;
    }

    while let TestCollateResult {
        has_unprocessed_messages: true,
    } = test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?
    {
        // process all messages
    }

    //--------------
    // TEST CASE 007: START NEW one-to-many AND RUN TRANSFERS BETWEEN MASTER AND SHARDS
    //--------------
    tracing::trace!(
        "TEST CASE 007: START NEW one-to-many AND RUN TRANSFERS BETWEEN MASTER AND SHARDS"
    );

    // add accounts from master to transfers
    transfers_wallets.clear();
    for i in 100..120 {
        let workchain = if i % 2 == 0 { 0 } else { -1 };
        transfers_wallets.insert(i, IntAddr::Std(StdAddr::new(workchain, HashBytes([i; 32]))));
    }

    // import anchor with one-to-many start message
    let messages = test_adapter
        .msgs_factory
        .create_one_to_many_start_message(one_to_many_address.clone())?;
    test_adapter.import_anchor_with_messages(messages);

    test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;

    for _ in 0..2 {
        test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;
    }

    for i in 0..10 {
        // we create such amount of externals per anchor so that they do not fit one buffer limit
        let msgs_count = (msgs_exec_params.buffer_limit + 20) as usize;
        let transfer_messages = test_adapter
            .msgs_factory
            .create_transfer_messages(&transfers_wallets, msgs_count)?;
        test_adapter.import_anchor_with_messages(transfer_messages);

        // collate after importing 2 anchors
        if i % 2 != 0 {
            test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?;
        }
    }

    //--------------
    // PROCESS THE REST OF MESSAGES QUEUES
    //--------------
    while let TestCollateResult {
        has_unprocessed_messages: true,
    } = test_adapter.test_collate_shards(DEFAULT_BLOCK_EXEC_COUNT_LIMIT)?
    {
        // process all messages
    }

    Ok(())
}

struct TestAdapter<V, F>
where
    V: InternalMessageValue,
    F: Fn(IntMsgInfo, Cell) -> V,
{
    msgs_factory: TestMessageFactory<V, F>,

    mc_collator: TestCollator<V>,
    sc_collator: TestCollator<V>,
}

impl<V, F> TestAdapter<V, F>
where
    V: InternalMessageValue,
    F: Fn(IntMsgInfo, Cell) -> V,
{
    fn test_collate_shards(&mut self, block_tx_limit: usize) -> Result<TestCollateResult> {
        let collate_master_every = 3;

        let all_shards_processed_to_by_partitions =
            self.get_all_shards_processed_to_by_partitions();

        let TestCollateResult {
            mut has_unprocessed_messages,
        } = self.sc_collator.test_collate_block_and_check_refill(
            block_tx_limit,
            &mut self.msgs_factory,
            self.mc_collator.last_block_gen_lt,
            vec![(
                self.sc_collator.shard_id,
                self.sc_collator.block_seqno,
                self.sc_collator.last_block_gen_lt,
            )],
            all_shards_processed_to_by_partitions,
            (self.sc_collator.block_seqno + 1) % collate_master_every == 1,
        )?;

        // collate master every 3 shard blocks
        if self.sc_collator.block_seqno % collate_master_every == 0 {
            let all_shards_processed_to = self.get_all_shards_processed_to_by_partitions();

            let TestCollateResult {
                has_unprocessed_messages: mc_has_unprocessed_messages,
            } = self.mc_collator.test_collate_block_and_check_refill(
                block_tx_limit,
                &mut self.msgs_factory,
                self.mc_collator.last_block_gen_lt,
                vec![(
                    self.sc_collator.shard_id,
                    self.sc_collator.block_seqno,
                    self.sc_collator.last_block_gen_lt,
                )],
                all_shards_processed_to,
                true, // every master is first after previous
            )?;
            has_unprocessed_messages = has_unprocessed_messages || mc_has_unprocessed_messages;
        }

        // emulate that we commit previous master block when the 2d shard block after it is collated
        if self.sc_collator.block_seqno % collate_master_every == 2
            && self.sc_collator.block_seqno != 2
        {
            let mut mc_top_blocks = vec![(self.mc_collator.get_block_id(), true)];
            mc_top_blocks.extend(self.mc_collator.last_mc_top_shards_blocks_info.iter().map(
                |(shard_id, seqno, _)| {
                    (
                        BlockId {
                            shard: *shard_id,
                            seqno: *seqno,
                            root_hash: HashBytes::default(),
                            file_hash: HashBytes::default(),
                        },
                        true,
                    )
                },
            ));
            let partitions = [0, 1].into_iter().collect();
            self.mc_collator
                .primary_mq_adapter
                .commit_diff(&mc_top_blocks, &partitions)?;
        }

        Ok(TestCollateResult {
            has_unprocessed_messages,
        })
    }

    fn get_all_shards_processed_to_by_partitions(
        &self,
    ) -> FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)> {
        let mut res: FastHashMap<_, _> = self
            .sc_collator
            .primary_working_state
            .as_ref()
            .map(|ws| {
                (
                    self.sc_collator.shard_id,
                    (
                        true,
                        ws.reader_state
                            .get_updated_processed_upto()
                            .get_internals_processed_to_by_partitions(),
                    ),
                )
            })
            .into_iter()
            .collect();

        if let Some(processed_to_by_partitions) =
            self.mc_collator.primary_working_state.as_ref().map(|ws| {
                ws.reader_state
                    .get_updated_processed_upto()
                    .get_internals_processed_to_by_partitions()
            })
        {
            res.insert(ShardIdent::MASTERCHAIN, (true, processed_to_by_partitions));
        }

        res
    }

    fn import_anchor_with_messages(&mut self, messages: Vec<TestExternalMessage>) {
        let anchor = self.msgs_factory.create_anchor_with_messages(messages);
        self.sc_collator.import_anchor(anchor.clone());
        self.mc_collator.import_anchor(anchor);
    }
}

struct TestCollateResult {
    has_unprocessed_messages: bool,
}

struct TestCollator<V: InternalMessageValue> {
    msgs_exec_params: Arc<MsgsExecutionParams>,

    shard_id: ShardIdent,
    block_seqno: BlockSeqno,

    last_block_gen_lt: Lt,
    curr_lt: Lt,

    last_queue_diff_hash: HashBytes,

    last_mc_top_shards_blocks_info: Vec<(ShardIdent, BlockSeqno, Lt)>,

    primary_working_state: Option<TestWorkingState>,

    primary_mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    secondary_mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
}

impl<V: InternalMessageValue> TestCollator<V> {
    fn get_block_id(&self) -> BlockId {
        BlockId {
            shard: self.shard_id,
            seqno: self.block_seqno,
            root_hash: HashBytes::default(),
            file_hash: HashBytes::default(),
        }
    }

    #[tracing::instrument("test_collate", skip_all, fields(block_id = %BlockIdShort { shard: self.shard_id, seqno: self.block_seqno + 1 }))]
    fn test_collate_block_and_check_refill<F>(
        &mut self,
        block_tx_limit: usize,
        msgs_factory: &mut TestMessageFactory<V, F>,
        mc_gen_lt: Lt,
        mc_top_shards_blocks_info: Vec<(ShardIdent, BlockSeqno, Lt)>,
        all_shards_processed_to_by_partitions: FastHashMap<
            ShardIdent,
            (bool, ProcessedToByPartitions),
        >,
        is_first_block_after_prev_master: bool,
    ) -> Result<TestCollateResult>
    where
        F: Fn(IntMsgInfo, Cell) -> V,
    {
        let TestWorkingState {
            anchors_cache,
            reader_state,
        } = self.primary_working_state.take().unwrap();

        let processed_upto = reader_state.get_updated_processed_upto();

        self.block_seqno += 1;
        let next_chain_time = anchors_cache
            .last_imported_anchor()
            .map(|a| a.ct)
            .unwrap_or_default();

        let mc_top_shards_end_lts: Vec<_> = mc_top_shards_blocks_info
            .iter()
            .map(|(shard_id, _, end_lt)| (*shard_id, *end_lt))
            .collect();
        self.last_mc_top_shards_blocks_info = mc_top_shards_blocks_info;

        let cumulative_stats_calc_params = Some(CumulativeStatsCalcParams {
            all_shards_processed_to_by_partitions,
        });

        // create primary reader
        let mut primary_messages_reader = self.create_primary_reader(
            MessagesReaderContext {
                for_shard_id: self.shard_id,
                block_seqno: self.block_seqno,
                next_chain_time,
                msgs_exec_params: self.msgs_exec_params.clone(),
                mc_state_gen_lt: mc_gen_lt,
                prev_state_gen_lt: self.last_block_gen_lt,
                mc_top_shards_end_lts: mc_top_shards_end_lts.clone(),
                reader_state,
                anchors_cache,
                is_first_block_after_prev_master,
                cumulative_stats_calc_params: cumulative_stats_calc_params.clone(),
            },
            self.primary_mq_adapter.clone(),
        )?;

        // create secondary reader
        let secondary_reader_state = ReaderState::new(&processed_upto);
        let secondary_anchors_cache =
            msgs_factory.init_anchors_cache(&self.shard_id, &processed_upto);
        let mut secondary_messages_reader = self.create_secondary_reader(
            MessagesReaderContext {
                for_shard_id: self.shard_id,
                block_seqno: self.block_seqno,
                next_chain_time,
                msgs_exec_params: self.msgs_exec_params.clone(),
                mc_state_gen_lt: mc_gen_lt,
                prev_state_gen_lt: self.last_block_gen_lt,
                mc_top_shards_end_lts,
                reader_state: secondary_reader_state,
                anchors_cache: secondary_anchors_cache,
                cumulative_stats_calc_params,
                is_first_block_after_prev_master: true,
            },
            self.secondary_mq_adapter.clone(),
        )?;

        // refill in secondary reader
        Self::refill_secondary(&mut secondary_messages_reader)?;

        // prepare to execute
        let start_lt = std::cmp::max(self.last_block_gen_lt, mc_gen_lt) + 1000;
        self.curr_lt = start_lt;

        // read and execute until messages finished or limits reached
        let mut total_exec_count = 0;
        let mut groups_count = 0;
        loop {
            groups_count += 1;

            // read message group in primary
            let msg_group_opt = Self::collect_primary(&mut primary_messages_reader, self.curr_lt)?;

            if groups_count == 1 {
                // read message group in secondary after refill
                let secondary_msg_group_opt =
                    Self::collect_secondary(&mut secondary_messages_reader, self.curr_lt)?;

                // compare messages groups
                self.assert_message_group_opt_eq(
                    msgs_factory,
                    &msg_group_opt,
                    &secondary_msg_group_opt,
                );
            }

            // execute message group and create new internal messages
            if let Some(msg_group) = msg_group_opt {
                let TestExecuteGroupResult {
                    exec_count,
                    created_messages,
                    max_lt,
                } = msgs_factory.execute_group(msg_group, self.curr_lt)?;
                self.curr_lt = max_lt;
                total_exec_count += exec_count;

                let created_messages_count = created_messages.len();

                let mut new_messages = vec![];
                for test_int_msg in created_messages {
                    new_messages.push(test_int_msg.msg);
                }
                primary_messages_reader.add_new_messages(new_messages);

                tracing::trace!(
                    groups_count,
                    group_tx_count = exec_count,
                    total_exec_count,
                    created_messages_count,
                    "message group executed",
                );

                if total_exec_count >= block_tx_limit {
                    tracing::trace!(
                        has_not_fully_read_externals_ranges =
                            primary_messages_reader.has_not_fully_read_externals_ranges(),
                        has_not_fully_read_internals_ranges =
                            primary_messages_reader.has_not_fully_read_internals_ranges(),
                        has_pending_new_messages =
                            primary_messages_reader.has_pending_new_messages(),
                        has_messages_in_buffers = primary_messages_reader.has_messages_in_buffers(),
                        has_externals_in_buffers =
                            primary_messages_reader.has_externals_in_buffers(),
                        has_internals_in_buffers =
                            primary_messages_reader.has_internals_in_buffers(),
                        has_pending_externals_in_cache =
                            primary_messages_reader.has_pending_externals_in_cache(),
                        "block limits reached: total_exec_count {}/{}",
                        total_exec_count,
                        block_tx_limit,
                    );
                    break;
                }
            } else if !primary_messages_reader.can_read_and_collect_more_messages() {
                // stop reading message groups when all buffers are empty
                // and all internals ranges, new messages, and externals fully read as well
                break;
            }
        }

        // finalize test block and update message queue

        // finalize reader
        let other_shards_top_block_diffs = FastHashMap::default();
        let FinalizedMessagesReader {
            has_unprocessed_messages,
            queue_diff_with_msgs,
            reader_state,
            anchors_cache,
            ..
        } = primary_messages_reader.finalize(self.curr_lt, &other_shards_top_block_diffs, true)?;

        // create diff and compute hash
        let (min_message, max_message) = {
            let messages = &queue_diff_with_msgs.messages;
            match messages.first_key_value().zip(messages.last_key_value()) {
                Some(((min, _), (max, _))) => (*min, *max),
                None => (
                    QueueKey::min_for_lt(start_lt),
                    QueueKey::max_for_lt(self.curr_lt),
                ),
            }
        };
        let queue_diff =
            QueueDiffStuff::builder(self.shard_id, self.block_seqno, &self.last_queue_diff_hash)
                .with_processed_to(reader_state.internals.get_min_processed_to_by_shards())
                .with_messages(
                    &min_message,
                    &max_message,
                    queue_diff_with_msgs.messages.keys().map(|k| &k.hash),
                )
                .serialize();
        let queue_diff_hash = *queue_diff.hash();

        // update messages queue
        let statistics = DiffStatistics::from_diff(
            &queue_diff_with_msgs,
            self.shard_id,
            min_message,
            max_message,
        );
        self.primary_mq_adapter.apply_diff(
            queue_diff_with_msgs.clone(),
            BlockIdShort {
                shard: self.shard_id,
                seqno: self.block_seqno,
            },
            &queue_diff_hash,
            statistics.clone(),
            Some(DiffZone::Both),
        )?;
        self.secondary_mq_adapter.apply_diff(
            queue_diff_with_msgs,
            BlockIdShort {
                shard: self.shard_id,
                seqno: self.block_seqno,
            },
            &queue_diff_hash,
            statistics,
            Some(DiffZone::Both),
        )?;

        // update shard block gen lt
        self.last_block_gen_lt = self.curr_lt;
        self.last_queue_diff_hash = queue_diff_hash;

        tracing::trace!(last_block_gen_lt = ?self.last_block_gen_lt);

        // update working state
        self.primary_working_state = Some(TestWorkingState {
            anchors_cache,
            reader_state,
        });

        Ok(TestCollateResult {
            has_unprocessed_messages,
        })
    }

    #[tracing::instrument(skip_all)]
    fn create_primary_reader(
        &self,
        cx: MessagesReaderContext,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    ) -> Result<MessagesReader<V>> {
        tracing::trace!(
            for_shard_id = %cx.for_shard_id,
            block_seqno = cx.block_seqno,
            prev_state_gen_lt = cx.prev_state_gen_lt,
            mc_state_gen_lt = cx.mc_state_gen_lt,
            mc_top_shards_end_lts = ?cx.mc_top_shards_end_lts,
            cumulative_stats_calc_params = ?cx.cumulative_stats_calc_params,
        );
        MessagesReader::new(cx, mq_adapter)
    }

    #[tracing::instrument(skip_all)]
    fn create_secondary_reader(
        &self,
        cx: MessagesReaderContext,
        mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    ) -> Result<MessagesReader<V>> {
        tracing::trace!(
            for_shard_id = %cx.for_shard_id,
            block_seqno = cx.block_seqno,
            prev_state_gen_lt = cx.prev_state_gen_lt,
            mc_state_gen_lt = cx.mc_state_gen_lt,
            mc_top_shards_end_lts = ?cx.mc_top_shards_end_lts,
            cumulative_stats_calc_params = ?cx.cumulative_stats_calc_params,
        );
        MessagesReader::new(cx, mq_adapter)
    }

    #[tracing::instrument(skip_all)]
    fn refill_secondary(secondary_messages_reader: &mut MessagesReader<V>) -> Result<()> {
        if secondary_messages_reader.check_need_refill() {
            secondary_messages_reader.refill_buffers_upto_offsets(|| false)?;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn collect_primary(
        messages_reader: &mut MessagesReader<V>,
        curr_lt: Lt,
    ) -> Result<Option<MessageGroup>> {
        let msg_group_opt =
            messages_reader.get_next_message_group(GetNextMessageGroupMode::Continue, curr_lt)?;
        Ok(msg_group_opt)
    }

    #[tracing::instrument(skip_all)]
    fn collect_secondary(
        messages_reader: &mut MessagesReader<V>,
        curr_lt: Lt,
    ) -> Result<Option<MessageGroup>> {
        let msg_group_opt =
            messages_reader.get_next_message_group(GetNextMessageGroupMode::Continue, curr_lt)?;
        Ok(msg_group_opt)
    }

    fn assert_message_group_opt_eq(
        &self,
        tst_msg_info_src: &impl GetTestMsgInfo,
        expected: &Option<MessageGroup>,
        actual: &Option<MessageGroup>,
    ) {
        match (expected, actual) {
            (Some(expected_group), Some(actual_group)) => {
                // both Some, compare
                self.assert_message_group_eq(tst_msg_info_src, expected_group, actual_group);
            }
            (None, None) => {
                // both None so they are equal
            }
            _ => {
                panic!(
                    "one message group is None while the other is Some (block_seqno={})",
                    self.block_seqno,
                );
            }
        }
    }

    fn assert_message_group_eq(
        &self,
        tst_msg_info_src: &impl GetTestMsgInfo,
        expected: &MessageGroup,
        actual: &MessageGroup,
    ) {
        // check that both have the same number of accounts
        assert_eq!(
            expected.msgs().len(),
            actual.msgs().len(),
            "different number of accounts in groups (block_seqno={})",
            self.block_seqno,
        );

        // iterate over each account and its messages in the expected group
        for (account_id, expected_msgs) in expected.msgs() {
            // retrieve the corresponding messages from the actual group
            let actual_msgs = actual.msgs().get(account_id).unwrap_or_else(|| {
                panic!(
                    "account {} is missing in the actual group (block_seqno={})",
                    account_id, self.block_seqno,
                )
            });

            // compare the number of messages
            assert_eq!(
                expected_msgs.len(),
                actual_msgs.len(),
                "different number of messages for account {} (block_seqno={})",
                account_id,
                self.block_seqno,
            );

            // compare each message one-by-one
            for (i, (exp_msg, act_msg)) in expected_msgs.iter().zip(actual_msgs.iter()).enumerate()
            {
                let exp_test_msg_info = tst_msg_info_src.get_test_msg_info(exp_msg);
                let act_test_msg_info = tst_msg_info_src.get_test_msg_info(act_msg);
                assert_eq!(
                    exp_test_msg_info, act_test_msg_info,
                    "mismatch message {} for account {} (block_seqno={})",
                    i, account_id, self.block_seqno,
                );

                assert_eq!(
                    exp_msg.info, act_msg.info,
                    "mismatch in 'info' field of message {} for account {} (block_seqno={})",
                    i, account_id, self.block_seqno,
                );
                assert_eq!(
                    exp_msg.dst_in_current_shard, act_msg.dst_in_current_shard,
                    "mismatch in 'dst_in_current_shard' field of message {} for account {} (block_seqno={})",
                    i, account_id, self.block_seqno,
                );
                assert_eq!(
                    exp_msg.cell.repr_hash(),
                    act_msg.cell.repr_hash(),
                    "mismatch in 'cell' field of message {} for account {} (block_seqno={})",
                    i,
                    account_id,
                    self.block_seqno,
                );
                assert_eq!(
                    exp_msg.special_origin, act_msg.special_origin,
                    "mismatch in 'special_origin' field of message {} for account {} (block_seqno={})",
                    i, account_id, self.block_seqno,
                );
                assert_eq!(
                    exp_msg.from_same_shard, act_msg.from_same_shard,
                    "mismatch in 'from_same_shard' field of message {} for account {} (block_seqno={})",
                    i, account_id, self.block_seqno,
                );
            }
        }
    }

    fn import_anchor(&mut self, anchor: Arc<MempoolAnchor>) {
        let Some(working_state) = self.primary_working_state.as_mut() else {
            return;
        };

        let our_exts_count = anchor.count_externals_for(&self.shard_id, 0);

        working_state
            .anchors_cache
            .insert(anchor.clone(), our_exts_count);
    }
}

struct TestWorkingState {
    anchors_cache: AnchorsCache,
    reader_state: ReaderState,
}

pub struct TestMessageFactory<V: InternalMessageValue, F>
where
    F: Fn(IntMsgInfo, Cell) -> V,
{
    rng: StdRng,

    mempool: BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>,

    last_anchor_id: MempoolAnchorId,
    last_anchor_ct: u64,

    ext_msgs_journal: IndexMap<HashBytes, TestExternalMessageState>,
    next_ext_idx: u64,

    int_msgs_journal: BTreeMap<QueueKey, TestInternalMessageState>,
    next_int_idx: u64,

    create_int_msg_value_func: F,

    one_to_many_counter: usize,

    dex_pairs: BTreeMap<u8, IntAddr>,
}

const DEVIATION_PERCENT: usize = 10;

impl<V: InternalMessageValue, F> TestMessageFactory<V, F>
where
    F: Fn(IntMsgInfo, Cell) -> V,
{
    pub fn new(dex_pairs: BTreeMap<u8, IntAddr>, create_int_msg_value_func: F) -> Self {
        Self {
            rng: StdRng::seed_from_u64(1236),

            mempool: BTreeMap::default(),

            last_anchor_id: 0,
            last_anchor_ct: 0,

            ext_msgs_journal: Default::default(),
            next_ext_idx: 0,

            int_msgs_journal: Default::default(),
            next_int_idx: 0,

            create_int_msg_value_func,

            one_to_many_counter: 0,

            dex_pairs,
        }
    }

    fn execute_group(
        &mut self,
        msg_group: MessageGroup,
        curr_lt: Lt,
    ) -> Result<TestExecuteGroupResult<V>> {
        let mut exec_count = 0;
        let mut created_messages = vec![];

        let mut max_lt = curr_lt;

        for (_account_id, msgs) in msg_group {
            let mut account_lt = curr_lt + 1;

            for msg in msgs {
                exec_count += 1;
                account_lt += 1;

                let test_msg_info = self.get_test_msg_info(&msg);

                tracing::trace!("executing message: {:?}", test_msg_info);

                match test_msg_info {
                    // exec external message
                    TestMessageInfo::Ext(msg_info) => {
                        let dst = msg_info.dst.clone();

                        match msg_info.msg_type {
                            TestExternalMessageType::OneToMany => {
                                let mut new_messages =
                                    self.create_one_to_many_int_messages(&mut account_lt, dst)?;
                                created_messages.append(&mut new_messages);
                            }
                            TestExternalMessageType::Swap { route } => {
                                if let Some(message) = self.create_swap_int_message(
                                    &mut account_lt,
                                    dst.clone(),
                                    route,
                                    dst,
                                    None,
                                )? {
                                    created_messages.push(message);
                                }
                            }
                            TestExternalMessageType::Transfer { target_addr } => {
                                let message = self.create_transfer_int_message(
                                    &mut account_lt,
                                    dst,
                                    target_addr,
                                )?;
                                created_messages.push(message);
                            }
                            TestExternalMessageType::Dummy => {
                                // just do nothing
                            }
                            TestExternalMessageType::ToNonExistedAccount => {
                                // external to non existed account will be skipped without creating transaction
                                exec_count -= 1;
                                account_lt -= 1;
                            }
                        }
                    }
                    // exec internal message
                    TestMessageInfo::Int(msg_info) => {
                        let dst = msg_info.dst.clone();

                        match msg_info.msg_type {
                            TestInternalMessageType::OneToMany => {
                                let mut new_messages = self.create_one_to_many_int_messages(
                                    &mut account_lt,
                                    msg_info.src,
                                )?;
                                created_messages.append(&mut new_messages);
                            }
                            TestInternalMessageType::Swap {
                                route,
                                target_addr,
                                step,
                            } => {
                                if let Some(message) = self.create_swap_int_message(
                                    &mut account_lt,
                                    dst,
                                    route,
                                    target_addr,
                                    Some(step),
                                )? {
                                    created_messages.push(message);
                                }
                            }
                            TestInternalMessageType::Transfer => {
                                // transfer completed
                            }
                        }
                    }
                }
            }

            max_lt = max_lt.max(account_lt);
        }

        for test_int_msg in &created_messages {
            self.int_msgs_journal
                .insert(test_int_msg.msg.key(), TestInternalMessageState {
                    info: test_int_msg.info.clone(),
                    _primary_exec_count: 0,
                    _secondary_exec_count: 0,
                });
        }

        Ok(TestExecuteGroupResult {
            exec_count,
            created_messages,
            max_lt,
        })
    }

    fn create_one_to_many_int_messages(
        &mut self,
        account_lt: &mut Lt,
        dst: IntAddr,
    ) -> Result<Vec<TestInternalMessage<V>>> {
        let mut res = vec![];

        for _ in 0..5 {
            if self.one_to_many_counter >= 200 {
                // do not create new one-to-many messages
                // balance finished
                break;
            }

            // create one-to-many internal message
            self.one_to_many_counter += 1;

            let idx = self.next_int_idx;
            self.next_int_idx += 1;

            let msg_type = TestInternalMessageType::OneToMany;

            let body = {
                let mut builder = CellBuilder::new();
                builder.store_u64(idx)?;
                builder.store_u32(msg_type.as_u32())?;
                builder.build()?
            };

            *account_lt += 1;

            tracing::trace!(
                idx, created_lt = %account_lt, src = %dst, %dst, ?msg_type,
                "created one-to-many int message"
            );

            let test_int_msg = self.create_test_int_message(
                idx,
                dst.clone(),
                dst.clone(),
                *account_lt,
                msg_type,
                body,
            )?;

            res.push(test_int_msg);
        }

        Ok(res)
    }

    /// Create next cross swap step internal message or nothing if finished.
    /// * `route` - target route (eg. USDT/BTC)
    /// * `target_addr` - wallet, that init swap and should receive swap result
    /// * `curr_step` - current cross swap step (eg. USDT/USDC, or `Finish`)
    /// * `curr_step_addr` - address for current swap step. It could be a pair addres (eg. USDC/USDT),
    ///   or the `target_address` at the end of cross swap.
    fn create_swap_int_message(
        &mut self,
        account_lt: &mut Lt,
        src: IntAddr,
        route: SwapRoute,
        target_addr: IntAddr,
        curr_step: Option<SwapRoute>,
    ) -> Result<Option<TestInternalMessage<V>>> {
        // if current step is final in cross swap then do not produce any more internal messages
        if curr_step == Some(SwapRoute::Finish) {
            return Ok(None);
        }

        let idx = self.next_int_idx;
        self.next_int_idx += 1;

        let (next_step, pair_addr) =
            Self::calc_next_swap_step_and_dex_pair(&route, curr_step.as_ref(), &self.dex_pairs);

        // Address for next swap step. It could be a pair address (eg. USDC/USDT),
        // or the `target_addr` at the end of cross swap.
        let next_step_addr = if next_step == SwapRoute::Finish {
            target_addr.clone()
        } else {
            pair_addr.unwrap()
        };

        let msg_type = TestInternalMessageType::Swap {
            route,
            target_addr: target_addr.clone(),
            step: next_step,
        };

        let body = {
            let mut builder = CellBuilder::new();
            builder.store_u64(idx)?;
            builder.store_u32(msg_type.as_u32())?;
            builder.store_u32(route as u32)?;
            target_addr.store_into(&mut builder, Cell::empty_context())?;
            builder.store_u32(next_step as u32)?;
            builder.build()?
        };

        *account_lt += 1;

        tracing::trace!(
            idx, created_lt = %account_lt, %src, dst = %next_step_addr, ?msg_type,
            "created swap int message"
        );

        let test_int_msg =
            self.create_test_int_message(idx, src, next_step_addr, *account_lt, msg_type, body)?;

        Ok(Some(test_int_msg))
    }

    pub fn create_random_transfer_int_messages(
        &mut self,
        account_lt: &mut Lt,
        transfers_wallets: &BTreeMap<u8, IntAddr>,
        count: usize,
    ) -> Result<Vec<TestInternalMessage<V>>> {
        let mut res = vec![];

        let count = count - self.rng.gen_range(0..=(count * DEVIATION_PERCENT / 100));
        for _ in 0..count {
            let (src, dst) = self.get_random_src_and_dst_addrs(transfers_wallets);

            let test_int_msg = self.create_transfer_int_message(account_lt, src, dst)?;

            res.push(test_int_msg);
        }

        Ok(res)
    }

    fn create_transfer_int_message(
        &mut self,
        account_lt: &mut Lt,
        src: IntAddr,
        dst: IntAddr,
    ) -> Result<TestInternalMessage<V>> {
        let idx = self.next_int_idx;
        self.next_int_idx += 1;

        let msg_type = TestInternalMessageType::Transfer;

        let body = {
            let mut builder = CellBuilder::new();
            builder.store_u64(idx)?;
            builder.store_u32(msg_type.as_u32())?;
            src.store_into(&mut builder, Cell::empty_context())?;
            dst.store_into(&mut builder, Cell::empty_context())?;
            builder.build()?
        };

        *account_lt += 1;

        tracing::trace!(
            idx, created_lt = %account_lt, %src, %dst, ?msg_type,
            "created transfer int message"
        );

        let test_int_msg =
            self.create_test_int_message(idx, src, dst, *account_lt, msg_type, body)?;

        Ok(test_int_msg)
    }

    fn create_test_int_message(
        &self,
        idx: u64,
        src: IntAddr,
        dst: IntAddr,
        created_lt: Lt,
        msg_type: TestInternalMessageType,
        body: Cell,
    ) -> Result<TestInternalMessage<V>> {
        let info = IntMsgInfo {
            src: src.clone(),
            dst: dst.clone(),
            created_lt,
            ..Default::default()
        };

        let cell = CellBuilder::build_from(Message {
            info: MsgInfo::Int(info.clone()),
            init: None,
            body: body.as_slice()?,
            layout: None,
        })?;

        let hash = *cell.repr_hash();

        let msg = Arc::new((self.create_int_msg_value_func)(info, cell));

        let test_int_msg = TestInternalMessage {
            info: TestInternalMessageInfo {
                idx,
                lt: created_lt,
                hash,
                src,
                dst,
                msg_type,
            },
            msg,
        };

        Ok(test_int_msg)
    }

    fn create_anchor_with_messages(
        &mut self,
        mut messages: Vec<TestExternalMessage>,
    ) -> Arc<MempoolAnchor> {
        messages.shuffle(&mut self.rng);

        let anchor_id = self.last_anchor_id + 4;
        let anchor_ct = self.last_anchor_ct + self.rng.gen_range(1000..=1200);

        let mut externals = vec![];
        for msg in messages {
            self.ext_msgs_journal
                .insert(msg.info.hash, TestExternalMessageState {
                    info: msg.info,
                    _primary_exec_count: 0,
                    _secondary_exec_count: 0,
                });

            externals.push(msg.msg);
        }

        let anchor = Arc::new(MempoolAnchor {
            id: anchor_id,
            prev_id: (self.last_anchor_id > 0).then_some(self.last_anchor_id),
            author: PeerId(Default::default()),
            chain_time: anchor_ct,
            externals,
        });

        self.mempool.insert(anchor_id, anchor.clone());

        self.last_anchor_id = anchor_id;
        self.last_anchor_ct = anchor_ct;

        anchor
    }

    #[tracing::instrument(skip_all)]
    fn init_anchors_cache(
        &self,
        shard_id: &ShardIdent,
        processed_upto: &ProcessedUptoInfoStuff,
    ) -> AnchorsCache {
        let mut anchors_cache = AnchorsCache::default();

        let (processed_to_anchor_id, processed_msgs_offset) = processed_upto
            .get_min_externals_processed_to()
            .unwrap_or_default();

        for (anchor_id, anchor) in self.mempool.range(processed_to_anchor_id..) {
            let msgs_offset = if anchor_id == &processed_to_anchor_id {
                processed_msgs_offset
            } else {
                0
            };
            let our_exts_count = anchor.count_externals_for(shard_id, msgs_offset as usize);
            if anchor.chain_time <= self.last_anchor_ct {
                anchors_cache.insert(anchor.clone(), our_exts_count);
            } else {
                break;
            }
        }

        anchors_cache
    }

    fn create_test_ext_message(
        idx: u64,
        dst: IntAddr,
        msg_type: TestExternalMessageType,
        body: Cell,
    ) -> Result<TestExternalMessage> {
        let info = ExtInMsgInfo {
            dst: dst.clone(),
            ..Default::default()
        };

        let cell = CellBuilder::build_from(Message {
            info: MsgInfo::ExtIn(info.clone()),
            init: None,
            body: body.as_slice()?,
            layout: None,
        })?;

        let hash = *cell.repr_hash();

        let msg = Arc::new(ExternalMessage { cell, info });

        let test_ext_msg = TestExternalMessage {
            info: TestExternalMessageInfo {
                idx,
                hash,
                dst,
                msg_type,
            },
            msg,
        };

        Ok(test_ext_msg)
    }

    fn create_one_to_many_start_message(
        &mut self,
        one_to_many_address: IntAddr,
    ) -> Result<Vec<TestExternalMessage>> {
        let idx = self.next_ext_idx;
        self.next_ext_idx += 1;

        let dst = one_to_many_address;

        let msg_type = TestExternalMessageType::OneToMany;

        let body = {
            let mut builder = CellBuilder::new();
            builder.store_u64(idx)?;
            builder.store_u32(msg_type.as_u32())?;
            builder.build()?
        };

        tracing::trace!(
            idx, %dst, ?msg_type,
            "created one-to-many ext message"
        );

        let test_ext_msg = Self::create_test_ext_message(idx, dst, msg_type, body)?;
        Ok(vec![test_ext_msg])
    }

    fn create_swap_messages(
        &mut self,
        dex_wallets: &BTreeMap<u8, IntAddr>,
        count: usize,
    ) -> Result<Vec<TestExternalMessage>> {
        let mut res = vec![];

        let count = count - self.rng.gen_range(0..=(count * DEVIATION_PERCENT / 100));
        for _ in 0..count {
            let idx = self.next_ext_idx;
            self.next_ext_idx += 1;

            // get random wallet
            let dex_wallets_keys: Vec<_> = dex_wallets.keys().copied().collect();
            let dex_wallet_key = dex_wallets_keys.choose(&mut self.rng).unwrap();
            let dex_wallet = dex_wallets.get(dex_wallet_key).unwrap().clone();

            // get random swap route
            let route_u32: u32 = self.rng.gen_range(1..13);
            let route: SwapRoute = unsafe { std::mem::transmute(route_u32) };

            let msg_type = TestExternalMessageType::Swap { route };

            let body = {
                let mut builder = CellBuilder::new();
                builder.store_u64(idx)?;
                builder.store_u32(msg_type.as_u32())?;
                builder.store_u32(route as u32)?;
                builder.build()?
            };

            tracing::trace!(
                idx, %dex_wallet, ?msg_type,
                "created swap ext message"
            );

            let test_ext_msg = Self::create_test_ext_message(idx, dex_wallet, msg_type, body)?;
            res.push(test_ext_msg);
        }

        Ok(res)
    }

    fn calc_next_swap_step_and_dex_pair(
        route: &SwapRoute,
        curr_step: Option<&SwapRoute>,
        dex_pairs: &BTreeMap<u8, IntAddr>,
    ) -> (SwapRoute, Option<IntAddr>) {
        match (curr_step, route) {
            (None, route) => match route {
                SwapRoute::UsdcNative | SwapRoute::UsdcEth | SwapRoute::UsdcBtc => (
                    SwapRoute::UsdcNative,
                    dex_pairs.get(&DEX_PAIR_USDC_NATIVE).cloned(),
                ),
                SwapRoute::UsdcUsdt
                | SwapRoute::UsdcBnb
                | SwapRoute::UsdtNative
                | SwapRoute::UsdtEth
                | SwapRoute::UsdtBtc
                | SwapRoute::UsdtUsdc => (
                    SwapRoute::UsdcUsdt,
                    dex_pairs.get(&DEX_PAIR_USDC_USDT).cloned(),
                ),
                SwapRoute::UsdtBnb => (
                    SwapRoute::UsdtBnb,
                    dex_pairs.get(&DEX_PAIR_USDT_BNB).cloned(),
                ),
                SwapRoute::NativeEth => (
                    SwapRoute::NativeEth,
                    dex_pairs.get(&DEX_PAIR_NATIVE_ETH).cloned(),
                ),
                SwapRoute::NativeBtc => (
                    SwapRoute::NativeBtc,
                    dex_pairs.get(&DEX_PAIR_NATIVE_BTC).cloned(),
                ),
                SwapRoute::Finish => unreachable!(),
            },
            (Some(SwapRoute::UsdcNative), route) => match route {
                SwapRoute::UsdcNative | SwapRoute::UsdtNative => (SwapRoute::Finish, None),
                SwapRoute::UsdcEth | SwapRoute::UsdtEth => (
                    SwapRoute::NativeEth,
                    dex_pairs.get(&DEX_PAIR_NATIVE_ETH).cloned(),
                ),
                SwapRoute::UsdcBtc | SwapRoute::UsdtBtc => (
                    SwapRoute::NativeBtc,
                    dex_pairs.get(&DEX_PAIR_NATIVE_BTC).cloned(),
                ),
                _ => unreachable!(),
            },
            (Some(SwapRoute::UsdcUsdt), route) => match route {
                SwapRoute::UsdcUsdt | SwapRoute::UsdtUsdc => (SwapRoute::Finish, None),
                SwapRoute::UsdcBnb => (
                    SwapRoute::UsdtBnb,
                    dex_pairs.get(&DEX_PAIR_USDT_BNB).cloned(),
                ),
                SwapRoute::UsdtNative | SwapRoute::UsdtEth | SwapRoute::UsdtBtc => (
                    SwapRoute::UsdcNative,
                    dex_pairs.get(&DEX_PAIR_USDC_NATIVE).cloned(),
                ),
                _ => unreachable!(),
            },
            (Some(SwapRoute::UsdtBnb), route) => match route {
                SwapRoute::UsdtBnb | SwapRoute::UsdcBnb => (SwapRoute::Finish, None),
                _ => unreachable!(),
            },
            (Some(SwapRoute::NativeEth), route) => match route {
                SwapRoute::NativeEth | SwapRoute::UsdcEth | SwapRoute::UsdtEth => {
                    (SwapRoute::Finish, None)
                }
                _ => unreachable!(),
            },
            (Some(SwapRoute::NativeBtc), route) => match route {
                SwapRoute::NativeBtc | SwapRoute::UsdcBtc | SwapRoute::UsdtBtc => {
                    (SwapRoute::Finish, None)
                }
                _ => unreachable!(),
            },
            (
                Some(
                    SwapRoute::Finish
                    | SwapRoute::UsdcEth
                    | SwapRoute::UsdcBtc
                    | SwapRoute::UsdcBnb
                    | SwapRoute::UsdtNative
                    | SwapRoute::UsdtEth
                    | SwapRoute::UsdtBtc
                    | SwapRoute::UsdtUsdc,
                ),
                _,
            ) => unreachable!(),
        }
    }

    fn create_transfer_messages(
        &mut self,
        transfers_wallets: &BTreeMap<u8, IntAddr>,
        count: usize,
    ) -> Result<Vec<TestExternalMessage>> {
        let mut res = vec![];

        let count = count - self.rng.gen_range(0..=(count * DEVIATION_PERCENT / 100));
        for _ in 0..count {
            let idx = self.next_ext_idx;
            self.next_ext_idx += 1;

            let (src_wallet, dst_wallet) = self.get_random_src_and_dst_addrs(transfers_wallets);

            let msg_type = TestExternalMessageType::Transfer {
                target_addr: dst_wallet.clone(),
            };

            let body = {
                let mut builder = CellBuilder::new();
                builder.store_u64(idx)?;
                builder.store_u32(msg_type.as_u32())?;
                dst_wallet.store_into(&mut builder, Cell::empty_context())?;
                builder.build()?
            };

            tracing::trace!(
                idx, %src_wallet, ?msg_type,
                "created transfer ext message"
            );

            let test_ext_msg = Self::create_test_ext_message(idx, src_wallet, msg_type, body)?;
            res.push(test_ext_msg);
        }

        Ok(res)
    }

    /// Returns: (source address, destination address)
    pub fn get_random_src_and_dst_addrs(
        &mut self,
        addrs: &BTreeMap<u8, IntAddr>,
    ) -> (IntAddr, IntAddr) {
        let addrs_keys: Vec<_> = addrs.keys().copied().collect();

        // get random src address
        let src_addr_key = addrs_keys.choose(&mut self.rng).unwrap();
        let src_addr = addrs.get(src_addr_key).unwrap().clone();

        // get random dst address
        let dst_addr_key = addrs_keys.choose(&mut self.rng).unwrap();
        let dst_addr = addrs.get(dst_addr_key).unwrap().clone();

        (src_addr, dst_addr)
    }

    fn create_dummy_messages(
        &mut self,
        target_accounts: &[IntAddr],
        count: usize,
    ) -> Result<Vec<TestExternalMessage>> {
        let mut res = vec![];

        let count = count - self.rng.gen_range(0..=(count * DEVIATION_PERCENT / 100));
        for _ in 0..count {
            let idx = self.next_ext_idx;
            self.next_ext_idx += 1;

            let dst = target_accounts.choose(&mut self.rng).unwrap().clone();

            let msg_type = TestExternalMessageType::Dummy;

            let body = {
                let mut builder = CellBuilder::new();
                builder.store_u64(idx)?;
                builder.store_u32(msg_type.as_u32())?;
                builder.build()?
            };

            tracing::trace!(
                idx, %dst, ?msg_type,
                "created dummy ext message"
            );

            let test_ext_msg = Self::create_test_ext_message(idx, dst, msg_type, body)?;
            res.push(test_ext_msg);
        }

        Ok(res)
    }

    fn create_messages_to_non_existed_account(
        &mut self,
        non_existed_account_address: &IntAddr,
        count: usize,
    ) -> Result<Vec<TestExternalMessage>> {
        let mut res = vec![];

        let count = count - self.rng.gen_range(0..=(count * DEVIATION_PERCENT / 100));
        for _ in 0..count {
            let idx = self.next_ext_idx;
            self.next_ext_idx += 1;

            let dst = non_existed_account_address.clone();

            let msg_type = TestExternalMessageType::ToNonExistedAccount;

            let body = {
                let mut builder = CellBuilder::new();
                builder.store_u64(idx)?;
                builder.store_u32(msg_type.as_u32())?;
                builder.build()?
            };

            tracing::trace!(
                idx, %dst, ?msg_type,
                "created ext message to non-existed account"
            );

            let test_ext_msg = Self::create_test_ext_message(idx, dst, msg_type, body)?;
            res.push(test_ext_msg);
        }

        Ok(res)
    }
}

struct TestExecuteGroupResult<V: InternalMessageValue> {
    exec_count: usize,
    created_messages: Vec<TestInternalMessage<V>>,
    max_lt: Lt,
}

trait GetTestMsgInfo {
    fn get_test_msg_info(&self, msg: &ParsedMessage) -> TestMessageInfo;
}

impl<V, F> GetTestMsgInfo for TestMessageFactory<V, F>
where
    V: InternalMessageValue,
    F: Fn(IntMsgInfo, Cell) -> V,
{
    fn get_test_msg_info(&self, msg: &ParsedMessage) -> TestMessageInfo {
        match msg.info {
            MsgInfo::ExtIn(_) => {
                let msg_state = self.ext_msgs_journal.get(msg.cell.repr_hash()).unwrap();
                TestMessageInfo::Ext(msg_state.info.clone())
            }
            MsgInfo::Int(IntMsgInfo { created_lt, .. }) => {
                let key = QueueKey {
                    lt: created_lt,
                    hash: *msg.cell.repr_hash(),
                };
                let msg_state = self.int_msgs_journal.get(&key).unwrap();
                TestMessageInfo::Int(msg_state.info.clone())
            }
            MsgInfo::ExtOut(_) => {
                unreachable!("ext out message in ordinary messages set: {:?}", msg.info)
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TestMessageInfo {
    Ext(TestExternalMessageInfo),
    Int(TestInternalMessageInfo),
}

struct TestExternalMessage {
    info: TestExternalMessageInfo,
    msg: Arc<ExternalMessage>,
}

struct TestExternalMessageState {
    info: TestExternalMessageInfo,
    _primary_exec_count: usize,
    _secondary_exec_count: usize,
}

#[derive(Clone, PartialEq, Eq)]
struct TestExternalMessageInfo {
    idx: u64,
    hash: HashBytes,
    dst: IntAddr,
    msg_type: TestExternalMessageType,
}

impl std::fmt::Debug for TestExternalMessageInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestExternalMessageInfo")
            .field("idx", &self.idx)
            .field("hash", &self.hash)
            .field("dst", &DebugDisplay(&self.dst))
            .field("msg_type", &self.msg_type)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
enum TestExternalMessageType {
    OneToMany,
    Swap {
        /// Target route (eg. USDT/BTC)
        route: SwapRoute,
    },
    Transfer {
        target_addr: IntAddr,
    },
    Dummy,
    ToNonExistedAccount,
}

impl TestExternalMessageType {
    fn as_u32(&self) -> u32 {
        match self {
            Self::OneToMany => 1,
            Self::Swap { .. } => 2,
            Self::Transfer { .. } => 3,
            Self::Dummy => 4,
            Self::ToNonExistedAccount => 5,
        }
    }
}

impl std::fmt::Debug for TestExternalMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OneToMany => write!(f, "OneToMany"),
            Self::Swap { route } => f.debug_struct("Swap").field("route", &route).finish(),
            Self::Transfer { target_addr } => f
                .debug_struct("Transfer")
                .field("target_addr", &DebugDisplay(&target_addr))
                .finish(),
            Self::Dummy => write!(f, "Dummy"),
            Self::ToNonExistedAccount => write!(f, "ToNonExistedAccount"),
        }
    }
}

/// Pairs: USDC/NATIVE,
/// NATIVE/ETH,
/// NATIVE/BTC,
/// USDC/USDT,
/// USDT/BNB.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
enum SwapRoute {
    Finish = 0,
    UsdcNative,
    UsdcEth,
    UsdcBtc,
    UsdcUsdt,
    UsdcBnb,
    UsdtNative,
    UsdtEth,
    UsdtBtc,
    UsdtUsdc,
    UsdtBnb,
    NativeEth,
    NativeBtc,
}

pub struct TestInternalMessage<V: InternalMessageValue> {
    info: TestInternalMessageInfo,
    pub msg: Arc<V>,
}

struct TestInternalMessageState {
    info: TestInternalMessageInfo,
    _primary_exec_count: usize,
    _secondary_exec_count: usize,
}

#[derive(Clone, PartialEq, Eq)]
struct TestInternalMessageInfo {
    idx: u64,
    lt: Lt,
    hash: HashBytes,
    src: IntAddr,
    dst: IntAddr,
    msg_type: TestInternalMessageType,
}

impl std::fmt::Debug for TestInternalMessageInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestInternalMessageInfo")
            .field("idx", &self.idx)
            .field("lt", &self.lt)
            .field("hash", &self.hash)
            .field("src", &DebugDisplay(&self.src))
            .field("dst", &DebugDisplay(&self.dst))
            .field("msg_type", &self.msg_type)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
enum TestInternalMessageType {
    OneToMany,
    Swap {
        /// Target route (eg. USDT/BTC)
        route: SwapRoute,
        /// Wallet, that init swap and should receive swap result.
        target_addr: IntAddr,
        /// Current cross swap step (eg. USDT/USDC)
        step: SwapRoute,
    },
    Transfer,
}

impl TestInternalMessageType {
    fn as_u32(&self) -> u32 {
        match self {
            Self::OneToMany => 1,
            Self::Swap { .. } => 2,
            Self::Transfer => 3,
        }
    }
}

impl std::fmt::Debug for TestInternalMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OneToMany => write!(f, "OneToMany"),
            Self::Swap {
                target_addr,
                route,
                step,
            } => f
                .debug_struct("Swap")
                .field("route", &route)
                .field("target_addr", &DebugDisplay(&target_addr))
                .field("step", &step)
                .finish(),
            Self::Transfer => write!(f, "Transfer"),
        }
    }
}
