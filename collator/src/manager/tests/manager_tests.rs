use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, CellBuilder, CellFamily, CellSliceRange, HashBytes, Lazy};
use everscale_types::merkle::MerkleUpdate;
use everscale_types::models::{
    Block, BlockExtra, BlockId, BlockIdShort, BlockInfo, BlockRef, BlockchainConfig, ConsensusInfo,
    IntAddr, IntMsgInfo, IntermediateAddr, McBlockExtra, McStateExtra, MsgEnvelope, MsgInfo,
    OutMsg, OutMsgDescr, OutMsgNew, OutMsgQueueUpdates, OwnedMessage, PrevBlockRef,
    ShardDescription, ShardHashes, ShardIdent, ShardStateUnsplit, StdAddr, ValidatorInfo,
    ValueFlow,
};
use parking_lot::Mutex;
use tycho_block_util::archive::WithArchiveData;
use tycho_block_util::block::{BlockStuff, BlockStuffAug};
use tycho_block_util::dict::RelaxedAugDict;
use tycho_block_util::queue::{QueueDiffStuff, QueueKey, QueuePartitionIdx};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_storage::{BlockHandle, NewBlockMeta, StoreStateHint};
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};

use super::{BlockCacheStoreResult, BlockSeqno, CollationManager};
use crate::collator::{
    CollatorStdImplFactory, ForceMasterCollation, ShardDescriptionExt as _, TestInternalMessage,
    TestMessageFactory,
};
use crate::internal_queue::types::{
    DiffStatistics, DiffZone, EnqueuedMessage, InternalMessageValue, PartitionRouter,
    QueueDiffWithMessages,
};
use crate::manager::blocks_cache::BlocksCache;
use crate::manager::types::{CollationSyncState, NextCollationStep};
use crate::manager::McBlockSubgraphExtract;
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{CollatorSyncContext, StateNodeAdapter};
use crate::test_utils::{create_test_queue_adapter, try_init_test_tracing};
use crate::types::processed_upto::{Lt, ProcessedUptoInfoStuff, ProcessedUptoPartitionStuff};
use crate::types::{
    BlockCandidate, BlockStuffForSync, ProcessedTo, ShardDescriptionExt as _,
    ShardDescriptionShort, ShardHashesExt, ShardIdentExt,
};
use crate::validator::{ValidationComplete, ValidationStatus, ValidatorStdImpl};

#[test]
fn test_detect_next_collation_step() {
    let collation_sync_state: Arc<Mutex<CollationSyncState>> = Default::default();

    let mc_shard_id = ShardIdent::MASTERCHAIN;
    let sc_shard_id = ShardIdent::new_full(0);
    let active_shards = vec![mc_shard_id, sc_shard_id];

    let mc_block_min_interval_ms = 2500;

    let mut mc_anchor_ct = 10000;
    let mut sc_anchor_ct = 10000;

    type CM = CollationManager<CollatorStdImplFactory, ValidatorStdImpl>;

    let mut guard = collation_sync_state.lock();

    // first anchor after genesis always exceed mc block interval
    // master collator ready to collate master, but should wait for shards
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "1: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.is_empty()));

    // when shard collator imported the same anchor then we should collate master
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        sc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "2: shard_id: {}, ct: {}, next_step: {:?}",
        sc_shard_id, sc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct));

    CM::renew_mc_block_latest_chain_time(&mut guard, sc_anchor_ct);

    // after master block collation we do not try to detect next step right away
    // we will resume collation attempts that will cause the import of the next anchor
    // next anchor in shard (11000) will not exceed master block interval
    // and we do not have new state from master collator
    // so shard collator will wait for updated master collator state
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        sc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "3: shard_id: {}, ct: {}, next_step: {:?}",
        sc_shard_id, sc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));

    // next anchor in master (11000) will not exceed master block interval as well
    // so it will cause next attempts for master and shard collators
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "4: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&mc_shard_id) && sl.contains(&sc_shard_id))
    );

    // next anchor in shard (12000) will not exceed master block interval
    // so it will cause next attempt for shard
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        sc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "5: shard_id: {}, ct: {}, next_step: {:?}",
        sc_shard_id, sc_anchor_ct, next_step
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&sc_shard_id))
    );

    // next anchor in shard (13000) will exceed master block interval
    // so shard collator should wait for other collators
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        sc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "6: shard_id: {}, ct: {}, next_step: {:?}",
        sc_shard_id, sc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.is_empty()));

    // next anchor in master (12000) will not exceed master block interval
    // so it will cause next attempt for master again
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "7: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&mc_shard_id))
    );

    // next anchor in master (13000) will exceed master block interval
    // master block interval was exceeded in every shard
    // so we can collate next master
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "8: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct));

    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);

    // next anchor in shard (14000) will not exceed master block interval
    // and we do not have new state from master collator
    // so shard collator will wait for updated master collator state
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        sc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "9: shard_id: {}, ct: {}, next_step: {:?}",
        sc_shard_id, sc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));

    // consider that master has unprocessed messages after collation
    // so master collator will force master collation without importing next anchor
    // and we will run master collation right now because shard is already waiting
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::ByUprocessedMessages,
        mc_block_min_interval_ms,
    );
    println!(
        "10: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct));

    CM::renew_mc_block_latest_chain_time(&mut guard, sc_anchor_ct);

    // consider that master has unprocessed messages after collation
    // so master collator will force master collation without importing next anchor
    // then we should wait for shard
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::ByUprocessedMessages,
        mc_block_min_interval_ms,
    );
    println!(
        "11: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.is_empty()));

    // next anchor in shard (15000) will not exceed master block interval
    // but master collation was already forced
    // and we will run master collation right now
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        sc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "12: shard_id: {}, ct: {}, next_step: {:?}",
        sc_shard_id, sc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct));

    CM::renew_mc_block_latest_chain_time(&mut guard, sc_anchor_ct);

    // consider that master has processed all messages
    // next anchor in master (14000) will not exceed master block interval
    // will continue attempts for master
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "13: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&mc_shard_id))
    );

    // next anchor in master (15000) will not exceed master block interval
    // will continue attempts for master
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "14: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&mc_shard_id))
    );

    // consider that shard has upprocessed messages after collation
    // so it will collate 31 blocks until max uncommitted chain length reached
    // then it will force master block collation
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        sc_anchor_ct,
        ForceMasterCollation::ByUncommittedChain,
        mc_block_min_interval_ms,
    );
    println!(
        "15: shard_id: {}, ct: {}, next_step: {:?}",
        sc_shard_id, sc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.is_empty()));

    // next anchor in master (16000) will not exceed master block interval
    // will continue attempts for master
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "16: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&mc_shard_id))
    );

    // next anchor in master (17000) will not exceed master block interval
    // will continue attempts for master
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "17: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&mc_shard_id))
    );

    // next anchor in master (18000) will exceed master block interval
    // master block interval was exceeded in master, master was forced in shard
    // so we can collate next master
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "18: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct));

    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);

    // consider shard has spent a lot wu so it will import many anchors at once
    // so the last imported anchor in shard (21000) will exceed master block interval
    // so shard collator should wait for other collators
    sc_anchor_ct += 1000; // 16
    sc_anchor_ct += 1000; // 17
    sc_anchor_ct += 1000; // 18
    sc_anchor_ct += 1000; // 19
    sc_anchor_ct += 1000; // 20
    sc_anchor_ct += 1000; // 21
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        sc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "19: shard_id: {}, ct: {}, next_step: {:?}",
        sc_shard_id, sc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.is_empty()));

    // next anchor in master (19000) will not exceed master block interval
    // will continue attempts for master
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "20: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&mc_shard_id))
    );

    // next anchor in master (20000) will not exceed master block interval
    // will continue attempts for master
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "21: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&mc_shard_id))
    );

    // next anchor in master (21000) will exceed master block interval
    // master block interval was exceeded in master and shards
    // so we can collate next master
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        mc_anchor_ct,
        ForceMasterCollation::No,
        mc_block_min_interval_ms,
    );
    println!(
        "22: shard_id: {}, ct: {}, next_step: {:?}",
        mc_shard_id, mc_anchor_ct, next_step
    );
    assert!(matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct));

    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
}

#[tokio::test]
async fn test_queue_restore_on_sync() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    //---------
    // set up test stuff

    // queue adapter
    let (mq_adapter, _tmp_dir) = create_test_queue_adapter::<EnqueuedMessage>()
        .await
        .unwrap();
    // test messages factory and executor
    let msgs_factory =
        TestMessageFactory::new(BTreeMap::new(), |info, cell| EnqueuedMessage { info, cell });
    // test state updater
    let state_adapter = Arc::new(TestStateNodeAdapter::default());
    // blocks cache
    let blocks_cache = BlocksCache::new();

    //---------
    // test data
    let shard = ShardIdent::new_full(0);
    let partitions: FastHashSet<QueuePartitionIdx> = [0, 1].into_iter().collect();

    let mut last_sc_block_stuff;
    let mut last_mc_block_stuff;

    // transfers wallets addresses
    let mut transfers_wallets = BTreeMap::<u8, IntAddr>::new();
    for i in 100..110 {
        transfers_wallets.insert(i, IntAddr::Std(StdAddr::new(0, HashBytes([i; 32]))));
    }
    for i in 110..120 {
        transfers_wallets.insert(i, IntAddr::Std(StdAddr::new(-1, HashBytes([i; 32]))));
    }

    //---------
    // test adapter
    let mut test_adapter = TestAdapter {
        state_adapter,
        mq_adapter,
        msgs_factory,
        blocks_cache,

        account_lt: 0,
        transfers_wallets,

        processed_to_stuff: TestProcessedToStuff::new(shard),

        last_sc_block_id: BlockId {
            shard,
            seqno: 0,
            root_hash: HashBytes::default(),
            file_hash: HashBytes::default(),
        },
        last_mc_block_id: BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: 0,
            root_hash: HashBytes::default(),
            file_hash: HashBytes::default(),
        },

        last_sc_blocks: BTreeMap::new(),
        last_mc_blocks: BTreeMap::new(),
    };

    //---------
    // CASE 01: collate 3 shard blocks, 2 master blocks, and commit
    //---------

    // shard block 01
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        1,
        (test_adapter.last_sc_block_id, 0),
        (test_adapter.last_mc_block_id, 0),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_candidate(generated_block_info);

    // shard processed to shard block 01
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&1).unwrap());

    // shard block 02
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        2,
        last_sc_block_stuff.prev_block_info(),
        (test_adapter.last_mc_block_id, 0),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_candidate(generated_block_info);

    // shard processed to shard block 01
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&1).unwrap());

    // shard block 03
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        3,
        last_sc_block_stuff.prev_block_info(),
        (test_adapter.last_mc_block_id, 0),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_candidate(generated_block_info);

    // master processed to shard block 02
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&2).unwrap(),
    );

    // check top shard blocks info for next master block 01
    let next_mc_block_id_short = BlockIdShort {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 1,
    };
    let top_sc_blocks_info = test_adapter
        .blocks_cache
        .get_top_shard_blocks_info_for_mc_block(next_mc_block_id_short)
        .unwrap();
    assert_eq!(top_sc_blocks_info.len(), 1);
    let top_sc_block_decr = &top_sc_blocks_info[0];
    assert_eq!(top_sc_block_decr.block_id, test_adapter.last_sc_block_id);

    // master block 01
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        1,
        (test_adapter.last_mc_block_id, 0),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_candidate(generated_block_info);

    // check top shard blocks of stored master block 01
    let top_sc_blocks = test_adapter
        .blocks_cache
        .get_top_shard_blocks(test_adapter.last_mc_block_id.as_short_id());
    assert!(top_sc_blocks.is_some());
    let top_sc_blocks = top_sc_blocks.unwrap();
    let top_sc_block_seqno = top_sc_blocks.get(&shard);
    assert_eq!(top_sc_block_seqno, Some(&3));

    // master processed to shard block 03, and master block 01
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&3).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&1).unwrap(),
    );

    // check top shard blocks info for next master block 02
    let next_mc_block_id_short = BlockIdShort {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 2,
    };
    let top_sc_blocks_info = test_adapter
        .blocks_cache
        .get_top_shard_blocks_info_for_mc_block(next_mc_block_id_short)
        .unwrap();
    assert!(top_sc_blocks_info.is_empty());

    // master block 02
    let top_sc_block_updated = false;
    let generated_block_info = test_adapter.gen_master_block(
        2,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_candidate(generated_block_info);

    // check top shard blocks of stored master block 02
    let top_sc_blocks = test_adapter
        .blocks_cache
        .get_top_shard_blocks(test_adapter.last_mc_block_id.as_short_id());
    assert!(top_sc_blocks.is_some());
    let top_sc_blocks = top_sc_blocks.unwrap();
    let top_sc_block_seqno = top_sc_blocks.get(&shard);
    assert_eq!(top_sc_block_seqno, Some(&3));

    // commit master block 02 first emulating faster validation for it
    test_adapter
        .blocks_cache
        .store_master_block_validation_result(
            &test_adapter.last_mc_block_id,
            ValidationStatus::Complete(ValidationComplete {
                signatures: Default::default(),
                total_weight: 100,
            }),
        );
    let extracted_subgraph = test_adapter
        .blocks_cache
        .extract_mc_block_subgraph_for_sync(&test_adapter.last_mc_block_id);
    assert!(matches!(
        extracted_subgraph,
        McBlockSubgraphExtract::Extracted(_)
    ));

    test_adapter
        .mq_adapter
        .commit_diff(
            [
                (test_adapter.last_sc_block_id, false),
                (test_adapter.last_mc_block_id, true),
            ]
            .into_iter()
            .collect(),
            &partitions,
        )
        .unwrap();

    // commit master block 01 after 02
    test_adapter
        .blocks_cache
        .store_master_block_validation_result(
            test_adapter.last_mc_blocks.get(&1).unwrap().id(),
            ValidationStatus::Complete(ValidationComplete {
                signatures: Default::default(),
                total_weight: 100,
            }),
        );
    let extracted_subgraph = test_adapter
        .blocks_cache
        .extract_mc_block_subgraph_for_sync(test_adapter.last_mc_blocks.get(&1).unwrap().id());
    assert!(matches!(
        extracted_subgraph,
        McBlockSubgraphExtract::Extracted(_)
    ));

    test_adapter
        .mq_adapter
        .commit_diff(
            [
                (test_adapter.last_sc_block_id, true),
                (*test_adapter.last_mc_blocks.get(&1).unwrap().id(), true),
            ]
            .into_iter()
            .collect(),
            &partitions,
        )
        .unwrap();

    //---------
    // CASE 02: emulate receiving some shard blocks and master blocks from bc with further queue restore on sync
    //          first required shard diff 02 is below last applied 03
    //---------

    // shard processed to shard block 02
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&2).unwrap());
    // shard processed to master block 01
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&1).unwrap());

    // receive shard block 04
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        4,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // master processed to shard block 04, and master block 02
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&4).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&2).unwrap(),
    );

    // check top shard blocks info for next master block 03
    let next_mc_block_id_short = BlockIdShort {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 3,
    };
    let top_sc_blocks_info = test_adapter
        .blocks_cache
        .get_top_shard_blocks_info_for_mc_block(next_mc_block_id_short)
        .unwrap();
    assert_eq!(top_sc_blocks_info.len(), 0); // we do not use received shard blocks to collate master

    // receive master block 03
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        3,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // check top shard blocks of stored master block 03
    let top_sc_blocks = test_adapter
        .blocks_cache
        .get_top_shard_blocks(test_adapter.last_mc_block_id.as_short_id());
    assert!(top_sc_blocks.is_some());
    let top_sc_blocks = top_sc_blocks.unwrap();
    let top_sc_block_seqno = top_sc_blocks.get(&shard);
    assert_eq!(top_sc_block_seqno, Some(&4));

    // shard processed to shard block 02
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&2).unwrap());
    // shard processed to master block 02
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&2).unwrap());

    // receive shard block 05
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        5,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // master processed to shard block 05, and master block 03
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&5).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&3).unwrap(),
    );

    // receive master block 04
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        4,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // shard processed to shard block 02
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&2).unwrap());
    // shard processed to master block 04
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&4).unwrap());

    // receive shard block 06
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        6,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // master processed to shard block 05, and master block 03
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&5).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&3).unwrap(),
    );

    // receive master block 05
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        5,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // restore queue in case of sync
    tracing::trace!("queue restore - case 02");
    let first_applied_mc_block_key = test_adapter
        .last_mc_blocks
        .get(&3)
        .unwrap()
        .id()
        .as_short_id();
    tracing::trace!("first_applied_mc_block_key: {}", first_applied_mc_block_key);
    let last_applied_mc_block_key = test_adapter
        .last_mc_blocks
        .get(&5)
        .unwrap()
        .id()
        .as_short_id();
    tracing::trace!("last_applied_mc_block_key: {}", last_applied_mc_block_key);
    let all_processed_to_by_shards = TestCollationManager::read_all_processed_to_for_mc_block(
        &last_applied_mc_block_key,
        &test_adapter.blocks_cache,
        test_adapter.state_adapter.clone(),
    )
    .await
    .unwrap();
    tracing::trace!(
        "all_processed_to_by_shards: {:?}",
        all_processed_to_by_shards,
    );
    let min_processed_to_by_shards =
        TestCollationManager::find_min_processed_to_by_shards(&all_processed_to_by_shards);
    tracing::trace!(
        "min_processed_to_by_shards: {:?}",
        min_processed_to_by_shards,
    );
    let before_tail_block_ids = test_adapter
        .blocks_cache
        .read_before_tail_ids_of_mc_block(&first_applied_mc_block_key)
        .unwrap();
    tracing::trace!("before_tail_block_ids: {:?}", before_tail_block_ids);
    let queue_diffs_applied_to_mc_block_id = *test_adapter.last_mc_blocks.get(&2).unwrap().id();
    let queue_diffs_applied_to_top_blocks = TestCollationManager::get_top_blocks_seqno(
        &queue_diffs_applied_to_mc_block_id,
        &test_adapter.blocks_cache,
        test_adapter.state_adapter.clone(),
    )
    .await
    .unwrap();
    tracing::trace!(
        "queue_diffs_applied_to_top_blocks: {:?}",
        queue_diffs_applied_to_top_blocks,
    );
    let queue_restore_res = TestCollationManager::restore_queue(
        &test_adapter.blocks_cache,
        test_adapter.state_adapter.clone(),
        test_adapter.mq_adapter.clone(),
        first_applied_mc_block_key.seqno,
        min_processed_to_by_shards,
        before_tail_block_ids,
        queue_diffs_applied_to_top_blocks,
    )
    .await
    .unwrap();

    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&2).unwrap().id()));
    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&3).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&4).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&5).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&6).unwrap().id()));
    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&3).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&4).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&5).unwrap().id()));

    test_adapter
        .blocks_cache
        .remove_next_collated_blocks_from_cache(&queue_restore_res.synced_to_blocks_keys);
    test_adapter.blocks_cache.gc_prev_blocks();

    //---------
    // CASE 03: emulate receiving some shard blocks and master blocks from bc with further queue restore on sync
    //          when first required shard diff 10 will be above last applied 07
    //---------

    // shard processed to shard block 06
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&6).unwrap());
    // shard processed to master block 05
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&5).unwrap());

    // collate shard block 07
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        7,
        test_adapter
            .last_sc_blocks
            .get(&6)
            .unwrap()
            .prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    test_adapter.store_as_candidate(generated_block_info.clone());

    // received shard block 07
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        7,
        test_adapter
            .last_sc_blocks
            .get(&6)
            .unwrap()
            .prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    let store_res = test_adapter.store_as_received(generated_block_info).await;
    last_sc_block_stuff = store_res.block_stuff;

    // clear uncommitted state because of block mismatch
    assert!(store_res.block_mismatch);
    test_adapter.mq_adapter.clear_uncommitted_state().unwrap();

    // shard processed to shard block 07
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&7).unwrap());
    // shard processed to master block 05
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&5).unwrap());

    // received shard block 08
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        8,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // master processed to shard block 08, and master block 05
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&8).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&5).unwrap(),
    );

    // receive master block 06
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        6,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // master processed to shard block 08, and master block 06
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&8).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&5).unwrap(),
    );

    // receive master block 07
    let top_sc_block_updated = false;
    let generated_block_info = test_adapter.gen_master_block(
        7,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // shard processed to shard block 08
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&8).unwrap());
    // shard processed to master block 05
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&5).unwrap());

    // received shard block 09
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        9,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // shard processed to shard block 09
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&9).unwrap());
    // shard processed to master block 06
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&6).unwrap());

    // received shard block 10
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        10,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // master processed to shard block 09, and master block 07
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&9).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&7).unwrap(),
    );

    // receive master block 08
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        8,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // shard processed to shard block 09
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&9).unwrap());
    // shard processed to master block 07
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&7).unwrap());

    // received shard block 11
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        11,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // shard processed to shard block 09
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&9).unwrap());
    // shard processed to master block 08
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&8).unwrap());

    // received shard block 12
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        12,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // master processed to shard block 11, and master block 07
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&11).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&7).unwrap(),
    );

    // receive master block 09
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        9,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // restore queue in case of sync
    tracing::trace!("queue restore - case 03");
    let first_applied_mc_block_key = test_adapter
        .last_mc_blocks
        .get(&6)
        .unwrap()
        .id()
        .as_short_id();
    tracing::trace!("first_applied_mc_block_key: {}", first_applied_mc_block_key);
    let last_applied_mc_block_key = test_adapter
        .last_mc_blocks
        .get(&9)
        .unwrap()
        .id()
        .as_short_id();
    tracing::trace!("last_applied_mc_block_key: {}", last_applied_mc_block_key);
    let all_processed_to_by_shards = TestCollationManager::read_all_processed_to_for_mc_block(
        &last_applied_mc_block_key,
        &test_adapter.blocks_cache,
        test_adapter.state_adapter.clone(),
    )
    .await
    .unwrap();
    tracing::trace!(
        "all_processed_to_by_shards: {:?}",
        all_processed_to_by_shards,
    );
    let min_processed_to_by_shards =
        TestCollationManager::find_min_processed_to_by_shards(&all_processed_to_by_shards);
    tracing::trace!(
        "min_processed_to_by_shards: {:?}",
        min_processed_to_by_shards,
    );
    let before_tail_block_ids = test_adapter
        .blocks_cache
        .read_before_tail_ids_of_mc_block(&first_applied_mc_block_key)
        .unwrap();
    tracing::trace!("before_tail_block_ids: {:?}", before_tail_block_ids);
    let queue_diffs_applied_to_mc_block_id = *test_adapter.last_mc_blocks.get(&5).unwrap().id();
    let queue_diffs_applied_to_top_blocks = TestCollationManager::get_top_blocks_seqno(
        &queue_diffs_applied_to_mc_block_id,
        &test_adapter.blocks_cache,
        test_adapter.state_adapter.clone(),
    )
    .await
    .unwrap();
    tracing::trace!(
        "queue_diffs_applied_to_top_blocks: {:?}",
        queue_diffs_applied_to_top_blocks,
    );
    let queue_restore_res = TestCollationManager::restore_queue(
        &test_adapter.blocks_cache,
        test_adapter.state_adapter.clone(),
        test_adapter.mq_adapter.clone(),
        first_applied_mc_block_key.seqno,
        min_processed_to_by_shards,
        before_tail_block_ids,
        queue_diffs_applied_to_top_blocks,
    )
    .await
    .unwrap();

    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&7).unwrap().id()));
    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&8).unwrap().id()));
    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&9).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&10).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&11).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&12).unwrap().id()));
    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&6).unwrap().id()));
    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&7).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&8).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&9).unwrap().id()));

    test_adapter
        .blocks_cache
        .remove_next_collated_blocks_from_cache(&queue_restore_res.synced_to_blocks_keys);
    test_adapter.blocks_cache.gc_prev_blocks();

    //---------
    // CASE 04: emulate node restart (block cache will be empty)
    //          node collate master block 10 but does not validate and commit it
    //          then it stops
    //          then bc produce master block 11, but node is down and does not receive it
    //          then node starts
    //          then bc produce master block 12, node receives it and run sync
    //          queue will be applied to master block 10 but committed to master block 09
    //          we should apply diffs from master block 10 (and shard block 13) again
    //---------

    // shard processed to shard block 10
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&10).unwrap());
    // shard processed to master block 08
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&8).unwrap());

    // collate shard block 13
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        13,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_candidate(generated_block_info);

    // master processed to shard block 12, and master block 07
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&12).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&7).unwrap(),
    );

    // collate master block 10
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        10,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_candidate(generated_block_info);

    // node was stopped here, blocks cache was dropped
    test_adapter.blocks_cache = BlocksCache::new();

    // shard processed to shard block 10
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&10).unwrap());
    // shard processed to master block 09
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&9).unwrap());

    // create shard block 14 but do not receive it (will not be stored into cache)
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        14,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    last_sc_block_stuff = generated_block_info.block_stuff;
    test_adapter.save_last_info(&last_sc_block_stuff);

    // master processed to shard block 14, and master block 08
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&14).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&8).unwrap(),
    );

    // create master block 11 but do not receive it (will not be stored into cache)
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        11,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    last_mc_block_stuff = generated_block_info.block_stuff;
    test_adapter.save_last_info(&last_mc_block_stuff);

    // check that master block 11 subgraph does not exists
    let extract_res = test_adapter
        .blocks_cache
        .extract_mc_block_subgraph_for_sync(&test_adapter.last_mc_block_id);
    assert!(matches!(
        extract_res,
        McBlockSubgraphExtract::AlreadyExtracted
    ));

    // shard processed to shard block 11
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&11).unwrap());
    // shard processed to master block 11
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&11).unwrap());

    // receive shard block 15
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        15,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // shard processed to shard block 12
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&12).unwrap());
    // shard processed to master block 11
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&11).unwrap());

    // receive shard block 16
    let generated_block_info = test_adapter.gen_shard_block(
        shard,
        16,
        last_sc_block_stuff.prev_block_info(),
        last_mc_block_stuff.prev_block_info(),
        10,
    );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // master processed to shard block 16, and master block 09
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_sc_blocks.get(&16).unwrap(),
    );
    test_adapter.processed_to_stuff.set_processed_to(
        ShardIdent::MASTERCHAIN,
        test_adapter.last_mc_blocks.get(&9).unwrap(),
    );

    // receive master block 12
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter.gen_master_block(
        12,
        last_mc_block_stuff.prev_block_info(),
        &last_sc_block_stuff.data,
        top_sc_block_updated,
        false,
        5,
    );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
        ..
    } = test_adapter.store_as_received(generated_block_info).await;

    // restore queue in case of sync
    tracing::trace!("queue restore - case 04");
    let first_applied_mc_block_key = test_adapter
        .last_mc_blocks
        .get(&12)
        .unwrap()
        .id()
        .as_short_id();
    tracing::trace!("first_applied_mc_block_key: {}", first_applied_mc_block_key);
    let last_applied_mc_block_key = test_adapter
        .last_mc_blocks
        .get(&12)
        .unwrap()
        .id()
        .as_short_id();
    tracing::trace!("last_applied_mc_block_key: {}", last_applied_mc_block_key);
    let all_processed_to_by_shards = TestCollationManager::read_all_processed_to_for_mc_block(
        &last_applied_mc_block_key,
        &test_adapter.blocks_cache,
        test_adapter.state_adapter.clone(),
    )
    .await
    .unwrap();
    tracing::trace!(
        "all_processed_to_by_shards: {:?}",
        all_processed_to_by_shards,
    );
    let min_processed_to_by_shards =
        TestCollationManager::find_min_processed_to_by_shards(&all_processed_to_by_shards);
    tracing::trace!(
        "min_processed_to_by_shards: {:?}",
        min_processed_to_by_shards,
    );
    let before_tail_block_ids = test_adapter
        .blocks_cache
        .read_before_tail_ids_of_mc_block(&first_applied_mc_block_key)
        .unwrap();
    tracing::trace!("before_tail_block_ids: {:?}", before_tail_block_ids);
    let queue_diffs_applied_to_mc_block_id = test_adapter
        .mq_adapter
        .get_last_commited_mc_block_id()
        .unwrap()
        .unwrap();
    assert_eq!(
        queue_diffs_applied_to_mc_block_id,
        *test_adapter.last_mc_blocks.get(&9).unwrap().id()
    );
    let queue_diffs_applied_to_top_blocks = TestCollationManager::get_top_blocks_seqno(
        &queue_diffs_applied_to_mc_block_id,
        &test_adapter.blocks_cache,
        test_adapter.state_adapter.clone(),
    )
    .await
    .unwrap();
    tracing::trace!(
        "queue_diffs_applied_to_top_blocks: {:?}",
        queue_diffs_applied_to_top_blocks,
    );
    let queue_restore_res = TestCollationManager::restore_queue(
        &test_adapter.blocks_cache,
        test_adapter.state_adapter.clone(),
        test_adapter.mq_adapter.clone(),
        first_applied_mc_block_key.seqno,
        min_processed_to_by_shards,
        before_tail_block_ids,
        queue_diffs_applied_to_top_blocks,
    )
    .await
    .unwrap();

    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&12).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&13).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&14).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&15).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_sc_blocks.get(&16).unwrap().id()));
    assert!(!queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&9).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&10).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&11).unwrap().id()));
    assert!(queue_restore_res
        .applied_diffs_ids
        .contains(test_adapter.last_mc_blocks.get(&12).unwrap().id()));

    test_adapter
        .blocks_cache
        .remove_next_collated_blocks_from_cache(&queue_restore_res.synced_to_blocks_keys);
    test_adapter.blocks_cache.gc_prev_blocks();
}

type TestCollationManager = CollationManager<CollatorStdImplFactory, ValidatorStdImpl>;

trait BlockStuffExt {
    fn end_lt(&self) -> Lt;
    fn prev_block_info(&self) -> (BlockId, Lt);
}
impl BlockStuffExt for BlockStuffAug {
    fn end_lt(&self) -> Lt {
        self.load_info().unwrap().end_lt
    }
    fn prev_block_info(&self) -> (BlockId, Lt) {
        (*self.id(), self.end_lt())
    }
}

struct TestProcessedToStuff {
    sc_processed_to_info: BTreeMap<ShardIdent, (BlockSeqno, QueueKey)>,
    mc_processed_to_info: BTreeMap<ShardIdent, (BlockSeqno, QueueKey)>,
}
impl TestProcessedToStuff {
    fn new(shard: ShardIdent) -> Self {
        Self {
            sc_processed_to_info: [
                (shard, (0, QueueKey::min_for_lt(0))),
                (ShardIdent::MASTERCHAIN, (0, QueueKey::min_for_lt(0))),
            ]
            .into_iter()
            .collect(),
            mc_processed_to_info: [
                (shard, (0, QueueKey::min_for_lt(0))),
                (ShardIdent::MASTERCHAIN, (0, QueueKey::min_for_lt(0))),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn set_processed_to(&mut self, shard: ShardIdent, block_stuff: &BlockStuffAug) {
        let value = (
            block_stuff.id().seqno,
            QueueKey::max_for_lt(block_stuff.end_lt()),
        );
        if shard.is_masterchain() {
            self.mc_processed_to_info
                .insert(block_stuff.id().shard, value);
        } else {
            self.sc_processed_to_info
                .insert(block_stuff.id().shard, value);
        }
    }

    fn get_sc_processed_to(&self) -> ProcessedTo {
        self.sc_processed_to_info
            .iter()
            .map(|(shard, (_seqno, key))| (*shard, *key))
            .collect()
    }

    fn get_mc_processed_to(&self) -> ProcessedTo {
        self.mc_processed_to_info
            .iter()
            .map(|(shard, (_seqno, key))| (*shard, *key))
            .collect()
    }

    fn calc_tail_len(&self, shard: &ShardIdent, next_seqno: BlockSeqno) -> u32 {
        let (mc_processed_to_seqno, _) = self.mc_processed_to_info.get(shard).unwrap();
        let (sc_processed_to_seqno, _) = self.mc_processed_to_info.get(shard).unwrap();
        let min_processed_to_seqno = mc_processed_to_seqno.min(sc_processed_to_seqno);
        next_seqno - *min_processed_to_seqno
    }
}

#[derive(Clone)]
struct CreatedBlockInfo<V: InternalMessageValue> {
    state_stuff: ShardStateStuff,
    block_stuff: BlockStuffAug,
    prev_block_id: BlockId,
    queue_diff_stuff: WithArchiveData<QueueDiffStuff>,
    queue_diff_with_msgs: QueueDiffWithMessages<V>,
    ref_by_mc_seqno: BlockSeqno,
}

struct StoreBlockResult {
    block_stuff: BlockStuffAug,
    block_mismatch: bool,
}

struct TestAdapter<V: InternalMessageValue, F>
where
    F: Fn(IntMsgInfo, Cell) -> V,
{
    state_adapter: Arc<TestStateNodeAdapter>,
    mq_adapter: Arc<dyn MessageQueueAdapter<V>>,
    msgs_factory: TestMessageFactory<V, F>,
    blocks_cache: BlocksCache,

    account_lt: Lt,
    transfers_wallets: BTreeMap<u8, IntAddr>,

    processed_to_stuff: TestProcessedToStuff,

    last_sc_block_id: BlockId,
    last_mc_block_id: BlockId,

    last_sc_blocks: BTreeMap<BlockSeqno, BlockStuffAug>,
    last_mc_blocks: BTreeMap<BlockSeqno, BlockStuffAug>,
}

impl<V: InternalMessageValue, F> TestAdapter<V, F>
where
    F: Fn(IntMsgInfo, Cell) -> V,
{
    fn gen_shard_block(
        &mut self,
        shard: ShardIdent,
        seqno: BlockSeqno,
        prev_block_info: (BlockId, Lt),
        ref_mc_block_info: (BlockId, Lt),
        msgs_count: usize,
    ) -> CreatedBlockInfo<V> {
        let (prev_block_id, prev_block_end_lt) = prev_block_info;
        let (ref_mc_block_id, ref_mc_block_end_lt) = ref_mc_block_info;
        let start_lt = self.account_lt;
        let test_messages = self
            .msgs_factory
            .create_random_transfer_int_messages(
                &mut self.account_lt,
                &self.transfers_wallets,
                msgs_count,
            )
            .unwrap();
        let processed_to = self.processed_to_stuff.get_sc_processed_to();
        let queue_diff_with_msgs =
            create_queue_diff_with_msgs(into_messages(test_messages), processed_to.clone());
        self.state_adapter
            .add_shard_block(
                shard,
                seqno,
                start_lt,
                self.account_lt,
                queue_diff_with_msgs,
                self.processed_to_stuff.calc_tail_len(&shard, seqno),
                prev_block_id,
                prev_block_end_lt,
                ref_mc_block_id,
                ref_mc_block_end_lt,
            )
            .unwrap()
    }

    fn gen_master_block(
        &mut self,
        seqno: BlockSeqno,
        prev_block_info: (BlockId, Lt),
        shard_block_stuff: &BlockStuff,
        top_sc_block_updated: bool,
        mc_is_key_block: bool,
        msgs_count: usize,
    ) -> CreatedBlockInfo<V> {
        let (prev_block_id, prev_block_end_lt) = prev_block_info;
        let start_lt = self.account_lt;
        let test_messages = self
            .msgs_factory
            .create_random_transfer_int_messages(
                &mut self.account_lt,
                &self.transfers_wallets,
                msgs_count,
            )
            .unwrap();
        let processed_to = self.processed_to_stuff.get_mc_processed_to();
        let queue_diff_with_msgs =
            create_queue_diff_with_msgs(into_messages(test_messages), processed_to.clone());
        self.state_adapter
            .add_master_block(
                seqno,
                start_lt,
                self.account_lt,
                queue_diff_with_msgs,
                self.processed_to_stuff
                    .calc_tail_len(&ShardIdent::MASTERCHAIN, seqno),
                prev_block_id,
                prev_block_end_lt,
                shard_block_stuff,
                top_sc_block_updated,
                mc_is_key_block,
            )
            .unwrap()
    }

    fn store_as_candidate(
        &mut self,
        generated_block_info: CreatedBlockInfo<V>,
    ) -> StoreBlockResult {
        let CreatedBlockInfo {
            state_stuff,
            block_stuff,
            prev_block_id,
            queue_diff_stuff,
            queue_diff_with_msgs,
            ref_by_mc_seqno,
        } = generated_block_info;
        let mc_top_shard_blocks_info = state_stuff
            .shards()
            .map(|shards| {
                shards
                    .as_vec()
                    .unwrap()
                    .iter()
                    .map(|(shard_id, shard_descr): &(_, ShardDescriptionShort)| {
                        (
                            shard_descr.get_block_id(*shard_id),
                            shard_descr.top_sc_block_updated,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let block_candidate = Box::new(BlockCandidate {
            collated_file_hash: block_stuff.id().file_hash,
            value_flow: block_stuff.block().value_flow.load().unwrap(),
            block: block_stuff.clone(),
            ref_by_mc_seqno,
            is_key_block: state_stuff
                .state_extra()
                .map(|extra| extra.after_key_block)
                .unwrap_or_default(),
            prev_blocks_ids: vec![prev_block_id],
            top_shard_blocks_ids: mc_top_shard_blocks_info
                .iter()
                .map(|(block_id, _updated)| *block_id)
                .collect(),
            collated_data: vec![],
            chain_time: 0,
            processed_to_anchor_id: 0,
            created_by: HashBytes::default(),
            queue_diff_aug: queue_diff_stuff.clone(),
            consensus_info: ConsensusInfo::default(),
            processed_upto: state_stuff
                .state()
                .processed_upto
                .load()
                .unwrap()
                .try_into()
                .unwrap(),
        });
        let statistics = DiffStatistics::from_diff(
            &queue_diff_with_msgs,
            block_stuff.id().shard,
            queue_diff_stuff.diff().min_message,
            queue_diff_stuff.diff().max_message,
        );
        self.mq_adapter
            .apply_diff(
                queue_diff_with_msgs,
                block_stuff.id().as_short_id(),
                queue_diff_stuff.diff_hash(),
                statistics,
                Some(DiffZone::Both),
            )
            .unwrap();
        let BlockCacheStoreResult { block_mismatch, .. } = self
            .blocks_cache
            .store_collated(block_candidate, mc_top_shard_blocks_info)
            .unwrap();

        self.save_last_info(&block_stuff);

        StoreBlockResult {
            block_stuff,
            block_mismatch,
        }
    }

    fn save_last_info(&mut self, block_stuff: &BlockStuffAug) {
        let block_id = *block_stuff.id();
        if block_id.is_masterchain() {
            self.last_mc_block_id = block_id;
            self.last_mc_blocks
                .insert(block_id.seqno, block_stuff.clone());
        } else {
            self.last_sc_block_id = block_id;
            self.last_sc_blocks
                .insert(block_id.seqno, block_stuff.clone());
        }
    }

    async fn store_as_received(
        &mut self,
        generated_block_info: CreatedBlockInfo<V>,
    ) -> StoreBlockResult {
        let CreatedBlockInfo {
            state_stuff,
            block_stuff,
            ..
        } = generated_block_info;
        let processed_upto = state_stuff
            .state()
            .processed_upto
            .load()
            .unwrap()
            .try_into()
            .unwrap();
        let block_mismatch = match self
            .blocks_cache
            .store_received(self.state_adapter.clone(), state_stuff, processed_upto)
            .await
            .unwrap()
        {
            Some(BlockCacheStoreResult { block_mismatch, .. }) => block_mismatch,
            None => false,
        };

        self.save_last_info(&block_stuff);

        StoreBlockResult {
            block_stuff,
            block_mismatch,
        }
    }
}

fn into_messages<V: InternalMessageValue>(
    test_messages: Vec<TestInternalMessage<V>>,
) -> Vec<Arc<V>> {
    test_messages.iter().map(|m| m.msg.clone()).collect()
}

fn create_queue_diff_with_msgs<V: InternalMessageValue>(
    out_msgs: Vec<Arc<V>>,
    processed_to: BTreeMap<ShardIdent, QueueKey>,
) -> QueueDiffWithMessages<V> {
    QueueDiffWithMessages {
        messages: out_msgs
            .iter()
            .map(|msg| (msg.key(), msg.clone()))
            .collect(),
        processed_to,
        partition_router: PartitionRouter::new(),
    }
}

#[allow(clippy::type_complexity)]
struct TestStateNodeAdapter {
    storage: FastDashMap<
        ShardIdent,
        BTreeMap<
            BlockSeqno,
            (
                ShardStateStuff,
                BlockStuffAug,
                WithArchiveData<QueueDiffStuff>,
                BlockSeqno,
            ),
        >,
    >,
    mcstate_tracker: MinRefMcStateTracker,
}

impl Default for TestStateNodeAdapter {
    fn default() -> Self {
        Self {
            storage: Default::default(),
            mcstate_tracker: MinRefMcStateTracker::new(),
        }
    }
}

impl TestStateNodeAdapter {
    #[allow(clippy::too_many_arguments)]
    fn create_and_store_block_and_queue_diff<V: InternalMessageValue>(
        &self,
        shard: ShardIdent,
        seqno: BlockSeqno,
        start_lt: Lt,
        end_lt: Lt,
        queue_diff_with_msgs: QueueDiffWithMessages<V>,
        tail_len: u32,
        prev_block_id: BlockId,
        prev_block_end_lt: Lt,
        master_ref_opt: Option<BlockRef>,
        shards_descr_opt: Option<FastHashMap<ShardIdent, ShardDescription>>,
        mc_is_key_block: bool,
    ) -> Result<CreatedBlockInfo<V>> {
        let prev_block_seqno = seqno.saturating_sub(1);

        //---------
        // calc ref by mc seqno
        let ref_by_mc_seqno = if shard.is_masterchain() {
            seqno
        } else {
            master_ref_opt.as_ref().unwrap().seqno + 1
        };

        //---------
        // prepare queue diff

        // get prev queue diff hash
        let prev_queue_diff_hash = self
            .storage
            .entry(shard)
            .or_default()
            .get(&prev_block_seqno)
            .map(|(_, _, queue_diff_stuff, _)| *queue_diff_stuff.diff_hash())
            .unwrap_or_default();

        // create diff and compute hash
        let (min_message, max_message) = {
            let messages = &queue_diff_with_msgs.messages;
            match messages.first_key_value().zip(messages.last_key_value()) {
                Some(((min, _), (max, _))) => (*min, *max),
                None => (QueueKey::min_for_lt(start_lt), QueueKey::max_for_lt(end_lt)),
            }
        };
        let queue_diff_serialized = QueueDiffStuff::builder(shard, seqno, &prev_queue_diff_hash)
            .with_processed_to(queue_diff_with_msgs.processed_to.clone())
            .with_messages(
                &min_message,
                &max_message,
                queue_diff_with_msgs.messages.keys().map(|k| &k.hash),
            )
            .serialize();
        let queue_diff_hash = *queue_diff_serialized.hash();

        //---------
        // create block stuff

        let mut block_info = BlockInfo {
            shard,
            seqno,
            start_lt,
            end_lt,
            master_ref: master_ref_opt.as_ref().map(Lazy::new).transpose()?,
            ..Default::default()
        };

        let prev_block_ref = BlockRef {
            end_lt: prev_block_end_lt,
            seqno: prev_block_id.seqno,
            root_hash: prev_block_id.root_hash,
            file_hash: prev_block_id.file_hash,
        };
        let prev_ref = PrevBlockRef::Single(prev_block_ref);
        block_info.set_prev_ref(&prev_ref);

        let mc_block_extra_opt = match shards_descr_opt {
            Some(shards_descr) => Some(McBlockExtra {
                shards: ShardHashes::from_shards(shards_descr.iter())?,
                ..Default::default()
            }),
            None => None,
        };

        let out_msg_description = build_out_msg_description(shard, &queue_diff_with_msgs)?;
        let extra = BlockExtra {
            out_msg_description: Lazy::new(&out_msg_description)?,
            custom: mc_block_extra_opt.as_ref().map(Lazy::new).transpose()?,
            ..Default::default()
        };

        let block = Block {
            global_id: 0,
            info: Lazy::new(&block_info).unwrap(),
            value_flow: Lazy::new(&ValueFlow::default()).unwrap(),
            state_update: Lazy::new(&MerkleUpdate::default()).unwrap(),
            out_msg_queue_updates: OutMsgQueueUpdates {
                diff_hash: queue_diff_hash,
                tail_len,
            },
            extra: Lazy::new(&extra).unwrap(),
        };

        let root = CellBuilder::build_from(&block).unwrap();
        let root_hash = *root.repr_hash();
        let data = Boc::encode(&root);
        let data_size = data.len();
        let file_hash = Boc::file_hash_blake(Boc::encode(&root));

        let block_id = BlockId {
            shard: block_info.shard,
            seqno: block_info.seqno,
            root_hash,
            file_hash,
        };

        let block_stuff = BlockStuff::from_block_and_root(&block_id, block, root, data_size);
        let block_stuff = WithArchiveData::new(block_stuff, data);

        //---------
        // create queue diff stuff
        let queue_diff_stuff = queue_diff_serialized.build(&block_id);

        //---------
        // create state stuff
        let mc_state_extra_opt = mc_block_extra_opt.map(|extra| McStateExtra {
            shards: extra.shards.clone(),
            after_key_block: mc_is_key_block,
            config: BlockchainConfig::new_empty(HashBytes::default()),
            validator_info: ValidatorInfo {
                catchain_seqno: 0,
                validator_list_hash_short: 0,
                nx_cc_updated: false,
            },
            consensus_info: Default::default(),
            global_balance: Default::default(),
            prev_blocks: Default::default(),
            last_key_block: None,
            block_create_stats: None,
        });
        let shard_state = ShardStateUnsplit {
            shard_ident: shard,
            seqno,
            min_ref_mc_seqno: 0,
            custom: mc_state_extra_opt.as_ref().map(Lazy::new).transpose()?,
            processed_upto: Lazy::new(
                &(ProcessedUptoInfoStuff {
                    partitions: [
                        (0, ProcessedUptoPartitionStuff::default()),
                        (1, ProcessedUptoPartitionStuff::default()),
                    ]
                    .into_iter()
                    .collect(),
                    msgs_exec_params: None,
                }
                .try_into()?),
            )?,
            ..Default::default()
        };

        let state_stuff = ShardStateStuff::from_state_and_root(
            &block_id,
            Box::new(shard_state),
            Cell::default(),
            &self.mcstate_tracker,
        )
        .unwrap();

        //---------
        // store block and queue diff
        self.storage.entry(shard).or_default().insert(
            seqno,
            (
                state_stuff.clone(),
                block_stuff.clone(),
                queue_diff_stuff.clone(),
                ref_by_mc_seqno,
            ),
        );

        Ok(CreatedBlockInfo {
            state_stuff,
            block_stuff,
            prev_block_id,
            queue_diff_stuff,
            queue_diff_with_msgs,
            ref_by_mc_seqno,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn add_shard_block<V: InternalMessageValue>(
        &self,
        shard: ShardIdent,
        seqno: BlockSeqno,
        start_lt: Lt,
        end_lt: Lt,
        queue_diff_with_msgs: QueueDiffWithMessages<V>,
        tail_len: u32,
        prev_block_id: BlockId,
        prev_block_end_lt: Lt,
        ref_mc_block_id: BlockId,
        ref_mc_block_end_lt: Lt,
    ) -> Result<CreatedBlockInfo<V>> {
        let master_ref = BlockRef {
            end_lt: ref_mc_block_end_lt,
            seqno: ref_mc_block_id.seqno,
            root_hash: ref_mc_block_id.root_hash,
            file_hash: ref_mc_block_id.file_hash,
        };

        self.create_and_store_block_and_queue_diff(
            shard,
            seqno,
            start_lt,
            end_lt,
            queue_diff_with_msgs,
            tail_len,
            prev_block_id,
            prev_block_end_lt,
            Some(master_ref),
            None,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn add_master_block<V: InternalMessageValue>(
        &self,
        seqno: BlockSeqno,
        start_lt: Lt,
        end_lt: Lt,
        queue_diff_with_msgs: QueueDiffWithMessages<V>,
        tail_len: u32,
        prev_block_id: BlockId,
        prev_block_end_lt: Lt,
        top_shard_block: &BlockStuff,
        top_sc_block_updated: bool,
        mc_is_key_block: bool,
    ) -> Result<CreatedBlockInfo<V>> {
        let shard = ShardIdent::MASTERCHAIN;

        // create shards description
        let shard_block_id = *top_shard_block.id();
        let mut shard_descr = ShardDescription::from_block_info(
            shard_block_id,
            top_shard_block.load_info()?,
            0,
            &ValueFlow::default(),
        );
        shard_descr.reg_mc_seqno = seqno;
        shard_descr.top_sc_block_updated = top_sc_block_updated;
        let shards_descr = [(shard_block_id.shard, shard_descr)].into_iter().collect();

        self.create_and_store_block_and_queue_diff(
            shard,
            seqno,
            start_lt,
            end_lt,
            queue_diff_with_msgs,
            tail_len,
            prev_block_id,
            prev_block_end_lt,
            None,
            Some(shards_descr),
            mc_is_key_block,
        )
    }
}

#[async_trait]
impl StateNodeAdapter for TestStateNodeAdapter {
    fn load_init_block_id(&self) -> Option<BlockId> {
        Some(BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: 0,
            root_hash: HashBytes::default(),
            file_hash: HashBytes::default(),
        })
    }

    async fn get_ref_by_mc_seqno(&self, block_id: &BlockId) -> Result<Option<BlockSeqno>> {
        let res = self.storage.get(&block_id.shard).and_then(|s| {
            s.get(&block_id.seqno)
                .map(|(_, _, _, ref_by_mc_seqno)| *ref_by_mc_seqno)
        });
        Ok(res)
    }

    async fn load_block(&self, block_id: &BlockId) -> Result<Option<BlockStuff>> {
        let res = self.storage.get(&block_id.shard).and_then(|s| {
            s.get(&block_id.seqno)
                .map(|(_, block_stuff, _, _)| &block_stuff.data)
                .cloned()
        });
        Ok(res)
    }

    async fn load_diff(&self, block_id: &BlockId) -> Result<Option<QueueDiffStuff>> {
        let res = self.storage.get(&block_id.shard).and_then(|s| {
            s.get(&block_id.seqno)
                .map(|(_, _, queue_diff_stuff, _)| &queue_diff_stuff.data)
                .cloned()
        });
        Ok(res)
    }

    async fn load_state(&self, block_id: &BlockId) -> Result<ShardStateStuff> {
        let res = self.storage.get(&block_id.shard).and_then(|s| {
            s.get(&block_id.seqno)
                .map(|(state_stuff, _, _, _)| state_stuff)
                .cloned()
        });
        res.ok_or_else(|| anyhow!("state not found for mc block {}", block_id.as_short_id()))
    }

    fn load_last_applied_mc_block_id(&self) -> Result<BlockId> {
        unreachable!()
    }
    async fn store_state_root(
        &self,
        _block_id: &BlockId,
        _meta: NewBlockMeta,
        _state_root: Cell,
        _hint: StoreStateHint,
    ) -> Result<bool> {
        unreachable!()
    }
    async fn load_block_by_handle(&self, _handle: &BlockHandle) -> Result<Option<BlockStuff>> {
        unreachable!()
    }
    async fn load_block_handle(&self, _block_id: &BlockId) -> Result<Option<BlockHandle>> {
        unreachable!()
    }
    fn accept_block(&self, _block: Arc<BlockStuffForSync>) -> Result<()> {
        unreachable!()
    }
    async fn wait_for_block(&self, _block_id: &BlockId) -> Option<Result<BlockStuffAug>> {
        unreachable!()
    }
    async fn wait_for_block_next(&self, _block_id: &BlockId) -> Option<Result<BlockStuffAug>> {
        unreachable!()
    }
    async fn handle_state(&self, _state: &ShardStateStuff) -> Result<()> {
        unreachable!()
    }
    fn set_sync_context(&self, _sync_context: CollatorSyncContext) {
        unreachable!()
    }
}

fn build_out_msg_description<V: InternalMessageValue>(
    curr_shard_id: ShardIdent,
    queue_diff_with_msgs: &QueueDiffWithMessages<V>,
) -> Result<OutMsgDescr> {
    let mut out_msgs = BTreeMap::new();

    for msg in queue_diff_with_msgs.messages.values() {
        let IntMsgInfo { fwd_fee, dst, .. } = msg.info();
        let dst_prefix = dst.prefix();
        let dst_workchain = dst.workchain();
        let dst_in_current_shard = curr_shard_id.contains_prefix(dst_workchain, dst_prefix);

        let out_msg = OutMsg::New(OutMsgNew {
            out_msg_envelope: Lazy::new(&MsgEnvelope {
                cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                next_addr: if dst_in_current_shard {
                    IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                } else {
                    IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                },
                fwd_fee_remaining: *fwd_fee,
                message: Lazy::new(&OwnedMessage {
                    info: MsgInfo::Int(msg.info().clone()),
                    init: None,
                    body: (msg.cell().clone(), CellSliceRange::default()),
                    layout: None,
                })?,
            })?,
            transaction: Lazy::from_raw(Cell::empty_cell())?,
        });

        out_msgs.insert(
            *msg.cell().repr_hash(),
            (out_msg.compute_exported_value()?, Lazy::new(&out_msg)?),
        );
    }

    let res = RelaxedAugDict::try_from_sorted_iter_lazy(
        out_msgs
            .iter()
            .map(|(msg_id, (exported_value, out_msg))| (msg_id, exported_value, out_msg)),
    )?
    .build()?;

    Ok(res)
}
