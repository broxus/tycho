use std::collections::BTreeMap;
use std::sync::Arc;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use parking_lot::Mutex;
use tracing::level_filters::LevelFilter;
use tycho_block_util::archive::WithArchiveData;
use tycho_block_util::block::{BlockStuff, BlockStuffAug};
use tycho_block_util::dict::RelaxedAugDict;
use tycho_block_util::queue::{QueueDiffStuff, QueueKey, QueuePartitionIdx};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_core::storage::{BlockHandle, NewBlockMeta, StoreStateHint};
use tycho_types::boc::Boc;
use tycho_types::cell::{Cell, CellBuilder, CellFamily, HashBytes, Lazy};
use tycho_types::merkle::MerkleUpdate;
use tycho_types::models::{
    Block, BlockExtra, BlockId, BlockIdShort, BlockInfo, BlockRef, BlockchainConfig,
    ConsensusInfo, GlobalCapabilities, GlobalCapability, IntAddr, IntMsgInfo,
    IntermediateAddr, McBlockExtra, McStateExtra, MsgEnvelope, MsgInfo, OutMsg,
    OutMsgDescr, OutMsgNew, OutMsgQueueUpdates, OwnedMessage, PrevBlockRef,
    ShardDescription, ShardHashes, ShardIdent, ShardStateUnsplit, StdAddr, ValidatorInfo,
    ValueFlow,
};
use tycho_util::{FastDashMap, FastHashMap, FastHashSet};
use super::{
    BlockCacheStoreResult, BlockSeqno, CollationManager, DetectNextCollationStepContext,
};
use crate::collator::{
    CollatorStdImplFactory, ForceMasterCollation, ShardDescriptionExt as _,
    TestInternalMessage, TestMessageFactory,
};
use crate::internal_queue::types::{
    DiffStatistics, DiffZone, EnqueuedMessage, InternalMessageValue, PartitionRouter,
    QueueDiffWithMessages,
};
use crate::manager::blocks_cache::BlocksCache;
use crate::manager::types::{CollationSyncState, NextCollationStep};
use crate::manager::{
    CollatedBlockInfo, CollationStatus, GlobalCapabilitiesExt, McBlockSubgraphExtract,
};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{CollatorSyncContext, StateNodeAdapter};
use crate::test_utils::{create_test_queue_adapter, try_init_test_tracing};
use crate::types::processed_upto::{
    InternalsProcessedUptoStuff, Lt, ProcessedUptoInfoStuff, ProcessedUptoPartitionStuff,
    find_min_processed_to_by_shards,
};
use crate::types::{
    BlockCandidate, BlockStuffForSync, ProcessedTo, ShardDescriptionShort,
    ShardDescriptionShortExt as _, ShardHashesExt, ShardIdentExt,
};
use crate::validator::{ValidationComplete, ValidationStatus, ValidatorStdImpl};
#[test]
fn test_detect_next_collation_step() {
    try_init_test_tracing(LevelFilter::TRACE);
    let collation_sync_state: Arc<Mutex<CollationSyncState>> = Default::default();
    let mc_shard_id = ShardIdent::MASTERCHAIN;
    let sc_shard_id = ShardIdent::new_full(0);
    let active_shards = vec![mc_shard_id, sc_shard_id];
    let mc_block_min_interval_ms = 2500;
    let mc_block_max_interval_ms = 2500;
    let mut mc_anchor_ct = 10000;
    let mut sc_anchor_ct = 10000;
    let mut mc_block_seqno = 0;
    type CM = CollationManager<CollatorStdImplFactory, ValidatorStdImpl>;
    let mut guard = collation_sync_state.lock();
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "10.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "10.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    CM::renew_mc_block_latest_chain_time(&mut guard, sc_anchor_ct);
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "20.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "20.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if ! sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "20.3: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, false)),
        ),
    );
    let collation_status = get_collation_status(&mut guard, &sc_shard_id);
    println!(
        "20.4: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, status: {collation_status:?}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    let collation_status = get_collation_status(&mut guard, &mc_shard_id);
    println!(
        "20.5: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, status: {collation_status:?}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id))
    );
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "20.6: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct)
    );
    mc_block_seqno += 1;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    sc_anchor_ct += 1000;
    println!("sc_anchor_ct: {sc_anchor_ct:?} mc_anchor_ct: {mc_anchor_ct:?}");
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "30.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "30.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    println!("sc_anchor_ct: {sc_anchor_ct:?} mc_anchor_ct: {mc_anchor_ct:?}");
    CM::renew_mc_block_latest_chain_time(&mut guard, sc_anchor_ct);
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno - 1, false)),
        ),
    );
    println!(
        "40.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "40.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    CM::renew_mc_block_latest_chain_time(&mut guard, sc_anchor_ct);
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "50.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "50.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "50.3: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::ByUncommittedChain,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, false)),
        ),
    );
    println!(
        "50.4: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id))
    );
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "50.5: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id))
    );
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "50.6: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id))
    );
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "50.7: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct)
    );
    mc_block_seqno += 1;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    sc_anchor_ct += 1000;
    sc_anchor_ct += 1000;
    sc_anchor_ct += 1000;
    sc_anchor_ct += 1000;
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "60.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "60.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id))
    );
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "60.3: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id))
    );
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "60.4: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 25000;
    sc_anchor_ct = 25000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "70.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "70.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "70.3: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if ! sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "70.4: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "70.5: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id) && ! sl.contains(& sc_shard_id))
    );
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "70.6: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct)
    );
    mc_block_seqno += 1;
    assert_eq!(mc_anchor_ct, sc_anchor_ct);
    mc_anchor_ct = 30000;
    sc_anchor_ct = 30000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "75.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "75.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    mc_anchor_ct += 2000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "75.3: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "75.4: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if ! sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "75.5: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    assert_eq!(mc_anchor_ct, sc_anchor_ct);
    mc_anchor_ct = 50000;
    sc_anchor_ct = 50000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "90.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    assert_eq!(guard.mc_forced_by_no_pending_msgs_on_ct, Some(sc_anchor_ct));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "90.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if ! sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    sc_anchor_ct += 2000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "90.3: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id) && ! sl.contains(& sc_shard_id))
    );
    mc_anchor_ct += 2000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "90.4: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct)
    );
    mc_block_seqno += 1;
    assert_eq!(sc_anchor_ct, mc_anchor_ct);
    mc_anchor_ct = 60000;
    sc_anchor_ct = 60000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, false)),
        ),
    );
    println!(
        "100.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    assert_eq!(guard.mc_forced_by_no_pending_msgs_on_ct, Some(sc_anchor_ct));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "100.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if ! sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    sc_anchor_ct += 3000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "100.3: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id) && ! sl.contains(& sc_shard_id))
    );
    mc_anchor_ct += 2000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "90.4: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct)
    );
    mc_block_seqno += 1;
    assert_eq!(sc_anchor_ct, mc_anchor_ct);
    let mc_block_min_interval_ms = 1000;
    mc_anchor_ct = 70000;
    sc_anchor_ct = 70000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "110.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "110.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 80000;
    sc_anchor_ct = 80000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "120.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "120.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(s) if s.contains(&
        mc_shard_id) && s.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "120.3: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "120.4: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 110000;
    sc_anchor_ct = 110000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "150.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "150.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(sl) if sl.contains(&
        mc_shard_id) && sl.contains(& sc_shard_id))
    );
    mc_anchor_ct += 2000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "150.3: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "150.4: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 120000;
    sc_anchor_ct = 120000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "160.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "160.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == mc_anchor_ct)
    );
    mc_block_seqno += 1;
    assert_eq!(mc_anchor_ct, sc_anchor_ct);
    mc_anchor_ct = 130000;
    sc_anchor_ct = 130000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "170.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "170.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    assert_eq!(mc_anchor_ct, sc_anchor_ct);
    mc_anchor_ct = 150000;
    sc_anchor_ct = 150000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "190.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "190.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 160000;
    sc_anchor_ct = 160000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "200.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "200.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 170000;
    sc_anchor_ct = 170000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "210.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "210.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 180000;
    sc_anchor_ct = 180000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "220.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "220.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 190000;
    sc_anchor_ct = 190000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "230.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "230.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    let mc_block_min_interval_ms = 1100;
    let mc_block_max_interval_ms = 4000;
    mc_anchor_ct = 200000;
    sc_anchor_ct = 200000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    mc_anchor_ct += 2000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "240.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "240.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(s) if s.contains(&
        mc_shard_id) && s.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "240.3: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "240.4: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 210000;
    sc_anchor_ct = 210000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "250.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    assert_eq!(guard.mc_forced_by_no_pending_msgs_on_ct, Some(sc_anchor_ct));
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "250.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 220000;
    sc_anchor_ct = 220000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByUnprocessedMessages,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "260.1: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForShardStatus));
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "260.2: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 230000;
    sc_anchor_ct = 230000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, false)),
        ),
    );
    println!(
        "270.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "270.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(s) if ! s.contains(&
        mc_shard_id) && s.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, false)),
        ),
    );
    println!(
        "270.3: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(s) if s.contains(&
        mc_shard_id) && s.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, true)),
        ),
    );
    println!(
        "270.4: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "270.5: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_block_seqno += 1;
    mc_anchor_ct = 240000;
    sc_anchor_ct = 240000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, false)),
        ),
    );
    println!(
        "280.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "280.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(s) if ! s.contains(&
        mc_shard_id) && s.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, false)),
        ),
    );
    println!(
        "280.3: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::ResumeAttemptsIn(s) if s.contains(&
        mc_shard_id) && s.contains(& sc_shard_id))
    );
    sc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::NoPendingMessagesAfterShardBlocks,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            Some(CollatedBlockInfo::new(mc_block_seqno, false)),
        ),
    );
    println!(
        "280.4: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    mc_anchor_ct += 1000;
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::No,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "280.5: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    mc_anchor_ct = 250000;
    sc_anchor_ct = 250000;
    CM::renew_mc_block_latest_chain_time(&mut guard, mc_anchor_ct);
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        sc_shard_id,
        DetectNextCollationStepContext::new(
            sc_anchor_ct,
            ForceMasterCollation::ByAnchorImportSkipped,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "290.1: shard_id: {sc_shard_id}, ct: {sc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(matches!(next_step, NextCollationStep::WaitForMasterStatus));
    let next_step = CM::detect_next_collation_step(
        &mut guard,
        active_shards.clone(),
        mc_shard_id,
        DetectNextCollationStepContext::new(
            mc_anchor_ct,
            ForceMasterCollation::ByAnchorImportSkipped,
            mc_block_min_interval_ms,
            mc_block_max_interval_ms,
            None,
        ),
    );
    println!(
        "290.2: shard_id: {mc_shard_id}, ct: {mc_anchor_ct}, next_step: {next_step:?}"
    );
    assert!(
        matches!(next_step, NextCollationStep::CollateMaster(ct) if ct == sc_anchor_ct)
    );
    assert_eq!(sc_anchor_ct, mc_anchor_ct);
}
fn get_collation_status(
    guard: &mut CollationSyncState,
    shard_id: &ShardIdent,
) -> CollationStatus {
    guard.states.get(shard_id).unwrap().status
}
#[tokio::test]
async fn test_queue_restore_on_sync() {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(test_queue_restore_on_sync)),
        file!(),
        1880u32,
    );
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);
    let (mq_adapter, _tmp_dir) = {
        __guard.end_section(1888u32);
        let __result = create_test_queue_adapter::<EnqueuedMessage>().await;
        __guard.start_section(1888u32);
        __result
    }
        .unwrap();
    let msgs_factory = TestMessageFactory::new(
        BTreeMap::new(),
        |info, cell| EnqueuedMessage { info, cell },
    );
    let state_adapter = Arc::new(TestStateNodeAdapter::default());
    let blocks_cache = BlocksCache::new();
    let shard = ShardIdent::new_full(0);
    let partitions = FastHashSet::from_iter([
        QueuePartitionIdx(0),
        QueuePartitionIdx(1),
    ]);
    let mut last_sc_block_stuff;
    let mut last_mc_block_stuff;
    let mut transfers_wallets = BTreeMap::<u8, IntAddr>::new();
    for i in 100..110 {
        __guard.checkpoint(1908u32);
        transfers_wallets.insert(i, IntAddr::Std(StdAddr::new(0, HashBytes([i; 32]))));
    }
    for i in 110..120 {
        __guard.checkpoint(1911u32);
        transfers_wallets.insert(i, IntAddr::Std(StdAddr::new(-1, HashBytes([i; 32]))));
    }
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
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            1,
            (test_adapter.last_sc_block_id, 0),
            (test_adapter.last_mc_block_id, 0),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = test_adapter.store_as_candidate(generated_block_info);
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&1).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            2,
            last_sc_block_stuff.prev_block_info(),
            (test_adapter.last_mc_block_id, 0),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = test_adapter.store_as_candidate(generated_block_info);
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&1).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            3,
            last_sc_block_stuff.prev_block_info(),
            (test_adapter.last_mc_block_id, 0),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = test_adapter.store_as_candidate(generated_block_info);
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&2).unwrap(),
        );
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
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            1,
            (test_adapter.last_mc_block_id, 0),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = test_adapter.store_as_candidate(generated_block_info);
    let top_sc_blocks = test_adapter
        .blocks_cache
        .get_top_shard_blocks(test_adapter.last_mc_block_id.as_short_id());
    assert!(top_sc_blocks.is_some());
    let top_sc_blocks = top_sc_blocks.unwrap();
    let top_sc_block_seqno = top_sc_blocks.get(&shard);
    assert_eq!(top_sc_block_seqno, Some(& 3));
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&3).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&1).unwrap(),
        );
    let next_mc_block_id_short = BlockIdShort {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 2,
    };
    let top_sc_blocks_info = test_adapter
        .blocks_cache
        .get_top_shard_blocks_info_for_mc_block(next_mc_block_id_short)
        .unwrap();
    assert!(top_sc_blocks_info.is_empty());
    let top_sc_block_updated = false;
    let generated_block_info = test_adapter
        .gen_master_block(
            2,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = test_adapter.store_as_candidate(generated_block_info);
    let top_sc_blocks = test_adapter
        .blocks_cache
        .get_top_shard_blocks(test_adapter.last_mc_block_id.as_short_id());
    assert!(top_sc_blocks.is_some());
    let top_sc_blocks = top_sc_blocks.unwrap();
    let top_sc_block_seqno = top_sc_blocks.get(&shard);
    assert_eq!(top_sc_block_seqno, Some(& 3));
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
    assert!(matches!(extracted_subgraph, McBlockSubgraphExtract::Extracted(_)));
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
        .extract_mc_block_subgraph_for_sync(
            test_adapter.last_mc_blocks.get(&1).unwrap().id(),
        );
    assert!(matches!(extracted_subgraph, McBlockSubgraphExtract::Extracted(_)));
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
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&2).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&1).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            4,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2173u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2173u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&4).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&2).unwrap(),
        );
    let next_mc_block_id_short = BlockIdShort {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 3,
    };
    let top_sc_blocks_info = test_adapter
        .blocks_cache
        .get_top_shard_blocks_info_for_mc_block(next_mc_block_id_short)
        .unwrap();
    assert_eq!(top_sc_blocks_info.len(), 0);
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            3,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = {
        __guard.end_section(2209u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2209u32);
        __result
    };
    let top_sc_blocks = test_adapter
        .blocks_cache
        .get_top_shard_blocks(test_adapter.last_mc_block_id.as_short_id());
    assert!(top_sc_blocks.is_some());
    let top_sc_blocks = top_sc_blocks.unwrap();
    let top_sc_block_seqno = top_sc_blocks.get(&shard);
    assert_eq!(top_sc_block_seqno, Some(& 4));
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&2).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&2).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            5,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2240u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2240u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&5).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&3).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            4,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = {
        __guard.end_section(2265u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2265u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&2).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&4).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            6,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2287u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2287u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&5).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&3).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            5,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = {
        __guard.end_section(2312u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2312u32);
        __result
    };
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
    let all_shards_processed_to_by_partitions = {
        __guard.end_section(2336u32);
        let __result = TestCollationManager::get_all_shards_processed_to_by_partitions_for_mc_block(
                &last_applied_mc_block_key,
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
            )
            .await;
        __guard.start_section(2336u32);
        __result
    }
        .unwrap();
    tracing::trace!(
        "all_shards_processed_to_by_partitions: {:?}",
        all_shards_processed_to_by_partitions,
    );
    let min_processed_to_by_shards = find_min_processed_to_by_shards(
        &all_shards_processed_to_by_partitions,
    );
    tracing::trace!("min_processed_to_by_shards: {:?}", min_processed_to_by_shards,);
    let before_tail_block_ids = test_adapter
        .blocks_cache
        .read_before_tail_ids_of_mc_block(&first_applied_mc_block_key)
        .unwrap();
    tracing::trace!("before_tail_block_ids: {:?}", before_tail_block_ids);
    let queue_diffs_applied_to_mc_block_id = *test_adapter
        .last_mc_blocks
        .get(&2)
        .unwrap()
        .id();
    let queue_diffs_applied_to_top_blocks = {
        __guard.end_section(2359u32);
        let __result = TestCollationManager::get_top_blocks_seqno(
                &queue_diffs_applied_to_mc_block_id,
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
            )
            .await;
        __guard.start_section(2359u32);
        __result
    }
        .unwrap();
    tracing::trace!(
        "queue_diffs_applied_to_top_blocks: {:?}", queue_diffs_applied_to_top_blocks,
    );
    let queue_restore_res = {
        __guard.end_section(2374u32);
        let __result = TestCollationManager::restore_queue(
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
                test_adapter.mq_adapter.clone(),
                first_applied_mc_block_key.seqno,
                min_processed_to_by_shards,
                before_tail_block_ids,
                queue_diffs_applied_to_top_blocks,
            )
            .await;
        __guard.start_section(2374u32);
        __result
    }
        .unwrap();
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        2).unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        3).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(& 4)
        .unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(& 5)
        .unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(& 6)
        .unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        3).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(& 4)
        .unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(& 5)
        .unwrap().id())
    );
    test_adapter
        .blocks_cache
        .remove_next_collated_blocks_from_cache(
            &queue_restore_res.synced_to_blocks_keys,
        );
    test_adapter.blocks_cache.gc_prev_blocks();
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&6).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&5).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            7,
            test_adapter.last_sc_blocks.get(&6).unwrap().prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    test_adapter.store_as_candidate(generated_block_info.clone());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            7,
            test_adapter.last_sc_blocks.get(&6).unwrap().prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    let store_res = {
        __guard.end_section(2463u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2463u32);
        __result
    };
    last_sc_block_stuff = store_res.block_stuff;
    assert!(store_res.block_mismatch);
    test_adapter.mq_adapter.clear_uncommitted_state(&[]).unwrap();
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&7).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&5).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            8,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2493u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2493u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&8).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&5).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            6,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = {
        __guard.end_section(2518u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2518u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&8).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&5).unwrap(),
        );
    let top_sc_block_updated = false;
    let generated_block_info = test_adapter
        .gen_master_block(
            7,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = {
        __guard.end_section(2543u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2543u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&8).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&5).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            9,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2565u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2565u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&9).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&6).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            10,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2587u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2587u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&9).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&7).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            8,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = {
        __guard.end_section(2612u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2612u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&9).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&7).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            11,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2634u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2634u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&9).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&8).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            12,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2656u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2656u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&11).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&7).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            9,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = {
        __guard.end_section(2681u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2681u32);
        __result
    };
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
    let all_shards_processed_to_by_partitions = {
        __guard.end_section(2705u32);
        let __result = TestCollationManager::get_all_shards_processed_to_by_partitions_for_mc_block(
                &last_applied_mc_block_key,
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
            )
            .await;
        __guard.start_section(2705u32);
        __result
    }
        .unwrap();
    tracing::trace!(
        "all_shards_processed_to_by_partitions: {:?}",
        all_shards_processed_to_by_partitions,
    );
    let min_processed_to_by_shards = find_min_processed_to_by_shards(
        &all_shards_processed_to_by_partitions,
    );
    tracing::trace!("min_processed_to_by_shards: {:?}", min_processed_to_by_shards,);
    let before_tail_block_ids = test_adapter
        .blocks_cache
        .read_before_tail_ids_of_mc_block(&first_applied_mc_block_key)
        .unwrap();
    tracing::trace!("before_tail_block_ids: {:?}", before_tail_block_ids);
    let queue_diffs_applied_to_mc_block_id = *test_adapter
        .last_mc_blocks
        .get(&5)
        .unwrap()
        .id();
    let queue_diffs_applied_to_top_blocks = {
        __guard.end_section(2728u32);
        let __result = TestCollationManager::get_top_blocks_seqno(
                &queue_diffs_applied_to_mc_block_id,
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
            )
            .await;
        __guard.start_section(2728u32);
        __result
    }
        .unwrap();
    tracing::trace!(
        "queue_diffs_applied_to_top_blocks: {:?}", queue_diffs_applied_to_top_blocks,
    );
    let queue_restore_res = {
        __guard.end_section(2743u32);
        let __result = TestCollationManager::restore_queue(
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
                test_adapter.mq_adapter.clone(),
                first_applied_mc_block_key.seqno,
                min_processed_to_by_shards,
                before_tail_block_ids,
                queue_diffs_applied_to_top_blocks,
            )
            .await;
        __guard.start_section(2743u32);
        __result
    }
        .unwrap();
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        7).unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        8).unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        9).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        10).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        11).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        12).unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        6).unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        7).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(& 8)
        .unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(& 9)
        .unwrap().id())
    );
    test_adapter
        .blocks_cache
        .remove_next_collated_blocks_from_cache(
            &queue_restore_res.synced_to_blocks_keys,
        );
    test_adapter.blocks_cache.gc_prev_blocks();
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&10).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&8).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            13,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = test_adapter.store_as_candidate(generated_block_info);
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&12).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&7).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            10,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = test_adapter.store_as_candidate(generated_block_info);
    test_adapter.blocks_cache = BlocksCache::new();
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&10).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&9).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            14,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    last_sc_block_stuff = generated_block_info.block_stuff;
    test_adapter.save_last_info(&last_sc_block_stuff);
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&14).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&8).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            11,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    last_mc_block_stuff = generated_block_info.block_stuff;
    test_adapter.save_last_info(&last_mc_block_stuff);
    let extract_res = test_adapter
        .blocks_cache
        .extract_mc_block_subgraph_for_sync(&test_adapter.last_mc_block_id);
    assert!(matches!(extract_res, McBlockSubgraphExtract::AlreadyExtracted));
    test_adapter.mq_adapter.clear_uncommitted_state(&[]).unwrap();
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&11).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&11).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            15,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2941u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2941u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&12).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&11).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            16,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(2963u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2963u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&16).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&9).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            12,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = {
        __guard.end_section(2988u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(2988u32);
        __result
    };
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
    let all_shards_processed_to_by_partitions = {
        __guard.end_section(3012u32);
        let __result = TestCollationManager::get_all_shards_processed_to_by_partitions_for_mc_block(
                &last_applied_mc_block_key,
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
            )
            .await;
        __guard.start_section(3012u32);
        __result
    }
        .unwrap();
    tracing::trace!(
        "all_shards_processed_to_by_partitions: {:?}",
        all_shards_processed_to_by_partitions,
    );
    let min_processed_to_by_shards = find_min_processed_to_by_shards(
        &all_shards_processed_to_by_partitions,
    );
    tracing::trace!("min_processed_to_by_shards: {:?}", min_processed_to_by_shards,);
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
        queue_diffs_applied_to_mc_block_id, * test_adapter.last_mc_blocks.get(& 9)
        .unwrap().id()
    );
    let queue_diffs_applied_to_top_blocks = {
        __guard.end_section(3043u32);
        let __result = TestCollationManager::get_top_blocks_seqno(
                &queue_diffs_applied_to_mc_block_id,
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
            )
            .await;
        __guard.start_section(3043u32);
        __result
    }
        .unwrap();
    tracing::trace!(
        "queue_diffs_applied_to_top_blocks: {:?}", queue_diffs_applied_to_top_blocks,
    );
    let queue_restore_res = {
        __guard.end_section(3058u32);
        let __result = TestCollationManager::restore_queue(
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
                test_adapter.mq_adapter.clone(),
                first_applied_mc_block_key.seqno,
                min_processed_to_by_shards,
                before_tail_block_ids,
                queue_diffs_applied_to_top_blocks,
            )
            .await;
        __guard.start_section(3058u32);
        __result
    }
        .unwrap();
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        12).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        13).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        14).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        15).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        16).unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        9).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        10).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        11).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        12).unwrap().id())
    );
    test_adapter
        .blocks_cache
        .remove_next_collated_blocks_from_cache(
            &queue_restore_res.synced_to_blocks_keys,
        );
    test_adapter.blocks_cache.gc_prev_blocks();
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&14).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&11).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            17,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    test_adapter.store_as_candidate(generated_block_info);
    test_adapter.blocks_cache = BlocksCache::new();
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            17,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    last_sc_block_stuff = generated_block_info.block_stuff;
    test_adapter.save_last_info(&last_sc_block_stuff);
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&17).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&12).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            13,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    last_mc_block_stuff = generated_block_info.block_stuff;
    test_adapter.save_last_info(&last_mc_block_stuff);
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&15).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&12).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            18,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    last_sc_block_stuff = generated_block_info.block_stuff;
    test_adapter.save_last_info(&last_sc_block_stuff);
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&15).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&13).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            19,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    last_sc_block_stuff = generated_block_info.block_stuff;
    test_adapter.save_last_info(&last_sc_block_stuff);
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&19).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&13).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            14,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    last_mc_block_stuff = generated_block_info.block_stuff;
    test_adapter.save_last_info(&last_mc_block_stuff);
    test_adapter.mq_adapter.clear_uncommitted_state(&[]).unwrap();
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&15).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&13).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            20,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(3269u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(3269u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_sc_blocks.get(&15).unwrap());
    test_adapter
        .processed_to_stuff
        .set_processed_to(shard, test_adapter.last_mc_blocks.get(&14).unwrap());
    let generated_block_info = test_adapter
        .gen_shard_block(
            shard,
            21,
            last_sc_block_stuff.prev_block_info(),
            last_mc_block_stuff.prev_block_info(),
            10,
        );
    StoreBlockResult {
        block_stuff: last_sc_block_stuff,
    } = {
        __guard.end_section(3291u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(3291u32);
        __result
    };
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_sc_blocks.get(&21).unwrap(),
        );
    test_adapter
        .processed_to_stuff
        .set_processed_to(
            ShardIdent::MASTERCHAIN,
            test_adapter.last_mc_blocks.get(&14).unwrap(),
        );
    let top_sc_block_updated = true;
    let generated_block_info = test_adapter
        .gen_master_block(
            15,
            last_mc_block_stuff.prev_block_info(),
            &last_sc_block_stuff.data,
            top_sc_block_updated,
            false,
            5,
        );
    StoreBlockResult {
        block_stuff: last_mc_block_stuff,
    } = {
        __guard.end_section(3316u32);
        let __result = test_adapter.store_as_received(generated_block_info).await;
        __guard.start_section(3316u32);
        __result
    };
    let _ = &last_mc_block_stuff;
    tracing::trace!("queue restore - case 05");
    let first_applied_mc_block_key = test_adapter
        .last_mc_blocks
        .get(&15)
        .unwrap()
        .id()
        .as_short_id();
    tracing::trace!("first_applied_mc_block_key: {}", first_applied_mc_block_key);
    let last_applied_mc_block_key = test_adapter
        .last_mc_blocks
        .get(&15)
        .unwrap()
        .id()
        .as_short_id();
    tracing::trace!("last_applied_mc_block_key: {}", last_applied_mc_block_key);
    let all_shards_processed_to_by_partitions = {
        __guard.end_section(3341u32);
        let __result = TestCollationManager::get_all_shards_processed_to_by_partitions_for_mc_block(
                &last_applied_mc_block_key,
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
            )
            .await;
        __guard.start_section(3341u32);
        __result
    }
        .unwrap();
    tracing::trace!(
        "all_processed_to_by_shards: {:?}", all_shards_processed_to_by_partitions,
    );
    let min_processed_to_by_shards = find_min_processed_to_by_shards(
        &all_shards_processed_to_by_partitions,
    );
    tracing::trace!("min_processed_to_by_shards: {:?}", min_processed_to_by_shards,);
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
        queue_diffs_applied_to_mc_block_id, * test_adapter.last_mc_blocks.get(& 12)
        .unwrap().id()
    );
    let queue_diffs_applied_to_top_blocks = {
        __guard.end_section(3372u32);
        let __result = TestCollationManager::get_top_blocks_seqno(
                &queue_diffs_applied_to_mc_block_id,
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
            )
            .await;
        __guard.start_section(3372u32);
        __result
    }
        .unwrap();
    tracing::trace!(
        "queue_diffs_applied_to_top_blocks: {:?}", queue_diffs_applied_to_top_blocks,
    );
    let queue_restore_res = {
        __guard.end_section(3387u32);
        let __result = TestCollationManager::restore_queue(
                &test_adapter.blocks_cache,
                test_adapter.state_adapter.clone(),
                test_adapter.mq_adapter.clone(),
                first_applied_mc_block_key.seqno,
                min_processed_to_by_shards,
                before_tail_block_ids,
                queue_diffs_applied_to_top_blocks,
            )
            .await;
        __guard.start_section(3387u32);
        __result
    }
        .unwrap();
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        16).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        17).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        18).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        19).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        20).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_sc_blocks.get(&
        21).unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        12).unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        13).unwrap().id())
    );
    assert!(
        ! queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        14).unwrap().id())
    );
    assert!(
        queue_restore_res.applied_diffs_ids.contains(test_adapter.last_mc_blocks.get(&
        15).unwrap().id())
    );
    test_adapter
        .blocks_cache
        .remove_next_collated_blocks_from_cache(
            &queue_restore_res.synced_to_blocks_keys,
        );
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
    sc_processed_to_info: FastHashMap<
        QueuePartitionIdx,
        BTreeMap<ShardIdent, (BlockSeqno, QueueKey)>,
    >,
    mc_processed_to_info: FastHashMap<
        QueuePartitionIdx,
        BTreeMap<ShardIdent, (BlockSeqno, QueueKey)>,
    >,
}
impl TestProcessedToStuff {
    fn new(shard: ShardIdent) -> Self {
        let default_partition_processed_to: BTreeMap<_, _> = [
            (shard, (0, QueueKey::min_for_lt(0))),
            (ShardIdent::MASTERCHAIN, (0, QueueKey::min_for_lt(0))),
        ]
            .into_iter()
            .collect();
        Self {
            sc_processed_to_info: [
                (QueuePartitionIdx(0), default_partition_processed_to.clone()),
                (QueuePartitionIdx(1), default_partition_processed_to.clone()),
            ]
                .into_iter()
                .collect(),
            mc_processed_to_info: [
                (QueuePartitionIdx(0), default_partition_processed_to.clone()),
                (QueuePartitionIdx(1), default_partition_processed_to.clone()),
            ]
                .into_iter()
                .collect(),
        }
    }
    fn set_processed_to(&mut self, shard: ShardIdent, block_stuff: &BlockStuffAug) {
        let value = (block_stuff.id().seqno, QueueKey::max_for_lt(block_stuff.end_lt()));
        if shard.is_masterchain() {
            for (_, partition_processed_to) in self.mc_processed_to_info.iter_mut() {
                partition_processed_to.insert(block_stuff.id().shard, value);
            }
        } else {
            for (_, partition_processed_to) in self.sc_processed_to_info.iter_mut() {
                partition_processed_to.insert(block_stuff.id().shard, value);
            }
        }
    }
    fn get_min_processed_to_from(
        processed_to_info: &FastHashMap<
            QueuePartitionIdx,
            BTreeMap<ShardIdent, (BlockSeqno, QueueKey)>,
        >,
    ) -> ProcessedTo {
        let mut min_processed_to = ProcessedTo::default();
        for partition_processed_to in processed_to_info.values() {
            for (&shard, (_, to_key)) in partition_processed_to {
                min_processed_to
                    .entry(shard)
                    .and_modify(|min| *min = std::cmp::min(*min, *to_key))
                    .or_insert(*to_key);
            }
        }
        min_processed_to
    }
    fn get_min_sc_processed_to(&self) -> ProcessedTo {
        Self::get_min_processed_to_from(&self.sc_processed_to_info)
    }
    fn get_min_mc_processed_to(&self) -> ProcessedTo {
        Self::get_min_processed_to_from(&self.mc_processed_to_info)
    }
    fn get_min_seqno_for_shard(
        shard: &ShardIdent,
        processed_to_info: &FastHashMap<
            QueuePartitionIdx,
            BTreeMap<ShardIdent, (BlockSeqno, QueueKey)>,
        >,
    ) -> BlockSeqno {
        let mut min_seqno = BlockSeqno::MAX;
        for partition_processed_to in processed_to_info.values() {
            let (seqno, _) = partition_processed_to.get(shard).unwrap();
            min_seqno = std::cmp::min(min_seqno, *seqno);
        }
        min_seqno
    }
    fn calc_tail_len(&self, shard: &ShardIdent, next_seqno: BlockSeqno) -> u32 {
        let mc_min_seqno = Self::get_min_seqno_for_shard(
            shard,
            &self.mc_processed_to_info,
        );
        let sc_min_seqno = Self::get_min_seqno_for_shard(
            shard,
            &self.sc_processed_to_info,
        );
        let min_processed_to_seqno = mc_min_seqno.min(sc_min_seqno);
        next_seqno - min_processed_to_seqno
    }
    fn gen_processed_upto(
        processed_to_info: &FastHashMap<
            QueuePartitionIdx,
            BTreeMap<ShardIdent, (BlockSeqno, QueueKey)>,
        >,
    ) -> ProcessedUptoInfoStuff {
        ProcessedUptoInfoStuff {
            msgs_exec_params: None,
            partitions: processed_to_info
                .iter()
                .map(|(par_id, par)| {
                    (
                        *par_id,
                        ProcessedUptoPartitionStuff {
                            internals: InternalsProcessedUptoStuff {
                                processed_to: par
                                    .iter()
                                    .map(|(shard, (_, to_key))| (*shard, *to_key))
                                    .collect(),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    )
                })
                .collect(),
        }
    }
    fn gen_sc_processed_upto(&self) -> ProcessedUptoInfoStuff {
        Self::gen_processed_upto(&self.sc_processed_to_info)
    }
    fn gen_mc_processed_upto(&self) -> ProcessedUptoInfoStuff {
        Self::gen_processed_upto(&self.mc_processed_to_info)
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
        let processed_to = self.processed_to_stuff.get_min_sc_processed_to();
        let queue_diff_with_msgs = create_queue_diff_with_msgs(
            into_messages(test_messages),
            processed_to.clone(),
        );
        let processed_upto = self.processed_to_stuff.gen_sc_processed_upto();
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
                processed_upto,
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
        let processed_to = self.processed_to_stuff.get_min_mc_processed_to();
        let queue_diff_with_msgs = create_queue_diff_with_msgs(
            into_messages(test_messages),
            processed_to.clone(),
        );
        let processed_upto = self.processed_to_stuff.gen_mc_processed_upto();
        self.state_adapter
            .add_master_block(
                seqno,
                start_lt,
                self.account_lt,
                queue_diff_with_msgs,
                self.processed_to_stuff.calc_tail_len(&ShardIdent::MASTERCHAIN, seqno),
                prev_block_id,
                prev_block_end_lt,
                shard_block_stuff,
                top_sc_block_updated,
                mc_is_key_block,
                processed_upto,
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
            consensus_config_changed: block_stuff.id().is_masterchain().then_some(false),
            prev_blocks_ids: vec![prev_block_id],
            top_shard_blocks_ids: mc_top_shard_blocks_info
                .iter()
                .map(|(block_id, _updated)| *block_id)
                .collect(),
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
            .store_collated(
                block_candidate,
                mc_top_shard_blocks_info,
                block_stuff.id().is_masterchain().then_some(0),
            )
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
            self.last_mc_blocks.insert(block_id.seqno, block_stuff.clone());
        } else {
            self.last_sc_block_id = block_id;
            self.last_sc_blocks.insert(block_id.seqno, block_stuff.clone());
        }
    }
    async fn store_as_received(
        &mut self,
        generated_block_info: CreatedBlockInfo<V>,
    ) -> StoreBlockResult {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(store_as_received)),
            file!(),
            3816u32,
        );
        let generated_block_info = generated_block_info;
        let CreatedBlockInfo { state_stuff, block_stuff, .. } = generated_block_info;
        let processed_upto = state_stuff
            .state()
            .processed_upto
            .load()
            .unwrap()
            .try_into()
            .unwrap();
        let block_mismatch = match {
            __guard.end_section(3832u32);
            let __result = self
                .blocks_cache
                .store_received(self.state_adapter.clone(), state_stuff, processed_upto)
                .await;
            __guard.start_section(3832u32);
            __result
        }
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
        messages: out_msgs.iter().map(|msg| (msg.key(), msg.clone())).collect(),
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
            (ShardStateStuff, BlockStuffAug, WithArchiveData<QueueDiffStuff>, BlockSeqno),
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
        processed_upto: ProcessedUptoInfoStuff,
    ) -> Result<CreatedBlockInfo<V>> {
        let prev_block_seqno = seqno.saturating_sub(1);
        let ref_by_mc_seqno = if shard.is_masterchain() {
            seqno
        } else {
            master_ref_opt.as_ref().unwrap().seqno + 1
        };
        let prev_queue_diff_hash = self
            .storage
            .entry(shard)
            .or_default()
            .get(&prev_block_seqno)
            .map(|(_, _, queue_diff_stuff, _)| *queue_diff_stuff.diff_hash())
            .unwrap_or_default();
        let (min_message, max_message) = {
            let messages = &queue_diff_with_msgs.messages;
            match messages.first_key_value().zip(messages.last_key_value()) {
                Some(((min, _), (max, _))) => (*min, *max),
                None => (QueueKey::min_for_lt(start_lt), QueueKey::max_for_lt(end_lt)),
            }
        };
        let queue_diff_serialized = QueueDiffStuff::builder(
                shard,
                seqno,
                &prev_queue_diff_hash,
            )
            .with_processed_to(queue_diff_with_msgs.processed_to.clone())
            .with_messages(
                &min_message,
                &max_message,
                queue_diff_with_msgs.messages.keys().map(|k| &k.hash),
            )
            .with_router(
                queue_diff_with_msgs.partition_router.to_router_partitions_src(),
                queue_diff_with_msgs.partition_router.to_router_partitions_dst(),
            )
            .serialize();
        let queue_diff_hash = *queue_diff_serialized.hash();
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
            Some(shards_descr) => {
                Some(McBlockExtra {
                    shards: ShardHashes::from_shards(shards_descr.iter())?,
                    ..Default::default()
                })
            }
            None => None,
        };
        let out_msg_description = build_out_msg_description(
            shard,
            &queue_diff_with_msgs,
        )?;
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
        let block_stuff = BlockStuff::from_block_and_root(
            &block_id,
            block,
            root,
            data_size,
        );
        let block_stuff = WithArchiveData::new(block_stuff, data);
        let queue_diff_stuff = queue_diff_serialized.build(&block_id);
        let mc_state_extra_opt = mc_block_extra_opt
            .map(|extra| McStateExtra {
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
            processed_upto: Lazy::new(&(processed_upto.try_into()?))?,
            ..Default::default()
        };
        let state_stuff = ShardStateStuff::from_state_and_root(
                &block_id,
                Box::new(shard_state),
                Cell::default(),
                self.mcstate_tracker.insert_untracked(),
            )
            .unwrap();
        self.storage
            .entry(shard)
            .or_default()
            .insert(
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
        processed_upto: ProcessedUptoInfoStuff,
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
            processed_upto,
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
        processed_upto: ProcessedUptoInfoStuff,
    ) -> Result<CreatedBlockInfo<V>> {
        let shard = ShardIdent::MASTERCHAIN;
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
            processed_upto,
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
    async fn get_ref_by_mc_seqno(
        &self,
        block_id: &BlockId,
    ) -> Result<Option<BlockSeqno>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_ref_by_mc_seqno)),
            file!(),
            4179u32,
        );
        let block_id = block_id;
        let res = self
            .storage
            .get(&block_id.shard)
            .and_then(|s| {
                s.get(&block_id.seqno).map(|(_, _, _, ref_by_mc_seqno)| *ref_by_mc_seqno)
            });
        Ok(res)
    }
    async fn load_block(&self, block_id: &BlockId) -> Result<Option<BlockStuff>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block)),
            file!(),
            4187u32,
        );
        let block_id = block_id;
        let res = self
            .storage
            .get(&block_id.shard)
            .and_then(|s| {
                s.get(&block_id.seqno)
                    .map(|(_, block_stuff, _, _)| &block_stuff.data)
                    .cloned()
            });
        Ok(res)
    }
    async fn load_diff(&self, block_id: &BlockId) -> Result<Option<QueueDiffStuff>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_diff)),
            file!(),
            4196u32,
        );
        let block_id = block_id;
        let res = self
            .storage
            .get(&block_id.shard)
            .and_then(|s| {
                s.get(&block_id.seqno)
                    .map(|(_, _, queue_diff_stuff, _)| &queue_diff_stuff.data)
                    .cloned()
            });
        Ok(res)
    }
    async fn load_state(
        &self,
        _ref_by_mc_seqno: u32,
        block_id: &BlockId,
    ) -> Result<ShardStateStuff> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_state)),
            file!(),
            4209u32,
        );
        let _ref_by_mc_seqno = _ref_by_mc_seqno;
        let block_id = block_id;
        let res = self
            .storage
            .get(&block_id.shard)
            .and_then(|s| {
                s.get(&block_id.seqno).map(|(state_stuff, _, _, _)| state_stuff).cloned()
            });
        res.ok_or_else(|| {
            anyhow!("state not found for mc block {}", block_id.as_short_id())
        })
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(store_state_root)),
            file!(),
            4227u32,
        );
        let _block_id = _block_id;
        let _meta = _meta;
        let _state_root = _state_root;
        let _hint = _hint;
        unreachable!()
    }
    async fn load_block_by_handle(
        &self,
        _handle: &BlockHandle,
    ) -> Result<Option<BlockStuff>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block_by_handle)),
            file!(),
            4230u32,
        );
        let _handle = _handle;
        unreachable!()
    }
    async fn load_block_handle(
        &self,
        _block_id: &BlockId,
    ) -> Result<Option<BlockHandle>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(load_block_handle)),
            file!(),
            4233u32,
        );
        let _block_id = _block_id;
        unreachable!()
    }
    fn accept_block(&self, _block: Arc<BlockStuffForSync>) -> Result<()> {
        unreachable!()
    }
    async fn wait_for_block(
        &self,
        _block_id: &BlockId,
    ) -> Option<Result<BlockStuffAug>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_for_block)),
            file!(),
            4239u32,
        );
        let _block_id = _block_id;
        unreachable!()
    }
    async fn wait_for_block_next(
        &self,
        _block_id: &BlockId,
    ) -> Option<Result<BlockStuffAug>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(wait_for_block_next)),
            file!(),
            4242u32,
        );
        let _block_id = _block_id;
        unreachable!()
    }
    async fn handle_state(&self, _state: &ShardStateStuff) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_state)),
            file!(),
            4245u32,
        );
        let _state = _state;
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
        let dst_in_current_shard = curr_shard_id
            .contains_prefix(dst_workchain, dst_prefix);
        let out_msg = OutMsg::New(OutMsgNew {
            out_msg_envelope: Lazy::new(
                &MsgEnvelope {
                    cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                    next_addr: if dst_in_current_shard {
                        IntermediateAddr::FULL_DEST_SAME_WORKCHAIN
                    } else {
                        IntermediateAddr::FULL_SRC_SAME_WORKCHAIN
                    },
                    fwd_fee_remaining: *fwd_fee,
                    message: Lazy::new(
                        &OwnedMessage {
                            info: MsgInfo::Int(msg.info().clone()),
                            init: None,
                            body: msg.cell().clone().into(),
                            layout: None,
                        },
                    )?,
                },
            )?,
            transaction: Lazy::from_raw(Cell::empty_cell())?,
        });
        out_msgs
            .insert(
                *msg.cell().repr_hash(),
                (out_msg.compute_exported_value()?, Lazy::new(&out_msg)?),
            );
    }
    let res = RelaxedAugDict::try_from_sorted_iter_lazy(
            out_msgs
                .iter()
                .map(|(msg_id, (exported_value, out_msg))| (
                    msg_id,
                    exported_value,
                    out_msg,
                )),
        )?
        .build()?;
    Ok(res)
}
#[test]
fn caps_subset_is_correct() {
    let block_caps = GlobalCapabilities::from_iter([
        GlobalCapability::CapBounceMsgBody,
        GlobalCapability::CapFullBodyInBounced,
    ]);
    let supported = GlobalCapabilities::from_iter([
        GlobalCapability::CapBounceMsgBody,
        GlobalCapability::CapFullBodyInBounced,
        GlobalCapability::CapReportVersion,
    ]);
    assert!(block_caps.is_subset_of(supported));
    assert!(! supported.is_subset_of(block_caps));
    let block_caps = GlobalCapabilities::new(block_caps.into_inner() | (1u64 << 63));
    assert!(! block_caps.is_subset_of(supported));
}
