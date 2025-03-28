use std::sync::Arc;

use everscale_types::models::ShardIdent;
use parking_lot::Mutex;

use super::CollationManager;
use crate::collator::{CollatorStdImplFactory, ForceMasterCollation};
use crate::manager::types::{CollationSyncState, NextCollationStep};
use crate::validator::ValidatorStdImpl;

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
