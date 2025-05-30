use std::sync::Arc;

use async_trait::async_trait;
use everscale_types::cell::HashBytes;
use everscale_types::dict::Dict;
use everscale_types::models::{
    BlockId, BlockchainConfig, CurrencyCollection, ShardIdent, ValidatorInfo,
};
use tycho_block_util::state::MinRefMcStateTracker;

use crate::collator::types::AnchorsCache;
use crate::collator::{CollatorStdImpl, ImportInitAnchorsResult, InitAnchorSource};
use crate::mempool::{MempoolAdapterStubImpl, MempoolAnchor, MempoolEventListener};
use crate::test_utils::try_init_test_tracing;
use crate::types::processed_upto::{
    ExternalsProcessedUptoStuff, ExternalsRangeInfo, ProcessedUptoInfoExtension,
    ProcessedUptoInfoStuff, ProcessedUptoPartitionStuff,
};
use crate::types::{McData, McDataStuff, ShardDescriptionShort};

struct MempoolEventStubListener;
#[async_trait]
impl MempoolEventListener for MempoolEventStubListener {
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> anyhow::Result<()> {
        tracing::trace!(
            "MempoolEventStubListener: on_new_anchor event emitted for anchor \
            (id: {}, chain_time: {}, externals: {})",
            anchor.id,
            anchor.chain_time,
            anchor.externals.len(),
        );
        Ok(())
    }
}

#[tokio::test]
async fn test_import_init_anchors() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);

    let shard_id = ShardIdent::new_full(0);
    let mut anchors_cache = AnchorsCache::default();

    let adapter =
        MempoolAdapterStubImpl::with_stub_externals(Arc::new(MempoolEventStubListener), None);
    let mpool_adapter = adapter;

    let filter_imported = |init_anchors_info: Vec<InitAnchorSource>| {
        init_anchors_info
            .into_iter()
            .filter_map(|item| match item {
                InitAnchorSource::Imported(info) => Some(info),
                InitAnchorSource::FromCache(_) => None,
            })
            .collect::<Vec<_>>()
    };

    // =========================================================================
    // Get all anchors from mempool
    // =========================================================================
    let processed_to_anchor_id = 9;
    let processed_to_msgs_offset = 0;
    let last_block_chain_time = 20832;
    let current_shard_last_imported_chain_time = 19096;

    let ImportInitAnchorsResult {
        anchors_info,
        anchors_count_above_last_imported_in_current_shard,
    } = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset,
        last_block_chain_time,
        current_shard_last_imported_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
    )
    .await
    .unwrap();

    let anchors_info = filter_imported(anchors_info);

    tracing::debug!(
        "imported anchors on init (count_above_last = {}): {:?}",
        anchors_count_above_last_imported_in_current_shard,
        anchors_info.as_slice(),
    );

    assert_eq!(anchors_info.len(), 4);
    assert_eq!(anchors_info[0].id, 9);
    let (anchor_id, _) = anchors_cache.get(0).unwrap();
    assert_eq!(anchor_id, processed_to_anchor_id);
    let (last_imported_id, last_imported_ct) =
        anchors_cache.get_last_imported_anchor_id_and_ct().unwrap();
    assert_eq!(last_imported_id, 12);
    assert_eq!(last_imported_ct, 20832);
    assert_eq!(anchors_cache.len(), 3);
    assert!(anchors_cache.has_pending_externals());
    assert_eq!(anchors_count_above_last_imported_in_current_shard, 1);

    // =========================================================================
    // Get all anchors from mempool. processed_to anchor is fully read
    // =========================================================================
    anchors_cache.clear();

    let processed_to_anchor_id = 9;
    let processed_to_msgs_offset = 4;
    let last_block_chain_time = 20832;
    let current_shard_last_imported_chain_time = 20832;

    let ImportInitAnchorsResult {
        anchors_info,
        anchors_count_above_last_imported_in_current_shard,
    } = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset,
        last_block_chain_time,
        current_shard_last_imported_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
    )
    .await
    .unwrap();

    let anchors_info = filter_imported(anchors_info);

    tracing::debug!(
        "imported anchors on init (count_above_last = {}): {:?}",
        anchors_count_above_last_imported_in_current_shard,
        anchors_info.as_slice(),
    );

    assert_eq!(anchors_info.len(), 4);
    assert_eq!(anchors_info[0].id, 9);
    let (anchor_id, _) = anchors_cache.get(0).unwrap();
    assert_eq!(anchor_id, 11);
    let (last_imported_id, last_imported_ct) =
        anchors_cache.get_last_imported_anchor_id_and_ct().unwrap();
    assert_eq!(last_imported_id, 12);
    assert_eq!(last_imported_ct, 20832);
    assert_eq!(anchors_cache.len(), 2);
    assert!(anchors_cache.has_pending_externals());
    assert_eq!(anchors_count_above_last_imported_in_current_shard, 0);

    // =========================================================================
    // processed_to anchor exists in cache, get some anchors from cache, rest from mempool
    // =========================================================================

    let processed_to_anchor_id = 12;
    let processed_to_msgs_offset = 1;
    let last_block_chain_time = 24304;
    let current_shard_last_imported_chain_time = 20832;

    let ImportInitAnchorsResult {
        anchors_info,
        anchors_count_above_last_imported_in_current_shard,
    } = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset,
        last_block_chain_time,
        current_shard_last_imported_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
    )
    .await
    .unwrap();

    let anchors_info = filter_imported(anchors_info);

    tracing::debug!(
        "imported anchors on init (count_above_last = {}): {:?}",
        anchors_count_above_last_imported_in_current_shard,
        anchors_info.as_slice(),
    );

    assert_eq!(anchors_info.len(), 2);
    assert_eq!(anchors_info[0].id, 13);
    let (anchor_id, _) = anchors_cache.get(0).unwrap();
    assert_eq!(anchor_id, 11);
    let (last_imported_id, last_imported_ct) =
        anchors_cache.get_last_imported_anchor_id_and_ct().unwrap();
    assert_eq!(last_imported_id, 14);
    assert_eq!(last_imported_ct, 24304);
    assert_eq!(anchors_cache.len(), 4);
    assert!(anchors_cache.has_pending_externals());
    assert_eq!(anchors_count_above_last_imported_in_current_shard, 2);

    // =========================================================================
    // processed_to anchor exists in cache, get all from cache
    // =========================================================================

    let processed_to_anchor_id = 11;
    let processed_to_msgs_offset = 2;
    let last_block_chain_time = 24304;
    let current_shard_last_imported_chain_time = 24304;

    let ImportInitAnchorsResult {
        anchors_info,
        anchors_count_above_last_imported_in_current_shard,
    } = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset,
        last_block_chain_time,
        current_shard_last_imported_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
    )
    .await
    .unwrap();

    let anchors_info = filter_imported(anchors_info);

    tracing::debug!(
        "imported anchors on init (count_above_last = {}): {:?}",
        anchors_count_above_last_imported_in_current_shard,
        anchors_info.as_slice(),
    );

    assert_eq!(anchors_info.len(), 0);
    let (anchor_id, _) = anchors_cache.get(0).unwrap();
    assert_eq!(anchor_id, 11);
    let (last_imported_id, last_imported_ct) =
        anchors_cache.get_last_imported_anchor_id_and_ct().unwrap();
    assert_eq!(last_imported_id, 14);
    assert_eq!(last_imported_ct, 24304);
    assert_eq!(anchors_cache.len(), 4);
    assert!(anchors_cache.has_pending_externals());
    assert_eq!(anchors_count_above_last_imported_in_current_shard, 0);

    // =========================================================================
    // processed_to anchor is before all anchors in cache, should clear cache and load all required from mempool
    // =========================================================================

    let processed_to_anchor_id = 9;
    let processed_to_msgs_offset = 2;
    let last_block_chain_time = 20832;
    let current_shard_last_imported_chain_time = 15624;

    let ImportInitAnchorsResult {
        anchors_info,
        anchors_count_above_last_imported_in_current_shard,
    } = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset,
        last_block_chain_time,
        current_shard_last_imported_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
    )
    .await
    .unwrap();

    let anchors_info = filter_imported(anchors_info);

    tracing::debug!(
        "imported anchors on init (count_above_last = {}): {:?}",
        anchors_count_above_last_imported_in_current_shard,
        anchors_info.as_slice(),
    );

    assert_eq!(anchors_info.len(), 4);
    assert_eq!(anchors_info[0].id, 9);
    let (anchor_id, _) = anchors_cache.get(0).unwrap();
    assert_eq!(anchor_id, processed_to_anchor_id);
    let (last_imported_id, last_imported_ct) =
        anchors_cache.get_last_imported_anchor_id_and_ct().unwrap();
    assert_eq!(last_imported_id, 12);
    assert_eq!(last_imported_ct, 20832);
    assert_eq!(anchors_cache.len(), 3);
    assert!(anchors_cache.has_pending_externals());
    assert_eq!(anchors_count_above_last_imported_in_current_shard, 3);

    // =========================================================================
    // processed_to anchor is after all anchors in cache, should clear cache and load all required from mempool
    // =========================================================================

    let processed_to_anchor_id = 13;
    let processed_to_msgs_offset = 3;
    let last_block_chain_time = 29512;
    let current_shard_last_imported_chain_time = 26040;

    let ImportInitAnchorsResult {
        anchors_info,
        anchors_count_above_last_imported_in_current_shard,
    } = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset,
        last_block_chain_time,
        current_shard_last_imported_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
    )
    .await
    .unwrap();

    let anchors_info = filter_imported(anchors_info);

    tracing::debug!(
        "imported anchors on init (count_above_last = {}): {:?}",
        anchors_count_above_last_imported_in_current_shard,
        anchors_info.as_slice(),
    );

    assert_eq!(anchors_info.len(), 5);
    assert_eq!(anchors_info[0].id, 13);
    let (anchor_id, _) = anchors_cache.get(0).unwrap();
    assert_eq!(anchor_id, processed_to_anchor_id);
    let (last_imported_id, last_imported_ct) =
        anchors_cache.get_last_imported_anchor_id_and_ct().unwrap();
    assert_eq!(last_imported_id, 17);
    assert_eq!(last_imported_ct, 29512);
    assert_eq!(anchors_cache.len(), 4);
    assert!(anchors_cache.has_pending_externals());
    assert_eq!(anchors_count_above_last_imported_in_current_shard, 2);
}

#[test]
fn test_get_anchors_processing_info() {
    let shard_id = ShardIdent::new_full(0);

    // test zerostate
    // prev data
    let prev_block_id = BlockId {
        shard: shard_id,
        seqno: 0,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };
    let prev_gen_chain_time = 0;
    let prev_processed_upto_externals = ExternalsProcessedUptoStuff::default();

    // mc data
    let tracker = MinRefMcStateTracker::new();
    let mut mc_data = McData {
        // test values
        block_id: BlockId {
            shard: ShardIdent::MASTERCHAIN,
            seqno: 0,
            root_hash: Default::default(),
            file_hash: Default::default(),
        },
        gen_chain_time: 0,
        processed_upto: ProcessedUptoInfoStuff::default(),
        shards: vec![(ShardIdent::new_full(0), ShardDescriptionShort {
            seqno: 0,
            ext_processed_to_anchor_id: 0,
            top_sc_block_updated: false,
            end_lt: 0,
            root_hash: Default::default(),
            file_hash: Default::default(),
        })],
        // dummy values
        global_id: 0,
        prev_key_block_seqno: 0,
        gen_lt: 0,
        libraries: Dict::default(),
        total_validator_fees: CurrencyCollection::default(),
        global_balance: CurrencyCollection::default(),
        config: BlockchainConfig::new_empty(HashBytes([0x55; 32])),
        validator_info: ValidatorInfo {
            validator_list_hash_short: 0,
            catchain_seqno: 1,
            nx_cc_updated: false,
        },
        consensus_info: Default::default(),
        top_processed_to_anchor: 0,
        ref_mc_state_handle: tracker.insert(0),
        shards_processed_to_by_partitions: Default::default(),
        prev_mc_block_id: None,
    };

    let mc_data_stuff = McDataStuff {
        current: Arc::new(mc_data.clone()),
        previous: None,
    };

    //------
    // on zerostate will return None
    let anchors_proc_info_opt = CollatorStdImpl::get_anchors_processing_info(
        &shard_id,
        &mc_data_stuff,
        &prev_block_id,
        prev_gen_chain_time,
        prev_processed_upto_externals.processed_to,
    );
    assert!(anchors_proc_info_opt.is_none());

    // ======
    // collated shard block 0:17, then collated master block 1:967
    // master block processed less externals because of large queue
    let prev_block_id = BlockId {
        shard: shard_id,
        seqno: 17,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };
    let prev_gen_chain_time = 1732479499855;
    let prev_processed_upto_externals = ExternalsProcessedUptoStuff {
        processed_to: (1764, 23429),
        ranges: [(17, ExternalsRangeInfo {
            skip_offset: 0,
            processed_offset: 0,
            chain_time: prev_gen_chain_time,
            from: (0, 0),
            to: (1764, 23429),
        })]
        .iter()
        .cloned()
        .collect(),
    };

    mc_data.block_id.seqno = 967;
    mc_data.gen_chain_time = 1732479499855;
    mc_data
        .processed_upto
        .partitions
        .insert(0, ProcessedUptoPartitionStuff {
            externals: ExternalsProcessedUptoStuff {
                processed_to: (1752, 12000),
                ranges: [(967, ExternalsRangeInfo {
                    skip_offset: 0,
                    processed_offset: 0,
                    chain_time: mc_data_stuff.current.gen_chain_time,
                    from: (0, 0),
                    to: (1752, 12000),
                })]
                .iter()
                .cloned()
                .collect(),
            },
            internals: Default::default(),
        });
    let (_, shard_desc) = mc_data.shards.get_mut(0).unwrap();
    shard_desc.seqno = 17;
    shard_desc.ext_processed_to_anchor_id = 1764;
    shard_desc.top_sc_block_updated = true;

    //------
    // will get anchors processing info from prev shard state
    let anchors_proc_info_opt = CollatorStdImpl::get_anchors_processing_info(
        &shard_id,
        &mc_data_stuff,
        &prev_block_id,
        prev_gen_chain_time,
        prev_processed_upto_externals.processed_to,
    );
    assert!(anchors_proc_info_opt.is_some());
    let anchors_proc_info = anchors_proc_info_opt.unwrap();
    assert_eq!(
        anchors_proc_info.processed_to_anchor_id,
        prev_processed_upto_externals.processed_to.0,
    );
    assert_eq!(
        anchors_proc_info.processed_to_msgs_offset,
        prev_processed_upto_externals.processed_to.1,
    );
    assert_eq!(
        anchors_proc_info.last_imported_chain_time,
        prev_gen_chain_time,
    );
    assert_eq!(anchors_proc_info.last_imported_in_block_id, prev_block_id);

    //======
    // collated master block 1:968, it used the same shard block 0:17
    // master still processed less externals then shard
    mc_data.block_id.seqno = 968;
    mc_data.gen_chain_time = 1732479502300;
    mc_data
        .processed_upto
        .partitions
        .insert(0, ProcessedUptoPartitionStuff {
            externals: ExternalsProcessedUptoStuff {
                processed_to: (1756, 7000),
                ranges: [(968, ExternalsRangeInfo {
                    skip_offset: 0,
                    processed_offset: 0,
                    chain_time: mc_data_stuff.current.gen_chain_time,
                    from: (1752, 12000),
                    to: (1756, 7000),
                })]
                .iter()
                .cloned()
                .collect(),
            },
            internals: Default::default(),
        });
    let (_, shard_desc) = mc_data.shards.get_mut(0).unwrap();
    shard_desc.seqno = 17;
    shard_desc.top_sc_block_updated = false;

    //------
    // will get anchors processing info from prev shard state
    // because it is still ahead of master

    let mc_data_stuff = McDataStuff {
        current: Arc::new(mc_data.clone()),
        previous: None,
    };
    let anchors_proc_info_opt = CollatorStdImpl::get_anchors_processing_info(
        &shard_id,
        &mc_data_stuff,
        &prev_block_id,
        prev_gen_chain_time,
        prev_processed_upto_externals.processed_to,
    );
    assert!(anchors_proc_info_opt.is_some());
    let anchors_proc_info = anchors_proc_info_opt.unwrap();
    assert_eq!(
        anchors_proc_info.processed_to_anchor_id,
        prev_processed_upto_externals.processed_to.0,
    );
    assert_eq!(
        anchors_proc_info.processed_to_msgs_offset,
        prev_processed_upto_externals.processed_to.1,
    );
    assert_eq!(
        anchors_proc_info.last_imported_chain_time,
        prev_gen_chain_time,
    );
    assert_eq!(anchors_proc_info.last_imported_in_block_id, prev_block_id);

    //======
    // collated master block 1:1005, it used the same shard block 0:17
    // but master processed anchors ahead of shard
    mc_data.block_id.seqno = 1005;
    mc_data.gen_chain_time = 1732479530330;
    mc_data
        .processed_upto
        .partitions
        .insert(0, ProcessedUptoPartitionStuff {
            externals: ExternalsProcessedUptoStuff {
                processed_to: (1816, 23429),
                ranges: [(1005, ExternalsRangeInfo {
                    skip_offset: 0,
                    processed_offset: 0,
                    chain_time: mc_data_stuff.current.gen_chain_time,
                    from: (1756, 7000),
                    to: (1816, 23429),
                })]
                .iter()
                .cloned()
                .collect(),
            },
            internals: Default::default(),
        });
    let (_, shard_desc) = mc_data.shards.get_mut(0).unwrap();
    shard_desc.top_sc_block_updated = false;

    //------
    // will get anchors processing info from mc data
    let mc_data_stuff = McDataStuff {
        current: Arc::new(mc_data.clone()),
        previous: None,
    };
    let anchors_proc_info_opt = CollatorStdImpl::get_anchors_processing_info(
        &shard_id,
        &mc_data_stuff,
        &prev_block_id,
        prev_gen_chain_time,
        prev_processed_upto_externals.processed_to,
    );
    assert!(anchors_proc_info_opt.is_some());
    let anchors_proc_info = anchors_proc_info_opt.unwrap();
    let min_externals_processed_to = mc_data_stuff
        .current
        .processed_upto
        .get_min_externals_processed_to()
        .unwrap_or_default();
    assert_eq!(
        anchors_proc_info.processed_to_anchor_id,
        min_externals_processed_to.0,
        "prev_block_id: {:?}, prev_gen_chain_time: {}, prev_processed_upto_externals: {:?}, mc_data: {:?}",
        prev_block_id, prev_gen_chain_time, prev_processed_upto_externals, mc_data_stuff.current,
    );
    assert_eq!(
        anchors_proc_info.processed_to_msgs_offset,
        min_externals_processed_to.1,
    );
    assert_eq!(
        anchors_proc_info.last_imported_chain_time,
        mc_data_stuff.current.gen_chain_time,
    );
    assert_eq!(
        anchors_proc_info.last_imported_in_block_id,
        mc_data_stuff.current.block_id,
    );
}
