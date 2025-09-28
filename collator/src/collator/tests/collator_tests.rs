use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_crypto::ed25519;
use tycho_types::cell::HashBytes;
use tycho_types::dict::Dict;
use tycho_types::models::{
    BlockId, BlockIdShort, BlockchainConfig, CollationConfig, CurrencyCollection, GenesisInfo,
    ShardFeeCreated, ShardIdent, ValidatorInfo,
};
use tycho_util::FastDashMap;

use crate::collator::types::AnchorsCache;
use crate::collator::{
    Collator, CollatorEventListener, CollatorStdImpl, ForceMasterCollation,
    ImportInitAnchorsResult, InitAnchorSource,
};
use crate::internal_queue::types::EnqueuedMessage;
use crate::mempool::{MempoolAdapter, MempoolAdapterStubImpl, MempoolAnchor, MempoolEventListener};
use crate::queue_adapter::MessageQueueAdapter;
use crate::state_node::{
    CollatorSyncContext, StateNodeAdapter, StateNodeAdapterStdImpl, StateNodeEventListener,
};
use crate::test_utils::{load_storage_from_dump, try_init_test_tracing};
use crate::types::processed_upto::{
    ExternalsProcessedUptoStuff, ExternalsRangeInfo, ProcessedUptoInfoExtension,
    ProcessedUptoInfoStuff, ProcessedUptoPartitionStuff,
};
use crate::types::{
    BlockCandidate, BlockCollationResult, BlockIdExt, CollationSessionId, CollationSessionInfo,
    McData, ShardDescriptionShort, ShardDescriptionShortExt, TopBlockDescription,
};
use crate::utils::async_queued_dispatcher::AsyncQueuedDispatcher;

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
        MempoolAdapterStubImpl::with_stub_externals(Arc::new(MempoolEventStubListener), None, None);
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
    let anchor = anchors_cache.first_with_our_externals().unwrap();
    assert_eq!(anchor.id, processed_to_anchor_id);
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
    let anchor = anchors_cache.first_with_our_externals().unwrap();
    assert_eq!(anchor.id, 11);
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
    let anchor = anchors_cache.first_with_our_externals().unwrap();
    assert_eq!(anchor.id, 11);
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
    let anchor = anchors_cache.first_with_our_externals().unwrap();
    assert_eq!(anchor.id, 11);
    let (last_imported_id, last_imported_ct) =
        anchors_cache.get_last_imported_anchor_id_and_ct().unwrap();
    assert_eq!(last_imported_id, 14);
    assert_eq!(last_imported_ct, 24304);
    assert_eq!(anchors_cache.len(), 4);
    assert!(anchors_cache.has_pending_externals());
    assert_eq!(anchors_count_above_last_imported_in_current_shard, 0);

    // =========================================================================
    // processed_to anchor exists in cache, fully read, get all from cache
    // =========================================================================

    let processed_to_anchor_id = 11;
    let processed_to_msgs_offset = 6;
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

    tracing::debug!("restored anchors on init: {:?}", anchors_info.as_slice(),);

    let anchor_info = anchors_info.first().unwrap();
    assert!(matches!(anchor_info, InitAnchorSource::FromCache(anchor) if anchor.id == 12));

    let anchors_info = filter_imported(anchors_info);

    tracing::debug!(
        "imported anchors on init (count_above_last = {}): {:?}",
        anchors_count_above_last_imported_in_current_shard,
        anchors_info.as_slice(),
    );

    assert_eq!(anchors_info.len(), 0);
    let anchor = anchors_cache.first_with_our_externals().unwrap();
    assert_eq!(anchor.id, 11);
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

    anchors_cache.pop_front();
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
    let anchor = anchors_cache.first_with_our_externals().unwrap();
    assert_eq!(anchor.id, processed_to_anchor_id);
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
    let anchor = anchors_cache.first_with_our_externals().unwrap();
    assert_eq!(anchor.id, processed_to_anchor_id);
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
        ref_mc_state_handle: tracker.insert_seqno(0),
        shards_processed_to_by_partitions: Default::default(),
        prev_mc_data: None,
    };

    //------
    // on zerostate will return None
    let anchors_proc_info_opt = CollatorStdImpl::get_anchors_processing_info(
        &shard_id,
        &mc_data,
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
        .insert(QueuePartitionIdx(0), ProcessedUptoPartitionStuff {
            externals: ExternalsProcessedUptoStuff {
                processed_to: (1752, 12000),
                ranges: [(967, ExternalsRangeInfo {
                    skip_offset: 0,
                    processed_offset: 0,
                    chain_time: mc_data.gen_chain_time,
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
        &mc_data,
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
        .insert(QueuePartitionIdx(0), ProcessedUptoPartitionStuff {
            externals: ExternalsProcessedUptoStuff {
                processed_to: (1756, 7000),
                ranges: [(968, ExternalsRangeInfo {
                    skip_offset: 0,
                    processed_offset: 0,
                    chain_time: mc_data.gen_chain_time,
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
    let anchors_proc_info_opt = CollatorStdImpl::get_anchors_processing_info(
        &shard_id,
        &mc_data,
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
        .insert(QueuePartitionIdx(0), ProcessedUptoPartitionStuff {
            externals: ExternalsProcessedUptoStuff {
                processed_to: (1816, 23429),
                ranges: [(1005, ExternalsRangeInfo {
                    skip_offset: 0,
                    processed_offset: 0,
                    chain_time: mc_data.gen_chain_time,
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
    let anchors_proc_info_opt = CollatorStdImpl::get_anchors_processing_info(
        &shard_id,
        &mc_data,
        &prev_block_id,
        prev_gen_chain_time,
        prev_processed_upto_externals.processed_to,
    );
    assert!(anchors_proc_info_opt.is_some());
    let anchors_proc_info = anchors_proc_info_opt.unwrap();
    let min_externals_processed_to = mc_data
        .processed_upto
        .get_min_externals_processed_to()
        .unwrap_or_default();
    assert_eq!(
        anchors_proc_info.processed_to_anchor_id, min_externals_processed_to.0,
        "prev_block_id: {prev_block_id:?}, prev_gen_chain_time: {prev_gen_chain_time},
        prev_processed_upto_externals: {prev_processed_upto_externals:?}, mc_data: {mc_data:?}",
    );
    assert_eq!(
        anchors_proc_info.processed_to_msgs_offset,
        min_externals_processed_to.1,
    );
    assert_eq!(
        anchors_proc_info.last_imported_chain_time,
        mc_data.gen_chain_time,
    );
    assert_eq!(
        anchors_proc_info.last_imported_in_block_id,
        mc_data.block_id,
    );
}

struct MockEventListener {
    accepted_count: Arc<AtomicUsize>,
}

#[async_trait]
impl StateNodeEventListener for MockEventListener {
    async fn on_block_accepted(&self, _block_id: &ShardStateStuff) -> Result<()> {
        self.accepted_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn on_block_accepted_external(&self, _state: &ShardStateStuff) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct DumpStateTestCollationManager {
    pub last_result: Arc<Mutex<Option<BlockCollationResult>>>,
    mc_data: Arc<Mutex<Arc<McData>>>,
    active_collators: Arc<FastDashMap<ShardIdent, AsyncQueuedDispatcher<CollatorStdImpl>>>,
    shards_info: Arc<FastDashMap<ShardIdent, Vec<BlockId>>>,
    block_produced_signal: Arc<Notify>,
    last_master_chain_time: Arc<Mutex<u64>>,
    shards_cache: Arc<FastDashMap<ShardIdent, BlockCandidate>>,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    mempool_adapter: Arc<dyn MempoolAdapter>,
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    collation_session: Arc<CollationSessionInfo>,
}

impl DumpStateTestCollationManager {
    async fn new(dump_path: &Path) -> Result<Arc<Self>> {
        let (storage, mq_adapter, _temp_dir, mc_block_id) =
            load_storage_from_dump(dump_path).await?;

        let mc_state = storage
            .shard_state_storage()
            .load_state(mc_block_id.seqno, &mc_block_id)
            .await?;
        let mc_data = McData::load_from_state(&mc_state, Default::default())?;

        let mempool_adapter = MempoolAdapterStubImpl::with_anchors_from_dump(
            Arc::new(MempoolEventStubListener),
            mc_data.top_processed_to_anchor,
            storage.context().clone().root_dir().path().join("mempool"),
        )?;

        let counter = Arc::new(AtomicUsize::new(0));

        let listener = Arc::new(MockEventListener {
            accepted_count: counter.clone(),
        });
        let state_node_adapter = Arc::new(StateNodeAdapterStdImpl::new(
            listener,
            storage.clone(),
            CollatorSyncContext::Historical,
        ));

        let keypair = Arc::new(ed25519::KeyPair::from(&ed25519::SecretKey::from_bytes(
            [0; 32],
        )));
        let vset = mc_data.config.get_current_validator_set()?;
        let (subset, hash_short) = vset
            .compute_mc_subset(0, true)
            .context("Failed to compute subset")?;

        let current_session_seqno = mc_data.validator_info.catchain_seqno;

        let collation_session = Arc::new(CollationSessionInfo::new(
            ShardIdent::MASTERCHAIN,
            current_session_seqno,
            tycho_block_util::block::ValidatorSubsetInfo {
                validators: subset.into_iter().collect(),
                short_hash: hash_short,
            },
            Some(keypair.clone()),
        ));

        let last_result = Arc::new(Mutex::new(None));

        let shards_info = FastDashMap::default();
        for (shard_id, descr) in mc_data.shards.iter() {
            let top_block_id = descr.get_block_id(*shard_id);
            shards_info.insert(*shard_id, vec![top_block_id]);
        }

        let listener = Arc::new(Self {
            mc_data: Arc::new(Mutex::new(mc_data.clone())),
            last_result,
            active_collators: Arc::new(FastDashMap::default()),
            block_produced_signal: Arc::new(Notify::new()),
            shards_info: Arc::new(shards_info),
            last_master_chain_time: Arc::new(Mutex::new(mc_data.gen_chain_time)),
            shards_cache: Arc::new(FastDashMap::default()),
            mq_adapter: mq_adapter.clone(),
            mempool_adapter: mempool_adapter.clone(),
            state_node_adapter: state_node_adapter.clone(),
            collation_session: collation_session.clone(),
        });

        let collator = CollatorStdImpl::start(
            mq_adapter,
            mempool_adapter,
            state_node_adapter,
            Arc::new(Default::default()),
            collation_session,
            listener.clone(),
            ShardIdent::MASTERCHAIN,
            vec![mc_block_id],
            mc_data.clone(),
            Some(MempoolGlobalConfig {
                genesis_info: GenesisInfo {
                    start_round: mc_data.top_processed_to_anchor,
                    genesis_millis: 0,
                },
                consensus_config: None,
            }),
            Arc::new(Notify::new()),
            None,
        )
        .await?;

        listener
            .active_collators
            .insert(mc_block_id.shard, collator);

        Ok(listener)
    }

    pub fn get_top_shard_blocks_info_for_mc_block(
        &self,
        next_mc_block_id_short: BlockIdShort,
    ) -> Result<Vec<TopBlockDescription>> {
        let mut result = vec![];
        for r in self.shards_cache.iter() {
            if r.ref_by_mc_seqno == next_mc_block_id_short.seqno {
                let processed_to_by_partitions =
                    r.processed_upto.get_internals_processed_to_by_partitions();

                let proof_funds = ShardFeeCreated {
                    create: r.value_flow.created.clone(),
                    fees: r.value_flow.fees_collected.clone(),
                };

                result.push(TopBlockDescription {
                    block_id: *r.block.id(),
                    block_info: r.block.load_info()?.clone(),
                    processed_to_anchor_id: r.processed_to_anchor_id,
                    value_flow: r.value_flow.clone(),
                    proof_funds,
                    #[cfg(feature = "block-creator-stats")]
                    creators: vec![],
                    processed_to_by_partitions,
                });
                break;
            }
        }

        Ok(result)
    }
}

#[async_trait::async_trait]
impl CollatorEventListener for DumpStateTestCollationManager {
    async fn on_skipped(
        &self,
        _prev_mc_block_id: BlockId,
        next_block_id_short: BlockIdShort,
        anchor_chain_time: u64,
        force_mc_block: ForceMasterCollation,
        collation_config: Arc<CollationConfig>,
    ) -> Result<()> {
        let mc_block_max_interval_ms = collation_config.mc_block_max_interval_ms as u64;

        tracing::info!(
            "Block skipped: shard={}, seqno={}, anchor_chain_time={}, force_mc_block={:?}",
            next_block_id_short.shard,
            next_block_id_short.seqno,
            anchor_chain_time,
            force_mc_block
        );

        let time_between_master_collation = mc_block_max_interval_ms;

        let last_master_chain_time = *self.last_master_chain_time.lock();
        let collate_next_master_block =
            anchor_chain_time > last_master_chain_time + time_between_master_collation;

        // Signal that collation was skipped
        // self.block_produced_signal.notify_one();

        // Continue with next collation step similar to on_block_candidate logic
        if collate_next_master_block {
            let mc_collator = self
                .active_collators
                .get(&ShardIdent::MASTERCHAIN)
                .unwrap()
                .clone();

            let top_shard_blocks_info =
                self.get_top_shard_blocks_info_for_mc_block(next_block_id_short)?;

            let next_mc_block_chain_time = anchor_chain_time;

            mc_collator
                .enqueue_do_collate(top_shard_blocks_info, next_mc_block_chain_time)
                .await?;
        } else {
            // Resume collation for all shards like in on_block_candidate
            let mc_data = self.mc_data.lock().clone();
            let current_session_seqno = mc_data.validator_info.catchain_seqno;

            for s in self.shards_info.iter() {
                let shard_id = *s.key();
                let prev_blocks_ids = s.value().clone();
                let collation_session = Arc::new(CollationSessionInfo::new(
                    shard_id,
                    current_session_seqno,
                    self.collation_session.collators().clone(),
                    self.collation_session.current_collator_keypair().cloned(),
                ));

                if let Some(collator) = self.active_collators.get(&shard_id) {
                    let reset_collators = false;
                    collator
                        .enqueue_resume_collation(
                            mc_data.clone(),
                            reset_collators,
                            collation_session,
                            prev_blocks_ids,
                        )
                        .await?;
                } else {
                    let collator = CollatorStdImpl::start(
                        self.mq_adapter.clone(),
                        self.mempool_adapter.clone(),
                        self.state_node_adapter.clone(),
                        Arc::new(Default::default()),
                        collation_session,
                        Arc::new(self.clone()),
                        shard_id,
                        prev_blocks_ids,
                        mc_data.clone(),
                        Some(MempoolGlobalConfig {
                            genesis_info: GenesisInfo {
                                start_round: mc_data.top_processed_to_anchor,
                                genesis_millis: 0,
                            },
                            consensus_config: None,
                        }),
                        Arc::new(Notify::new()),
                        None,
                    )
                    .await?;

                    self.active_collators.insert(shard_id, collator);
                }
            }
        }

        Ok(())
    }
    async fn on_cancelled(
        &self,
        _prev_mc_block_id: BlockId,
        _next_block_id_short: BlockIdShort,
        _cancel_reason: crate::collator::CollationCancelReason,
    ) -> Result<()> {
        Ok(())
    }
    async fn on_block_candidate(&self, collation_result: BlockCollationResult) -> Result<()> {
        let block_id = *collation_result.candidate.block.id();
        let load_info = collation_result.candidate.block.load_info()?;
        let chain_time = load_info.gen_utime as u64 * 1000 + load_info.gen_utime_ms as u64;
        let mc_block_max_interval_ms =
            collation_result.collation_config.mc_block_max_interval_ms as u64;
        let mc_data = if let Some(mc_data) = collation_result.mc_data.clone() {
            mc_data
        } else {
            self.mc_data.lock().clone()
        };
        let next_mc_block_id_short = mc_data.block_id.get_next_id_short();
        let candidate = *collation_result.candidate.clone();
        let current_session_seqno = mc_data.validator_info.catchain_seqno;
        let processed_to_anchor_id = candidate.processed_to_anchor_id;

        tracing::info!(
            "Block candidate received: shard={}, seqno={}, chain_time={}",
            block_id.shard,
            block_id.seqno,
            chain_time
        );

        // 1. Save collation result
        *self.last_result.lock() = Some(collation_result);
        *self.mc_data.lock() = mc_data.clone();

        let time_between_master_collation = mc_block_max_interval_ms; // TODO: check

        let last_master_chain_time = *self.last_master_chain_time.lock();
        let collate_next_master_block =
            chain_time > last_master_chain_time + time_between_master_collation;

        // 2. Check if this is a master block or shard block
        if block_id.shard.is_masterchain() {
            // Master block - update last master block chain time
            *self.last_master_chain_time.lock() = chain_time;
            tracing::info!(
                "Master block collated: seqno={}, chain_time={}",
                block_id.seqno,
                chain_time
            );
        } else {
            tracing::info!(
                "Shard block collated: shard={}, seqno={}",
                block_id.shard,
                block_id.seqno
            );
            self.shards_info.insert(block_id.shard, vec![block_id]);
            self.shards_cache.insert(block_id.shard, candidate);
        }

        // 3. Signal that a block was produced
        self.block_produced_signal.notify_one();

        // 4. Collate next block
        if collate_next_master_block {
            let mc_collator = self
                .active_collators
                .get(&ShardIdent::MASTERCHAIN)
                .unwrap()
                .clone();

            let top_shard_blocks_info =
                self.get_top_shard_blocks_info_for_mc_block(next_mc_block_id_short)?;

            let next_mc_block_chain_time = if !block_id.shard.is_masterchain() {
                chain_time
            } else {
                self.mempool_adapter
                    .get_next_anchor(processed_to_anchor_id)
                    .await?
                    .anchor()
                    .ok_or_else(|| {
                        anyhow::anyhow!("Failed to get next anchor ({})", processed_to_anchor_id)
                    })?
                    .chain_time
            };

            mc_collator
                .enqueue_do_collate(top_shard_blocks_info, next_mc_block_chain_time)
                .await?;
        } else {
            for s in self.shards_info.iter() {
                let shard_id = *s.key();
                let prev_blocks_ids = s.value().clone();
                let collation_session = Arc::new(CollationSessionInfo::new(
                    shard_id,
                    current_session_seqno,
                    self.collation_session.collators().clone(),
                    self.collation_session.current_collator_keypair().cloned(),
                ));

                if let Some(collator) = self.active_collators.get(&shard_id) {
                    let reset_collators = false;
                    collator
                        .enqueue_resume_collation(
                            mc_data.clone(),
                            reset_collators,
                            collation_session,
                            prev_blocks_ids,
                        )
                        .await?;
                } else {
                    let collator = CollatorStdImpl::start(
                        self.mq_adapter.clone(),
                        self.mempool_adapter.clone(),
                        self.state_node_adapter.clone(),
                        Arc::new(Default::default()),
                        collation_session,
                        Arc::new(self.clone()),
                        shard_id,
                        prev_blocks_ids,
                        mc_data.clone(),
                        Some(MempoolGlobalConfig {
                            genesis_info: GenesisInfo {
                                start_round: mc_data.top_processed_to_anchor,
                                genesis_millis: 0,
                            },
                            consensus_config: None,
                        }),
                        Arc::new(Notify::new()),
                        None,
                    )
                    .await?;

                    self.active_collators.insert(shard_id, collator);
                }
            }
        }

        Ok(())
    }
    async fn on_collator_stopped(&self, _collation_session_id: CollationSessionId) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_collation_from_dump() -> Result<()> {
    try_init_test_tracing("trace".parse().unwrap());

    let dump_dir = "../test/data/dump/"; // TODO: insert real dump

    let manager = DumpStateTestCollationManager::new(Path::new(dump_dir)).await?;
    let notify = manager.block_produced_signal.clone();
    let result = manager.last_result.clone();

    // Track blocks collated for testing
    let mut blocks_collated = 0;
    let max_blocks = 5; // Limit test to a reasonable number of blocks

    loop {
        let notify = notify.clone();
        let result = result.clone();
        notify.notified().await;
        let (is_masterchain, seqno, chain_time) = {
            let lock = result.lock();
            let lock = lock.as_ref();
            let collation_result = lock.unwrap();
            let block_id = *collation_result.candidate.block.id();
            let load_info = collation_result.candidate.block.load_info()?;
            let chain_time = load_info.gen_utime as u64 + load_info.gen_utime_ms as u64;
            (block_id.shard.is_masterchain(), block_id.seqno, chain_time)
        };

        tracing::info!(
            "Processing block: seqno={}, chain_time={}, is_masterchain={}",
            seqno,
            chain_time,
            is_masterchain
        );

        blocks_collated += 1;

        // Check if this is a master chain block or we've hit max blocks
        if is_masterchain || blocks_collated >= max_blocks {
            tracing::info!("Test completed after {} blocks", blocks_collated);
            break;
        }
    }

    Ok(())
}
