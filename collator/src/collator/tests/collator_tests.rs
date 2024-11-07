use std::sync::Arc;

use async_trait::async_trait;
use everscale_types::models::ShardIdent;

use crate::collator::types::AnchorsCache;
use crate::collator::CollatorStdImpl;
use crate::mempool::{MempoolAdapterStubImpl, MempoolAnchor, MempoolEventListener};
use crate::test_utils::try_init_test_tracing;

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

    // =========================================================================
    // Get all anchors from mempool
    // =========================================================================
    let processed_to_anchor_id = 9;
    let processed_to_msgs_offset = 0;
    let last_block_chain_time = 20832;

    let anchors_info = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset as _,
        last_block_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
        0,
    )
    .await
    .unwrap();

    tracing::debug!("imported anchors on init: {:?}", anchors_info.as_slice());

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

    // =========================================================================
    // Get all anchors from mempool. processed_to anchor is fully read
    // =========================================================================
    anchors_cache.clear();

    let processed_to_anchor_id = 9;
    let processed_to_msgs_offset = 4;
    let last_block_chain_time = 20832;

    let anchors_info = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset as _,
        last_block_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
        0,
    )
    .await
    .unwrap();

    tracing::debug!("imported anchors on init: {:?}", anchors_info.as_slice());

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

    // =========================================================================
    // processed_to anchor exists in cache, get some anchors from cache, rest from mempool
    // =========================================================================

    let processed_to_anchor_id = 12;
    let processed_to_msgs_offset = 1;
    let last_block_chain_time = 24304;

    let anchors_info = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset as _,
        last_block_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
        0,
    )
    .await
    .unwrap();

    tracing::debug!("imported anchors on init: {:?}", anchors_info.as_slice());

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

    // =========================================================================
    // processed_to anchor exists in cache, get all from cache
    // =========================================================================

    let processed_to_anchor_id = 11;
    let processed_to_msgs_offset = 2;
    let last_block_chain_time = 24304;

    let anchors_info = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset as _,
        last_block_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
        0,
    )
    .await
    .unwrap();

    tracing::debug!("imported anchors on init: {:?}", anchors_info.as_slice());

    assert_eq!(anchors_info.len(), 0);
    let (anchor_id, _) = anchors_cache.get(0).unwrap();
    assert_eq!(anchor_id, 11);
    let (last_imported_id, last_imported_ct) =
        anchors_cache.get_last_imported_anchor_id_and_ct().unwrap();
    assert_eq!(last_imported_id, 14);
    assert_eq!(last_imported_ct, 24304);
    assert_eq!(anchors_cache.len(), 4);
    assert!(anchors_cache.has_pending_externals());

    // =========================================================================
    // processed_to anchor is before all anchors in cache, should clear cache and load all required from mempool
    // =========================================================================

    let processed_to_anchor_id = 9;
    let processed_to_msgs_offset = 2;
    let last_block_chain_time = 20832;

    let anchors_info = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset as _,
        last_block_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
        0,
    )
    .await
    .unwrap();

    tracing::debug!("imported anchors on init: {:?}", anchors_info.as_slice());

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

    // =========================================================================
    // processed_to anchor is after all anchors in cache, should clear cache and load all required from mempool
    // =========================================================================

    let processed_to_anchor_id = 13;
    let processed_to_msgs_offset = 3;
    let last_block_chain_time = 29512;

    let anchors_info = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset as _,
        last_block_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
        0,
    )
    .await
    .unwrap();

    tracing::debug!("imported anchors on init: {:?}", anchors_info.as_slice());

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
}
