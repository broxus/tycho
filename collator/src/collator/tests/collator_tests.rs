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

    let processed_to_anchor_id = 1;
    let processed_to_msgs_offset = 0;
    let last_block_chain_time = 0;
    let shard_id = ShardIdent::default();
    let mut anchors_cache = AnchorsCache::default();
    let now = tycho_util::time::now_millis();

    let adapter =
        MempoolAdapterStubImpl::with_stub_externals(Arc::new(MempoolEventStubListener), Some(now));
    let mpool_adapter = adapter;

    // =========================================================================
    // Get first anchor from mempool
    // =========================================================================
    let anchors_info = CollatorStdImpl::import_init_anchors(
        processed_to_anchor_id,
        processed_to_msgs_offset as _,
        last_block_chain_time,
        shard_id,
        &mut anchors_cache,
        mpool_adapter.clone(),
    )
    .await
    .unwrap();
    assert_eq!(anchors_info.len(), 1);
    assert!(anchors_cache.get(0).is_some());
    assert!(anchors_cache.get_last_imported_anchor_ct().is_some());
    assert!(anchors_cache.has_pending_externals());

    // =========================================================================

    // =========================================================================
    // Get first anchor from cache
    // =========================================================================
}
