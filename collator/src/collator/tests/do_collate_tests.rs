use std::sync::Arc;

use async_trait::async_trait;
use everscale_types::models::*;
use everscale_types::prelude::*;

use crate::collator::types::BlockCollationDataBuilder;
use crate::collator::{AnchorsCache, CollatorStdImpl};
use crate::mempool::{
    make_stub_anchor, MempoolAdapterStubImpl, MempoolAnchor, MempoolEventListener,
};
use crate::test_utils::try_init_test_tracing;

#[test]
fn test_read_next_externals() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let mut anchors_cache = AnchorsCache::default();

    let shard_id = ShardIdent::new_full(0);

    for anchor_id in 1..=40 {
        if anchor_id % 4 != 0 {
            continue;
        }
        let anchor = make_stub_anchor(anchor_id);
        let our_exts_count = anchor.count_externals_for(&shard_id, 0);
        let has_externals = our_exts_count > 0;
        if has_externals {
            tracing::trace!(
                "anchor (id: {}, chain_time: {}, externals_count: {}): has_externals for shard {}: {}, externals dst: {:?}",
                anchor_id,
                anchor.chain_time,
                anchor.externals.len(),
                shard_id,
                has_externals,
                anchor
                    .iter_externals(0)
                    .map(|ext_msg| ext_msg.info.dst.to_string())
                    .collect::<Vec<_>>()
                    .as_slice(),
            );
        }
        anchors_cache.insert(anchor, our_exts_count);
    }

    let mut collation_data = BlockCollationDataBuilder::new(
        BlockIdShort {
            shard: shard_id,
            seqno: 1,
        },
        HashBytes::ZERO,
        1,
        anchors_cache
            .get_last_imported_anchor_ct()
            .unwrap_or_default(),
        Default::default(),
        HashBytes::ZERO,
    )
    .build(0, DEFAULT_BLOCK_LIMITS);

    let externals = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        3,
        &mut collation_data,
        false,
    )
    .unwrap();

    assert_eq!(externals.len(), 3);
    assert!(anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (4, 0));
    assert_eq!(ext_processed_upto.read_to, (8, 3));
    let kv = anchors_cache.get(0).unwrap();
    assert_eq!(kv.0, 8);

    collation_data.processed_upto.externals = Some(ExternalsProcessedUpto {
        processed_to: (8, 3),
        read_to: (12, 1),
    });

    let externals = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        3,
        &mut collation_data,
        false,
    )
    .unwrap();

    assert_eq!(externals.len(), 3);
    assert!(anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (8, 8));
    let kv = anchors_cache.get(0).unwrap();
    assert_eq!(kv.0, 8);

    let externals = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        10,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 10);
    assert!(anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (24, 3));
    let kv = anchors_cache.get(0).unwrap();
    assert_eq!(kv.0, 12);

    let externals = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        20,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 13);
    assert!(!anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    let kv = anchors_cache.get(0).unwrap();
    assert_eq!(kv.0, 24);

    let externals = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        3,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 0);
    assert!(!anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    let kv = anchors_cache.get(0);
    assert!(kv.is_none());

    // all anchors removed from cache, should not fail on empty cache
    let externals = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        3,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 0);
    assert!(!anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    let kv = anchors_cache.get(0);
    assert!(kv.is_none());
}

const DEFAULT_BLOCK_LIMITS: BlockLimits = BlockLimits {
    bytes: BlockParamLimits {
        underload: 131072,
        soft_limit: 524288,
        hard_limit: 1048576,
    },
    gas: BlockParamLimits {
        underload: 900000,
        soft_limit: 1200000,
        hard_limit: 2000000,
    },
    lt_delta: BlockParamLimits {
        underload: 1000,
        soft_limit: 5000,
        hard_limit: 10000,
    },
};

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

    let adapter = MempoolAdapterStubImpl::with_stub_externals(Arc::new(MempoolEventStubListener));
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
