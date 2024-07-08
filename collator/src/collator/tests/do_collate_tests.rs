use std::collections::VecDeque;

use everscale_types::models::*;
use everscale_types::prelude::*;

use crate::collator::types::{BlockCollationDataBuilder, CachedMempoolAnchor};
use crate::collator::CollatorStdImpl;
use crate::mempool::_stub_create_random_anchor_with_stub_externals;
use crate::test_utils::try_init_test_tracing;

#[test]
fn test_read_next_externals() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let mut anchors_cache = VecDeque::new();

    let shard_id = ShardIdent::new_full(0);

    for anchor_id in 1..=40 {
        if anchor_id % 4 != 0 {
            continue;
        }
        let anchor = _stub_create_random_anchor_with_stub_externals(anchor_id);
        let has_externals = anchor.check_has_externals_for(&shard_id);
        tracing::trace!(
            "anchor (id: {}, chain_time: {}, externals_count: {}): has_externals for shard {}: {}, externals dst: {:?}",
            anchor_id,
            anchor.chain_time(),
            anchor.externals_count(),
            shard_id,
            has_externals,
            anchor
                .externals_iterator(0)
                .map(|ext_msg| ext_msg.info().dst.to_string())
                .collect::<Vec<_>>()
                .as_slice(),
        );
        anchors_cache.push_back((anchor_id, CachedMempoolAnchor {
            anchor,
            has_externals,
        }));
    }

    let mut collation_data = BlockCollationDataBuilder::new(
        BlockIdShort {
            shard: shard_id,
            seqno: 1,
        },
        HashBytes::ZERO,
        1,
        0,
        Default::default(),
        HashBytes::ZERO,
    )
    .build(0, DEFAULT_BLOCK_LIMITS);

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        3,
        &mut collation_data,
        false,
    )
    .unwrap();

    assert_eq!(externals.len(), 3);
    assert!(has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (4, 0));
    assert_eq!(ext_processed_upto.read_to, (4, 3));
    let kv = anchors_cache.front().unwrap();
    assert_eq!(kv.0, 4);

    collation_data.processed_upto.externals = Some(ExternalsProcessedUpto {
        processed_to: (4, 3),
        read_to: (8, 1),
    });

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        3,
        &mut collation_data,
        false,
    )
    .unwrap();

    assert_eq!(externals.len(), 3);
    assert!(has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (4, 3));
    assert_eq!(ext_processed_upto.read_to, (8, 3));
    let kv = anchors_cache.front().unwrap();
    assert_eq!(kv.0, 4);

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        10,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 10);
    assert!(has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (4, 3));
    assert_eq!(ext_processed_upto.read_to, (16, 6));
    let kv = anchors_cache.front().unwrap();
    assert_eq!(kv.0, 8);

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        20,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 16);
    assert!(!has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (4, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    let kv = anchors_cache.front().unwrap();
    assert_eq!(kv.0, 20);

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        3,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 0);
    assert!(!has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (4, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    let kv = anchors_cache.front();
    assert!(kv.is_none());

    // all anchors removed from cache, should not fail on empty cache
    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        3,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 0);
    assert!(!has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (4, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    let kv = anchors_cache.front();
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
