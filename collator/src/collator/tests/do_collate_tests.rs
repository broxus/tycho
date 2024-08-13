use std::collections::VecDeque;

use everscale_types::models::*;
use everscale_types::prelude::*;

use crate::collator::types::{AnchorInfo, BlockCollationDataBuilder, CachedMempoolAnchor};
use crate::collator::CollatorStdImpl;
use crate::mempool::make_stub_anchor;
use crate::test_utils::try_init_test_tracing;

fn get_test_block_limits() -> BlockLimits {
    BlockLimits {
        bytes: everscale_types::models::BlockParamLimits {
            underload: 1_000_000,
            soft_limit: 2_000_000,
            hard_limit: 3_000_000,
        },
        gas: everscale_types::models::BlockParamLimits {
            underload: 1_000_000,
            soft_limit: 2_000_000,
            hard_limit: 3_000_000,
        },
        lt_delta: everscale_types::models::BlockParamLimits {
            underload: 1_000_000,
            soft_limit: 2_000_000,
            hard_limit: 3_000_000,
        },
    }
}

#[test]
fn test_read_next_externals() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let mut anchors_cache = VecDeque::new();

    let shard_id = ShardIdent::new_full(0);

    let mut last_imported_anchor = None;
    for anchor_id in 1..=40 {
        if anchor_id % 4 != 0 {
            continue;
        }
        let anchor = make_stub_anchor(anchor_id);
        let our_exts_count = anchor.count_externals_for(&shard_id, 0);
        last_imported_anchor = Some(AnchorInfo::from_anchor(anchor.clone(), our_exts_count));
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
            anchors_cache.push_back((anchor_id, CachedMempoolAnchor {
                anchor,
                has_externals,
            }));
        }
    }

    let mut collation_data = BlockCollationDataBuilder::new(
        BlockIdShort {
            shard: shard_id,
            seqno: 1,
        },
        HashBytes::ZERO,
        1,
        last_imported_anchor
            .as_ref()
            .map(|a| a.ct)
            .unwrap_or_default(),
        Default::default(),
        HashBytes::ZERO,
    )
    .build(0, DEFAULT_BLOCK_LIMITS);

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        last_imported_anchor.as_ref(),
        3,
        &mut collation_data,
        false,
    )
    .unwrap();

    assert_eq!(externals.len(), 3);
    assert!(has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (4, 0));
    assert_eq!(ext_processed_upto.read_to, (8, 3));
    let kv = anchors_cache.front().unwrap();
    assert_eq!(kv.0, 8);

    collation_data.processed_upto.externals = Some(ExternalsProcessedUpto {
        processed_to: (8, 3),
        read_to: (12, 1),
    });

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        last_imported_anchor.as_ref(),
        3,
        &mut collation_data,
        false,
    )
    .unwrap();

    assert_eq!(externals.len(), 3);
    assert!(has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (8, 8));
    let kv = anchors_cache.front().unwrap();
    assert_eq!(kv.0, 8);

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        last_imported_anchor.as_ref(),
        10,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 10);
    assert!(has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (24, 3));
    let kv = anchors_cache.front().unwrap();
    assert_eq!(kv.0, 12);

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        last_imported_anchor.as_ref(),
        20,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 13);
    assert!(!has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    let kv = anchors_cache.front().unwrap();
    assert_eq!(kv.0, 24);

    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        last_imported_anchor.as_ref(),
        3,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 0);
    assert!(!has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    let kv = anchors_cache.front();
    assert!(kv.is_none());

    // all anchors removed from cache, should not fail on empty cache
    let (externals, has_pending_externals) = CollatorStdImpl::read_next_externals_impl(
        &shard_id,
        &mut anchors_cache,
        last_imported_anchor.as_ref(),
        3,
        &mut collation_data,
        true,
    )
    .unwrap();

    assert_eq!(externals.len(), 0);
    assert!(!has_pending_externals);
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
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
