use std::sync::Arc;

use everscale_types::models::*;
use everscale_types::prelude::*;

use crate::collator::types::{
    BlockCollationData, BlockCollationDataBuilder, ParsedExternals, ReadNextExternalsMode,
};
use crate::collator::{AnchorsCache, CollatorStdImpl};
use crate::mempool::make_stub_anchor;
use crate::test_utils::try_init_test_tracing;
use crate::types::supported_capabilities;

pub(crate) fn fill_test_anchors_cache(anchors_cache: &mut AnchorsCache, shard_id: ShardIdent) {
    for anchor_id in 1..=40 {
        if anchor_id % 4 != 0 {
            continue;
        }
        let anchor = Arc::new(make_stub_anchor(anchor_id));
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
        hard_limit: 20_000_000,
    },
    lt_delta: BlockParamLimits {
        underload: 1000,
        soft_limit: 5000,
        hard_limit: 10000,
    },
};

pub(crate) fn build_stub_collation_data(
    next_block_id: BlockIdShort,
    anchors_cache: &AnchorsCache,
    start_lt: u64,
) -> BlockCollationData {
    BlockCollationDataBuilder::new(
        next_block_id,
        HashBytes::ZERO,
        1,
        anchors_cache
            .get_last_imported_anchor_ct()
            .unwrap_or_default(),
        Default::default(),
        HashBytes::ZERO,
        GlobalVersion {
            version: 50,
            capabilities: supported_capabilities(),
        },
        None,
    )
    .build(start_lt, DEFAULT_BLOCK_LIMITS)
}

#[test]
fn test_read_next_externals() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let shard_id = ShardIdent::new_full(0);
    let next_block_id_short = BlockIdShort {
        shard: shard_id,
        seqno: 1,
    };

    let mut anchors_cache = AnchorsCache::default();
    fill_test_anchors_cache(&mut anchors_cache, shard_id);

    let mut collation_data = build_stub_collation_data(next_block_id_short, &anchors_cache, 0);

    let ParsedExternals {
        ext_messages: externals,
        current_reader_position,
        ..
    } = CollatorStdImpl::read_next_externals(
        &shard_id,
        &mut anchors_cache,
        3,
        collation_data.get_gen_chain_time(),
        &mut collation_data.processed_upto.externals,
        None,
        ReadNextExternalsMode::ToTheEnd,
    )
    .unwrap();

    assert_eq!(externals.len(), 3);
    assert!(anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (4, 0));
    assert_eq!(ext_processed_upto.read_to, (8, 3));
    assert_eq!(current_reader_position, Some((8, 3)));
    let kv = anchors_cache.get(0).unwrap();
    assert_eq!(kv.0, 8);

    // check stop_on_prev_read_to
    collation_data.processed_upto.externals = Some(ExternalsProcessedUpto {
        processed_to: (8, 3),
        read_to: (12, 1),
    });

    // on 1st read should start reading from processed_to
    // should exit before reaching read_to
    let ParsedExternals {
        ext_messages: externals,
        current_reader_position,
        ..
    } = CollatorStdImpl::read_next_externals(
        &shard_id,
        &mut anchors_cache,
        3,
        collation_data.get_gen_chain_time(),
        &mut collation_data.processed_upto.externals,
        None,
        ReadNextExternalsMode::ToPreviuosReadTo,
    )
    .unwrap();

    assert_eq!(externals.len(), 3);
    assert!(anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (12, 1));
    assert_eq!(current_reader_position, Some((8, 8)));
    let kv = anchors_cache.get(0).unwrap();
    assert_eq!(kv.0, 12);

    // on 2d read should start reading from current_reader_position
    // should stop reading on read_to and exit before 3 messages collected
    let ParsedExternals {
        ext_messages: externals,
        current_reader_position,
        ..
    } = CollatorStdImpl::read_next_externals(
        &shard_id,
        &mut anchors_cache,
        3,
        collation_data.get_gen_chain_time(),
        &mut collation_data.processed_upto.externals,
        current_reader_position,
        ReadNextExternalsMode::ToPreviuosReadTo,
    )
    .unwrap();

    assert_eq!(externals.len(), 1);
    assert!(anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (12, 1));
    assert_eq!(current_reader_position, Some((12, 1)));
    let kv = anchors_cache.get(0).unwrap();
    assert_eq!(kv.0, 12);

    // continue reading
    let ParsedExternals {
        ext_messages: externals,
        current_reader_position,
        ..
    } = CollatorStdImpl::read_next_externals(
        &shard_id,
        &mut anchors_cache,
        9,
        collation_data.get_gen_chain_time(),
        &mut collation_data.processed_upto.externals,
        current_reader_position,
        ReadNextExternalsMode::ToTheEnd,
    )
    .unwrap();

    assert_eq!(externals.len(), 9);
    assert!(anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (24, 3));
    assert_eq!(current_reader_position, Some((24, 3)));
    let kv = anchors_cache.get(0).unwrap();
    assert_eq!(kv.0, 24);

    let ParsedExternals {
        ext_messages: externals,
        current_reader_position,
        ..
    } = CollatorStdImpl::read_next_externals(
        &shard_id,
        &mut anchors_cache,
        20,
        collation_data.get_gen_chain_time(),
        &mut collation_data.processed_upto.externals,
        current_reader_position,
        ReadNextExternalsMode::ToTheEnd,
    )
    .unwrap();

    assert_eq!(externals.len(), 13);
    assert!(!anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    assert_eq!(current_reader_position, Some((40, 0)));
    let kv = anchors_cache.get(0);
    assert!(kv.is_none());

    // all anchors removed from cache, should not fail on empty cache
    let ParsedExternals {
        ext_messages: externals,
        current_reader_position,
        ..
    } = CollatorStdImpl::read_next_externals(
        &shard_id,
        &mut anchors_cache,
        3,
        collation_data.get_gen_chain_time(),
        &mut collation_data.processed_upto.externals,
        current_reader_position,
        ReadNextExternalsMode::ToTheEnd,
    )
    .unwrap();

    assert_eq!(externals.len(), 0);
    assert!(!anchors_cache.has_pending_externals());
    let ext_processed_upto = collation_data.processed_upto.externals.as_ref().unwrap();
    assert_eq!(ext_processed_upto.processed_to, (8, 3));
    assert_eq!(ext_processed_upto.read_to, (40, 0));
    assert_eq!(current_reader_position, Some((40, 0)));
    let kv = anchors_cache.get(0);
    assert!(kv.is_none());
}
