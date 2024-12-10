use std::collections::BTreeMap;

use everscale_types::models::{ExternalsProcessedUpto, ShardIdent};
use tycho_block_util::queue::QueueKey;

use crate::mempool::MempoolAnchorId;

type BlockSeqno = u32;

/// Processed up to info for externals and internals.
#[derive(Default, Clone)]
pub struct ProcessedUptoInfoStuff {
    /// Externals read range and processed to info
    pub externals: ExternalsProcessedUptoStuff,

    /// We split internals storage by partitions.
    /// There are at least 2: normal and low-priority.
    /// And inside partitions we split messages by source shard.
    pub internals: InternalsProcessedUptoStuff,
}

#[derive(Default, Clone)]
pub struct ExternalsProcessedUptoStuff {
    pub range: ExternalsRangeInfo,
    pub processed_to: (MempoolAnchorId, u64),
}

#[derive(Default, Clone, Copy)]
pub struct ExternalsRangeInfo {
    pub from: (MempoolAnchorId, u64),
    pub to: (MempoolAnchorId, u64),
    pub offset: u16,
}

#[derive(Default, Clone)]
pub struct InternalsProcessedUptoStuff {
    pub partitions: BTreeMap<u8, PartitionProcessedUptoStuff>,
}

#[derive(Default, Clone)]
pub struct PartitionProcessedUptoStuff {
    pub ranges: BTreeMap<BlockSeqno, InternalsRange>,
    pub processed_to: BTreeMap<ShardIdent, QueueKey>,
}

#[derive(Default, Clone)]
pub struct InternalsRange {
    pub shards: BTreeMap<ShardIdent, ShardRange>,
    pub offset: u16,
}

#[derive(Default, Clone, Copy)]
pub struct ShardRange {
    pub from: u64,
    pub to: u64,
}
