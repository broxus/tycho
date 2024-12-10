use std::collections::BTreeMap;

use everscale_types::models::{
    ExternalsProcessedUpto, ExternalsRange, InternalsProcessedUpto, InternalsRange,
    PartitionProcessedUpto, ProcessedUptoInfo, ShardIdent, ShardIdentFull, ShardRange,
};
use tycho_block_util::queue::QueueKey;

use crate::mempool::MempoolAnchorId;

pub type PartitionId = u8;
type BlockSeqno = u32;

/// Processed up to info for externals and internals.
#[derive(Debug, Default, Clone)]
pub struct ProcessedUptoInfoStuffV2 {
    /// Externals read range and processed to info.
    pub externals: ExternalsProcessedUptoStuff,

    /// We split internals storage by partitions.
    /// There are at least 2: normal and low-priority.
    /// And inside partitions we split messages by source shard.
    pub internals: InternalsProcessedUptoStuffV2,
}

impl ProcessedUptoInfoStuffV2 {
    pub fn check_has_non_zero_processed_offset(&self) -> bool {
        let check_externals = self
            .externals
            .ranges
            .iter()
            .any(|(_, r)| r.processed_offset > 0);

        if check_externals {
            return check_externals;
        }

        let check_internals = self
            .internals
            .partitions
            .iter()
            .any(|(_, p)| p.ranges.iter().any(|(_, r)| r.processed_offset > 0));

        check_internals
    }
}

impl TryFrom<ProcessedUptoInfo> for ProcessedUptoInfoStuffV2 {
    type Error = everscale_types::error::Error;

    fn try_from(value: ProcessedUptoInfo) -> std::result::Result<Self, Self::Error> {
        let mut externals = ExternalsProcessedUptoStuff {
            processed_to: value.externals.processed_to,
            ranges: Default::default(),
        };
        for r_item in value.externals.ranges.iter() {
            let (seqno, r) = r_item?;
            externals.ranges.insert(seqno, r.into());
        }

        let mut internals = InternalsProcessedUptoStuffV2::default();
        for par_item in value.internals.partitions.iter() {
            let (par_id, par) = par_item?;
            let mut partition = PartitionProcessedUptoStuff::default();
            for item in par.processed_to.iter() {
                let (shard_id_full, p_to) = item?;
                partition
                    .processed_to
                    .insert(ShardIdent::try_from(shard_id_full)?, p_to.into());
            }
            for r_item in par.ranges.iter() {
                let (seqno, r) = r_item?;
                let mut int_range = InternalsRangeStuff {
                    processed_offset: r.processed_offset,
                    shards: Default::default(),
                };
                for s_r_item in r.shards.iter() {
                    let (shard_id_full, s_r) = s_r_item?;
                    int_range
                        .shards
                        .insert(ShardIdent::try_from(shard_id_full.clone())?, s_r.into());
                }
                partition.ranges.insert(seqno, int_range);
            }
            internals.partitions.insert(par_id, partition);
        }

        Ok(Self {
            externals,
            internals,
        })
    }
}

impl TryFrom<ProcessedUptoInfoStuffV2> for ProcessedUptoInfo {
    type Error = everscale_types::error::Error;

    fn try_from(value: ProcessedUptoInfoStuffV2) -> std::result::Result<Self, Self::Error> {
        let mut externals = ExternalsProcessedUpto {
            processed_to: value.externals.processed_to,
            ranges: Default::default(),
        };
        for (seqno, r) in value.externals.ranges {
            externals.ranges.set(seqno, ExternalsRange::from(r));
        }

        let mut internals = InternalsProcessedUpto::default();
        for (par_id, par) in value.internals.partitions {
            let mut partition = PartitionProcessedUpto::default();
            for (shard_id, p_to) in par.processed_to {
                partition
                    .processed_to
                    .set(ShardIdentFull::from(shard_id), p_to.split())?;
            }
            for (seqno, r) in par.ranges {
                let mut int_range = InternalsRange {
                    processed_offset: r.processed_offset,
                    shards: Default::default(),
                };
                for (shard_id, s_r) in r.shards {
                    int_range
                        .shards
                        .set(ShardIdentFull::from(shard_id), ShardRange::from(s_r))?;
                }
                partition.ranges.set(seqno, int_range)?;
            }
            internals.partitions.set(par_id, partition)?;
        }

        Ok(Self {
            externals,
            internals,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct ExternalsProcessedUptoStuff {
    pub processed_to: (MempoolAnchorId, u64),
    pub ranges: BTreeMap<BlockSeqno, ExternalsRangeInfo>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ExternalsRangeInfo {
    pub from: (MempoolAnchorId, u64),
    pub to: (MempoolAnchorId, u64),
    pub ct: u64,
    pub processed_offset: u16,
}

impl From<everscale_types::models::shard::ExternalsRange> for ExternalsRangeInfo {
    fn from(value: everscale_types::models::shard::ExternalsRange) -> Self {
        Self {
            from: value.from,
            to: value.to,
            ct: value.ct,
            processed_offset: value.processed_offset,
        }
    }
}
impl From<ExternalsRangeInfo> for everscale_types::models::shard::ExternalsRange {
    fn from(value: ExternalsRangeInfo) -> Self {
        Self {
            from: value.from,
            to: value.to,
            ct: value.ct,
            processed_offset: value.processed_offset,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct InternalsProcessedUptoStuffV2 {
    pub partitions: BTreeMap<PartitionId, PartitionProcessedUptoStuff>,
}

#[derive(Debug, Default, Clone)]
pub struct PartitionProcessedUptoStuff {
    pub processed_to: BTreeMap<ShardIdent, QueueKey>,
    pub ranges: BTreeMap<BlockSeqno, InternalsRangeStuff>,
}

#[derive(Debug, Default, Clone)]
pub struct InternalsRangeStuff {
    pub processed_offset: u16,
    pub shards: BTreeMap<ShardIdent, ShardRangeInfo>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ShardRangeInfo {
    pub from: u64,
    pub to: u64,
}

impl From<everscale_types::models::shard::ShardRange> for ShardRangeInfo {
    fn from(value: everscale_types::models::shard::ShardRange) -> Self {
        Self {
            from: value.from,
            to: value.to,
        }
    }
}
impl From<ShardRangeInfo> for everscale_types::models::shard::ShardRange {
    fn from(value: ShardRangeInfo) -> Self {
        Self {
            from: value.from,
            to: value.to,
        }
    }
}
