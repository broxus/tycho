use std::collections::BTreeMap;

use anyhow::Result;
use everscale_types::cell::Lazy;
use everscale_types::models::{
    BlockIdShort, ExternalsRange, InternalsRange, MsgsExecutionParams, ProcessedUptoInfo,
    ProcessedUptoPartition, ShardIdent, ShardIdentFull, ShardRange,
};
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_util::{FastHashMap, FastHashSet};

use super::{ProcessedTo, ProcessedToByPartitions, ShardDescriptionShort};
use crate::mempool::MempoolAnchorId;

pub type Lt = u64;
pub type BlockSeqno = u32;

/// Processed up to info by partitions.
#[derive(Debug, Default, Clone)]
pub struct ProcessedUptoInfoStuff {
    /// We split messages by partitions.
    /// Main partition 0 and others.
    pub partitions: BTreeMap<QueuePartitionIdx, ProcessedUptoPartitionStuff>,

    /// Actual messages execution params used for collated block.
    /// They help to refill messages buffers on sync/restart and
    /// process remaning messages in queues with previous params
    /// before switching to a new params version.
    pub msgs_exec_params: Option<MsgsExecutionParams>,
}

impl ProcessedUptoInfoStuff {
    pub fn check_has_non_zero_processed_offset(&self) -> bool {
        self.partitions.values().any(|p| {
            p.externals.ranges.values().any(|r| r.processed_offset > 0)
                || p.internals.ranges.values().any(|r| r.processed_offset > 0)
        })
    }

    pub fn get_internals_processed_to_by_partitions(&self) -> ProcessedToByPartitions {
        self.partitions
            .iter()
            .map(|(&par_id, par)| (par_id, par.internals.processed_to.clone()))
            .collect()
    }

    pub fn get_min_internals_processed_to_by_shards(&self) -> ProcessedTo {
        let mut shards_min_processed_to = ProcessedTo::default();
        for par in self.partitions.values() {
            for (shard_id, key) in &par.internals.processed_to {
                shards_min_processed_to
                    .entry(*shard_id)
                    .and_modify(|min_key| *min_key = std::cmp::min(*min_key, *key))
                    .or_insert(*key);
            }
        }
        shards_min_processed_to
    }

    pub fn get_partitions(&self) -> FastHashSet<QueuePartitionIdx> {
        self.partitions.keys().copied().collect()
    }
}

impl TryFrom<ProcessedUptoInfo> for ProcessedUptoInfoStuff {
    type Error = everscale_types::error::Error;

    fn try_from(value: ProcessedUptoInfo) -> std::result::Result<Self, Self::Error> {
        let mut res = ProcessedUptoInfoStuff::default();

        for p_item in value.partitions.iter() {
            let (par_id, par) = p_item?;
            let mut par_stuff = ProcessedUptoPartitionStuff::default();

            par_stuff.externals.processed_to = par.externals.processed_to;
            for r_item in par.externals.ranges.iter() {
                let (seqno, r) = r_item?;
                par_stuff.externals.ranges.insert(seqno, r.into());
            }

            for item in par.internals.processed_to.iter() {
                let (shard_id_full, p_to) = item?;
                par_stuff
                    .internals
                    .processed_to
                    .insert(ShardIdent::try_from(shard_id_full)?, p_to.into());
            }
            for r_item in par.internals.ranges.iter() {
                let (seqno, r) = r_item?;
                let mut int_range = InternalsRangeStuff {
                    skip_offset: r.skip_offset,
                    processed_offset: r.processed_offset,
                    shards: Default::default(),
                };
                for s_r_item in r.shards.iter() {
                    let (shard_id_full, s_r) = s_r_item?;
                    int_range
                        .shards
                        .insert(ShardIdent::try_from(shard_id_full.clone())?, s_r.into());
                }
                par_stuff.internals.ranges.insert(seqno, int_range);
            }

            res.partitions.insert(par_id.into(), par_stuff);
        }

        if let Some(msgs_exec_params) = value.msgs_exec_params {
            res.msgs_exec_params = Some(msgs_exec_params.load()?);
        }

        Ok(res)
    }
}

impl TryFrom<ProcessedUptoInfoStuff> for ProcessedUptoInfo {
    type Error = everscale_types::error::Error;

    fn try_from(value: ProcessedUptoInfoStuff) -> std::result::Result<Self, Self::Error> {
        let mut res = ProcessedUptoInfo::default();

        for (par_id, par_stuff) in value.partitions {
            let mut par = ProcessedUptoPartition::default();

            par.externals.processed_to = par_stuff.externals.processed_to;
            for (seqno, r) in par_stuff.externals.ranges {
                par.externals.ranges.set(seqno, ExternalsRange::from(r))?;
            }

            for (shard_id, p_to) in par_stuff.internals.processed_to {
                par.internals
                    .processed_to
                    .set(ShardIdentFull::from(shard_id), p_to.split())?;
            }
            for (seqno, r) in par_stuff.internals.ranges {
                let mut int_range = InternalsRange {
                    skip_offset: r.skip_offset,
                    processed_offset: r.processed_offset,
                    shards: Default::default(),
                };
                for (shard_id, s_r) in r.shards {
                    int_range
                        .shards
                        .set(ShardIdentFull::from(shard_id), ShardRange::from(s_r))?;
                }
                par.internals.ranges.set(seqno, int_range)?;
            }

            res.partitions.set(par_id.0, par)?;
        }

        if let Some(msgs_exec_params) = value.msgs_exec_params {
            res.msgs_exec_params = Some(Lazy::new(&msgs_exec_params)?);
        }

        Ok(res)
    }
}

/// Processed up to info for externals and internals in one partition.
#[derive(Debug, Default, Clone)]
pub struct ProcessedUptoPartitionStuff {
    /// Externals read ranges and processed to info.
    pub externals: ExternalsProcessedUptoStuff,

    /// Internals read ranges and processed to info.
    pub internals: InternalsProcessedUptoStuff,
}

#[derive(Default, Clone)]
pub struct ExternalsProcessedUptoStuff {
    pub processed_to: (MempoolAnchorId, u64),
    pub ranges: BTreeMap<BlockSeqno, ExternalsRangeInfo>,
}

impl std::fmt::Debug for ExternalsProcessedUptoStuff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct ExternalsRangesInfo {
            seqno: String,
            processed_offset: String,
            skip_offset: String,
            chain_time: String,
            from: (MempoolAnchorId, u64),
            to: (MempoolAnchorId, u64),
        }

        let (first_seqno, first_skip_offset, first_processed_offset, first_ct, first_from) = self
            .ranges
            .first_key_value()
            .map(|(seqno, r)| {
                (
                    *seqno,
                    r.skip_offset,
                    r.processed_offset,
                    r.chain_time,
                    r.from,
                )
            })
            .unwrap_or_default();
        let (last_seqno, last_skip_offset, last_processed_offset, last_ct, last_to) = self
            .ranges
            .last_key_value()
            .map(|(seqno, r)| {
                (
                    *seqno,
                    r.skip_offset,
                    r.processed_offset,
                    r.chain_time,
                    r.to,
                )
            })
            .unwrap_or_default();

        let ranges = ExternalsRangesInfo {
            seqno: format!("{}-{}", first_seqno, last_seqno),
            processed_offset: format!("{}-{}", first_processed_offset, last_processed_offset),
            skip_offset: format!("{}-{}", first_skip_offset, last_skip_offset),
            chain_time: format!("{}-{}", first_ct, last_ct),
            from: first_from,
            to: last_to,
        };

        f.debug_struct("ExternalsProcessedUptoStuff")
            .field("processed_to", &self.processed_to)
            .field("ranges", &ranges)
            .finish()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ExternalsRangeInfo {
    pub from: (MempoolAnchorId, u64),
    pub to: (MempoolAnchorId, u64),
    pub chain_time: u64,
    pub skip_offset: u32,
    pub processed_offset: u32,
}

impl From<everscale_types::models::shard::ExternalsRange> for ExternalsRangeInfo {
    fn from(value: everscale_types::models::shard::ExternalsRange) -> Self {
        Self {
            from: value.from,
            to: value.to,
            chain_time: value.chain_time,
            skip_offset: value.skip_offset,
            processed_offset: value.processed_offset,
        }
    }
}
impl From<ExternalsRangeInfo> for everscale_types::models::shard::ExternalsRange {
    fn from(value: ExternalsRangeInfo) -> Self {
        Self {
            from: value.from,
            to: value.to,
            chain_time: value.chain_time,
            skip_offset: value.skip_offset,
            processed_offset: value.processed_offset,
        }
    }
}

#[derive(Default, Clone)]
pub struct InternalsProcessedUptoStuff {
    pub processed_to: ProcessedTo,
    pub ranges: BTreeMap<BlockSeqno, InternalsRangeStuff>,
}

impl std::fmt::Debug for InternalsProcessedUptoStuff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct InternalsRangesInfo {
            seqno: String,
            processed_offset: String,
            skip_offset: String,
            shards: BTreeMap<ShardIdent, ShardRangeInfo>,
        }

        let (first_seqno, first_skip_offset, first_processed_offset, mut first_shards) = self
            .ranges
            .first_key_value()
            .map(|(seqno, r)| (*seqno, r.skip_offset, r.processed_offset, r.shards.clone()))
            .unwrap_or_default();
        let (last_seqno, last_skip_offset, last_processed_offset, last_shards) = self
            .ranges
            .last_key_value()
            .map(|(seqno, r)| (*seqno, r.skip_offset, r.processed_offset, r.shards.clone()))
            .unwrap_or_default();

        for (shard_id, last_shard_range) in last_shards {
            first_shards
                .entry(shard_id)
                .and_modify(|r| r.to = last_shard_range.to)
                .or_insert(last_shard_range);
        }

        let ranges = InternalsRangesInfo {
            seqno: format!("{}-{}", first_seqno, last_seqno),
            processed_offset: format!("{}-{}", first_processed_offset, last_processed_offset),
            skip_offset: format!("{}-{}", first_skip_offset, last_skip_offset),
            shards: first_shards,
        };

        f.debug_struct("PartitionProcessedUptoStuff")
            .field("processed_to", &self.processed_to)
            .field("ranges", &ranges)
            .finish()
    }
}

#[derive(Debug, Default, Clone)]
pub struct InternalsRangeStuff {
    pub skip_offset: u32,
    pub processed_offset: u32,
    pub shards: BTreeMap<ShardIdent, ShardRangeInfo>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ShardRangeInfo {
    pub from: Lt,
    pub to: Lt,
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

pub trait ProcessedUptoInfoExtension {
    fn get_min_externals_processed_to(&self) -> Result<(MempoolAnchorId, u64)>;
}

impl ProcessedUptoInfoExtension for ProcessedUptoInfo {
    fn get_min_externals_processed_to(&self) -> Result<(MempoolAnchorId, u64)> {
        let mut min_opt: Option<(MempoolAnchorId, u64)> = None;
        for item in self.partitions.iter() {
            let (_, par) = item?;
            match min_opt.as_mut() {
                Some(min) => {
                    if par.externals.processed_to < *min {
                        *min = par.externals.processed_to;
                    }
                }
                None => min_opt = Some(par.externals.processed_to),
            };
        }
        Ok(min_opt.unwrap_or_default())
    }
}

impl ProcessedUptoInfoExtension for ProcessedUptoInfoStuff {
    fn get_min_externals_processed_to(&self) -> Result<(MempoolAnchorId, u64)> {
        let min_opt = self
            .partitions
            .values()
            .map(|par| par.externals.processed_to)
            .min_by(|(x_id, x_offset), (y_id, y_offset)| match x_id.cmp(y_id) {
                std::cmp::Ordering::Equal => x_offset.cmp(y_offset),
                ord => ord,
            });
        Ok(min_opt.unwrap_or_default())
    }
}

pub fn build_all_shards_processed_to_by_partitions(
    next_block_id_short: BlockIdShort,
    current_shard_processed_to: ProcessedToByPartitions,
    mc_processed_to: ProcessedToByPartitions,
    mc_shards_processed_to: FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,
    mc_shards: &[(ShardIdent, ShardDescriptionShort)],
) -> FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)> {
    let mut res = mc_shards_processed_to;

    if !next_block_id_short.shard.is_masterchain() {
        // add processed_to from master
        res.insert(ShardIdent::MASTERCHAIN, (true, mc_processed_to));
    }

    // detect if shard is updated or not
    let current_shard_updated = if next_block_id_short.shard.is_masterchain() {
        true
    } else {
        let (_, shard_descr) = mc_shards
            .iter()
            .find(|(shard_id, _)| *shard_id == next_block_id_short.shard)
            .unwrap();
        // shard was updated in master
        shard_descr.top_sc_block_updated
        // next block is after the first one after master
        || (next_block_id_short.seqno - shard_descr.seqno) > 1
    };

    // replace processed_to from current shard
    res.insert(
        next_block_id_short.shard,
        (current_shard_updated, current_shard_processed_to),
    );

    res
}

pub fn find_min_processed_to_by_shards(
    all_shards_processed_to_by_partitions: &FastHashMap<
        ShardIdent,
        (bool, ProcessedToByPartitions),
    >,
) -> ProcessedTo {
    let mut result = ProcessedTo::default();

    for (_, shard_processed_to_by_partitions) in all_shards_processed_to_by_partitions
        .values()
        .filter(|(updated, _)| *updated)
    {
        for partition_processed_to in shard_processed_to_by_partitions.values() {
            for (&shard_id, &to_key) in partition_processed_to {
                result
                    .entry(shard_id)
                    .and_modify(|min| *min = std::cmp::min(*min, to_key))
                    .or_insert(to_key);
            }
        }
    }

    result
}
