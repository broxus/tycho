use std::collections::BTreeMap;

use anyhow::Context;
use tycho_block_util::queue::QueuePartitionIdx;
use tycho_util::FastHashSet;

use crate::collator::messages_buffer::{
    BufferFillStateByCount, BufferFillStateBySlots, MessagesBuffer, MessagesBufferLimits,
};
use crate::collator::state::DisplayRangeReaderStateByPartition;
use crate::mempool::MempoolAnchorId;
use crate::types::DebugIter;
use crate::types::processed_upto::{BlockSeqno, ExternalsRangeInfo};

#[derive(Default)]
pub struct ExternalsReaderState {
    /// We fully read each externals range
    /// because we unable to get remaning messages info
    /// in any other way.
    /// We need this for not to get messages for account `A` from range `2`
    /// when we still have messages for account `A` in range `1`.
    ///
    /// Ranges will be extracted during collation process.
    /// Should access them only before collation and after reader finalization.
    pub ranges: BTreeMap<BlockSeqno, ExternalsRangeReaderState>,

    /// Partition related externals reader state
    pub by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionReaderState>,

    /// last read to anchor chain time
    pub last_read_to_anchor_chain_time: Option<u64>,
    tx: Option<ExternalsReaderStateTransaction>,
}

struct ExternalsReaderStateTransaction {
    snapshot_by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionReaderState>,
    snapshot_last_read_to_anchor_chain_time: Option<u64>,

    snapshot_range_keys: FastHashSet<BlockSeqno>,
    removed_ranges: BTreeMap<BlockSeqno, ExternalsRangeReaderState>,
}

// Transactional operations
impl ExternalsReaderState {
    pub fn begin(&mut self) {
        assert!(self.tx.is_none(), "transaction already in progress");

        self.tx = Some(ExternalsReaderStateTransaction {
            snapshot_by_partitions: self.by_partitions.clone(),
            snapshot_last_read_to_anchor_chain_time: self.last_read_to_anchor_chain_time,
            snapshot_range_keys: self.ranges.keys().copied().collect(),
            removed_ranges: BTreeMap::new(),
        });

        for r in self.ranges.values_mut() {
            r.begin();
        }
    }

    pub fn commit(&mut self) {
        assert!(self.tx.is_some(), "no active transaction");

        for r in self.ranges.values_mut() {
            r.commit();
        }

        self.tx = None;
    }

    pub fn rollback(&mut self) {
        let tx = self.tx.take().expect("no active transaction");

        self.by_partitions = tx.snapshot_by_partitions;
        self.last_read_to_anchor_chain_time = tx.snapshot_last_read_to_anchor_chain_time;

        self.ranges
            .retain(|k, _| tx.snapshot_range_keys.contains(k));
        self.ranges.extend(tx.removed_ranges);

        for r in self.ranges.values_mut() {
            r.rollback();
        }
    }
}

impl ExternalsReaderState {
    pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
        &self,
        par_id: T,
    ) -> anyhow::Result<&ExternalsPartitionReaderState> {
        let par_id = par_id.into();
        self.by_partitions
            .get(&par_id)
            .with_context(|| format!("externals reader state not exists for partition {par_id}"))
    }

    pub fn insert_range(
        &mut self,
        seqno: BlockSeqno,
        mut state: ExternalsRangeReaderState,
    ) -> Option<ExternalsRangeReaderState> {
        if self.tx.is_some() {
            state.begin();
        }

        if let Some(tx) = &mut self.tx {
            tx.removed_ranges.remove(&seqno);
        }

        self.ranges.insert(seqno, state)
    }

    pub fn remove_range(&mut self, seqno: BlockSeqno) {
        let Some(removed) = self.ranges.remove(&seqno) else {
            return;
        };

        if let Some(tx) = &mut self.tx {
            if tx.snapshot_range_keys.contains(&seqno) {
                tx.removed_ranges.insert(seqno, removed);
            }
        }
    }

    pub fn ranges(&self) -> &BTreeMap<BlockSeqno, ExternalsRangeReaderState> {
        &self.ranges
    }

    pub fn ranges_mut(&mut self) -> &mut BTreeMap<BlockSeqno, ExternalsRangeReaderState> {
        &mut self.ranges
    }

    pub fn retain_ranges<F>(&mut self, mut f: F)
    where
        F: FnMut(&BlockSeqno, &mut ExternalsRangeReaderState) -> bool,
    {
        if self.tx.is_none() {
            self.ranges.retain(|k, v| f(k, v));
            return;
        }

        let mut to_remove: Vec<BlockSeqno> = Vec::new();

        for (k, v) in self.ranges.iter_mut() {
            if !f(k, v) {
                to_remove.push(*k);
            }
        }

        for k in to_remove {
            let removed = self
                .ranges
                .remove(&k)
                .expect("key collected for removal must exist");

            if let Some(tx) = &mut self.tx {
                if tx.snapshot_range_keys.contains(&k) {
                    tx.removed_ranges.insert(k, removed);
                }
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ExternalsPartitionReaderState {
    /// The last processed external message from all ranges
    pub processed_to: ExternalKey,

    /// Actual current processed offset
    /// during the messages reading.
    /// Is incremented before collect.
    pub curr_processed_offset: u32,
}

struct RangeReaderStateTransaction {
    snapshot_range: ExternalsReaderRange,
    snapshot_fully_read: bool,
    snapshot_partition_keys: FastHashSet<QueuePartitionIdx>,
    removed_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState>,
}

pub struct ExternalsRangeReaderState {
    /// Range info
    pub range: ExternalsReaderRange,
    /// Partition related externals range reader state
    by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState>,
    pub fully_read: bool,
    tx: Option<RangeReaderStateTransaction>,
}

impl ExternalsRangeReaderState {
    pub fn new(
        range: ExternalsReaderRange,
        by_partitions: BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState>,
    ) -> Self {
        Self {
            range,
            by_partitions,
            fully_read: false,
            tx: None,
        }
    }

    pub fn begin(&mut self) {
        assert!(self.tx.is_none(), "transaction already in progress");

        self.tx = Some(RangeReaderStateTransaction {
            snapshot_range: self.range.clone(),
            snapshot_fully_read: self.fully_read,
            snapshot_partition_keys: self.by_partitions.keys().copied().collect(),
            removed_partitions: BTreeMap::new(),
        });

        for state in self.by_partitions.values_mut() {
            state.begin();
        }
    }

    pub fn commit(&mut self) {
        assert!(self.tx.is_some(), "no active transaction");

        for state in self.by_partitions.values_mut() {
            state.commit();
        }
        self.tx = None;
    }

    pub fn rollback(&mut self) {
        let tx = self.tx.take().expect("no active transaction");

        self.range = tx.snapshot_range;
        self.fully_read = tx.snapshot_fully_read;
        self.by_partitions
            .retain(|k, _| tx.snapshot_partition_keys.contains(k));

        self.by_partitions.extend(tx.removed_partitions);

        for state in self.by_partitions.values_mut() {
            state.rollback();
        }
    }

    pub fn get_state_by_partition_mut<T: Into<QueuePartitionIdx>>(
        &mut self,
        par_id: T,
    ) -> anyhow::Result<&mut ExternalsPartitionRangeReaderState> {
        let par_id = par_id.into();
        self.by_partitions.get_mut(&par_id).with_context(|| {
            format!("externals range reader state not exists for partition {par_id}")
        })
    }

    pub fn get_state_by_partition<T: Into<QueuePartitionIdx>>(
        &self,
        par_id: T,
    ) -> anyhow::Result<&ExternalsPartitionRangeReaderState> {
        let par_id = par_id.into();
        self.by_partitions.get(&par_id).with_context(|| {
            format!("externals range reader state not exists for partition {par_id}")
        })
    }

    pub fn partitions(&self) -> &BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState> {
        &self.by_partitions
    }

    pub fn partitions_mut(
        &mut self,
    ) -> &mut BTreeMap<QueuePartitionIdx, ExternalsPartitionRangeReaderState> {
        &mut self.by_partitions
    }

    pub fn insert_partition(
        &mut self,
        par_id: QueuePartitionIdx,
        state: ExternalsPartitionRangeReaderState,
    ) {
        if let Some(tx) = &mut self.tx {
            tx.removed_partitions.remove(&par_id);
        }
        self.by_partitions.insert(par_id, state);
    }

    pub fn remove_partition(&mut self, par_id: QueuePartitionIdx) {
        let Some(removed) = self.by_partitions.remove(&par_id) else {
            return;
        };

        if let Some(tx) = &mut self.tx {
            if tx.snapshot_partition_keys.contains(&par_id) {
                tx.removed_partitions.insert(par_id, removed);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExternalsReaderRange {
    pub from: ExternalKey,
    pub to: ExternalKey,

    pub current_position: ExternalKey,

    /// Chain time of the block during whose collation the range was read
    pub chain_time: u64,
}

impl ExternalsReaderRange {
    pub fn from_range_info(range_info: &ExternalsRangeInfo, processed_to: ExternalKey) -> Self {
        let from = range_info.from.into();
        let to = range_info.to.into();
        let current_position = if processed_to < from {
            from
        } else if processed_to < to {
            processed_to
        } else {
            to
        };
        Self {
            from,
            to,
            current_position,
            chain_time: range_info.chain_time,
        }
    }
}

#[derive(Default)]
struct ReaderStateTransaction {
    snapshot_skip_offset: u32,
    snapshot_processed_offset: u32,
    snapshot_last_expire_check_on_ct: Option<u64>,
}

pub struct ExternalsPartitionRangeReaderState {
    /// Buffer to store external messages
    /// before collect them to the next execution group
    pub buffer: MessagesBuffer,
    /// Skip offset before collecting messages from this range.
    /// Because we should collect from others.
    pub skip_offset: u32,
    /// How many times externals messages were collected from all ranges.
    /// Every range contains offset that was reached when range was the last.
    /// So the current last range contains the actual offset.
    pub processed_offset: u32,
    /// Last chain time used to check externals expiration.
    /// If `next_chain_time` was not changed on collect,
    /// we can omit the expire check.
    pub last_expire_check_on_ct: Option<u64>,
    tx: Option<ReaderStateTransaction>,
}

impl ExternalsPartitionRangeReaderState {
    pub fn begin(&mut self) {
        assert!(self.tx.is_none(), "transaction already in progress");

        self.tx = Some(ReaderStateTransaction {
            snapshot_skip_offset: self.skip_offset,
            snapshot_processed_offset: self.processed_offset,
            snapshot_last_expire_check_on_ct: self.last_expire_check_on_ct,
        });
        self.buffer.begin();
    }

    pub fn commit(&mut self) {
        assert!(self.tx.is_some(), "no active transaction");

        self.tx = None;
        self.buffer.commit();
    }

    pub fn rollback(&mut self) {
        let tx = self.tx.take().expect("no active transaction");

        self.skip_offset = tx.snapshot_skip_offset;
        self.processed_offset = tx.snapshot_processed_offset;
        self.last_expire_check_on_ct = tx.snapshot_last_expire_check_on_ct;

        self.buffer.rollback();
    }
}

impl ExternalsPartitionRangeReaderState {
    pub fn new(
        buffer: MessagesBuffer,
        skip_offset: u32,
        processed_offset: u32,
        last_expire_check_on_ct: Option<u64>,
    ) -> Self {
        Self {
            buffer,
            skip_offset,
            processed_offset,
            last_expire_check_on_ct,
            tx: None,
        }
    }

    pub fn check_buffer_fill_state(
        &self,
        buffer_limits: &MessagesBufferLimits,
    ) -> (BufferFillStateByCount, BufferFillStateBySlots) {
        self.buffer.check_is_filled(buffer_limits)
    }
}

impl From<&ExternalsRangeInfo> for ExternalsPartitionRangeReaderState {
    fn from(value: &ExternalsRangeInfo) -> Self {
        Self {
            buffer: Default::default(),
            skip_offset: value.skip_offset,
            processed_offset: value.processed_offset,
            last_expire_check_on_ct: None,
            tx: Default::default(),
        }
    }
}

impl From<(&ExternalsReaderRange, &ExternalsPartitionRangeReaderState)> for ExternalsRangeInfo {
    fn from((range, state): (&ExternalsReaderRange, &ExternalsPartitionRangeReaderState)) -> Self {
        Self {
            from: range.from.into(),
            to: range.to.into(),
            chain_time: range.chain_time,
            skip_offset: state.skip_offset,
            processed_offset: state.processed_offset,
        }
    }
}

pub struct DebugExternalsRangeReaderState<'a>(pub &'a ExternalsRangeReaderState);

impl std::fmt::Debug for DebugExternalsRangeReaderState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("range", &self.0.range)
            .field(
                "by_partitions",
                &DebugIter(
                    self.0
                        .by_partitions
                        .iter()
                        .map(|(par_id, par)| (par_id, DisplayRangeReaderStateByPartition(par))),
                ),
            )
            .finish()
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExternalKey {
    pub anchor_id: MempoolAnchorId,
    pub msgs_offset: u64,
}

impl std::fmt::Debug for ExternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.anchor_id, self.msgs_offset)
    }
}

impl From<(MempoolAnchorId, u64)> for ExternalKey {
    fn from(value: (MempoolAnchorId, u64)) -> Self {
        Self {
            anchor_id: value.0,
            msgs_offset: value.1,
        }
    }
}

impl From<ExternalKey> for (MempoolAnchorId, u64) {
    fn from(value: ExternalKey) -> Self {
        (value.anchor_id, value.msgs_offset)
    }
}
