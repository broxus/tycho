use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use croaring::{Bitmap64, Portable};
use rayon::ThreadPool;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use tycho_util::FastHashMap;

const COUNTERS_SNAPSHOT_VERSION: u8 = 1;

#[derive(Debug, Clone)]
pub struct Counters {
    pub(crate) next_idx: NextIdx,
    // Invariants:
    // - `present_in_any_small_tier` contains exactly ids materialized in `small_rc_bits` or `large_rc`.
    // - `live_cells` tracks total live rows and includes implicit `RefCount::ONE` rows.
    live_cells: u64,
    // Counter ids which are alive in counter storage: present in `small_rc_bits` or `large_rc`.
    // Cell-row liveness is tracked by the cells CF, not by this bitmap.
    present_in_any_small_tier: Bitmap64,
    small_rc_bits: [Bitmap64; 8],
    large_rc: FastHashMap<Idx, RefCount>,
    batch: CounterBatchScratch,
    maintenance: CounterMaintenance,
    worker_pool: Arc<ThreadPool>,
}

impl Counters {
    pub fn new(next_idx: NextIdx, worker_pool: Arc<ThreadPool>) -> Self {
        Self {
            next_idx,
            live_cells: 0,
            present_in_any_small_tier: Bitmap64::new(),
            small_rc_bits: std::array::from_fn(|_| Bitmap64::new()),
            large_rc: FastHashMap::default(),
            batch: CounterBatchScratch::default(),
            maintenance: CounterMaintenance::default(),
            worker_pool,
        }
    }

    pub fn alloc_idx(&mut self) -> Idx {
        let idx = Idx::new(self.next_idx.get());
        // At 10**10 allocated ids per second, u64 space lasts about 58 years
        self.next_idx = NextIdx::new(self.next_idx.get() + 1);
        idx
    }

    /// Returns the live-row refcount for an allocated idx whose row still exists.
    ///
    /// `Counters` stores only non-default overrides. For a live row, absence from both
    /// `small_rc_bits` and `large_rc` decodes to `RefCount::ONE`.
    pub fn get(&self, idx: Idx) -> u64 {
        debug_assert!(
            idx.get() < self.next_idx.get(),
            "counter idx must be allocated: idx={idx:?}, next_idx={:?}",
            self.next_idx
        );
        self.get_allocated(idx).get()
    }

    pub fn begin(&mut self) -> CounterBatch<'_> {
        let scratch = std::mem::take(&mut self.batch);
        CounterBatch {
            counters: self,
            scratch,
        }
    }

    // Format:
    //
    // [version: u8]
    // [next_idx: u64 LE]
    // [live_cells: u64 LE]
    // 8 * [roaring bitmap payload]
    // where each payload is [len: u64 LE][portable bitmap bytes]
    // N * [idx: u64 LE][ref_count: u64 LE]
    pub fn serialize(&mut self) -> Vec<u8> {
        let capacity = size_of::<u8>()
            + size_of::<u64>()
            + size_of::<u64>()
            + self.small_rc_bits.len() * size_of::<u64>()
            + self
                .small_rc_bits
                .iter()
                .map(Bitmap64::get_serialized_size_in_bytes::<Portable>)
                .sum::<usize>()
            + self.large_rc.len() * 2 * size_of::<u64>();
        let mut data = Vec::with_capacity(capacity);

        data.put_u8(COUNTERS_SNAPSHOT_VERSION);
        data.put_u64_le(self.next_idx.get());
        data.put_u64_le(self.live_cells);

        // 8 * [roaring bitmap payload]
        for set in &mut self.small_rc_bits {
            let len = set.get_serialized_size_in_bytes::<Portable>();
            data.put_u64_le(len as u64);

            let start = data.len();
            data.resize(start + len, 0);
            let serialized = set
                .try_serialize_into::<Portable>(&mut data[start..])
                .expect("buffer size was computed with get_serialized_size_in_bytes");
            debug_assert_eq!(serialized.len(), len);
        }

        for (idx, count) in &self.large_rc {
            data.put_u64_le(idx.get());
            data.put_u64_le(count.get());
        }

        data
    }

    pub fn record_metrics(&self) {
        metrics::gauge!("tycho_storage_cell_counters_total_cells").set(self.live_cells as f64);
        metrics::gauge!("tycho_storage_cell_counters_alive_ids_count")
            .set(self.present_in_any_small_tier.cardinality() as f64);
        metrics::gauge!("tycho_storage_cell_counters_alive_ids_serialized_bytes").set(
            self.present_in_any_small_tier
                .get_serialized_size_in_bytes::<Portable>() as f64,
        );

        for (bit, set) in self.small_rc_bits.iter().enumerate() {
            let labels = [("bit", bit.to_string())];
            let serialized_bytes = set.get_serialized_size_in_bytes::<Portable>();

            metrics::gauge!("tycho_storage_cell_counters_small_bit_count", &labels)
                .set(set.cardinality() as f64);
            metrics::gauge!(
                "tycho_storage_cell_counters_small_bit_serialized_bytes",
                &labels
            )
            .set(serialized_bytes as f64);
        }

        metrics::gauge!("tycho_storage_cell_counters_big_entries").set(self.large_rc.len() as f64);
        metrics::gauge!("tycho_storage_cell_counters_big_map_capacity")
            .set(self.large_rc.capacity() as f64);
        metrics::gauge!("tycho_storage_cell_counters_big_map_bytes")
            .set((self.large_rc.capacity() * size_of::<(Idx, RefCount)>()) as f64);
        metrics::gauge!("tycho_storage_cell_counters_next_idx").set(self.next_idx.get() as f64);
    }

    pub fn deserialize_trusted(
        mut data: &[u8],
        worker_pool: Arc<ThreadPool>,
    ) -> Result<Self, CountersError> {
        if !data.has_remaining() {
            return Err(CountersError::invalid_snapshot(
                "counter snapshot payload is empty",
            ));
        }
        let version = data.get_u8();
        if version != COUNTERS_SNAPSHOT_VERSION {
            return Err(CountersError::invalid_snapshot(
                "unsupported counter snapshot version",
            ));
        }

        if data.remaining() < size_of::<u64>() {
            return Err(CountersError::invalid_snapshot(
                "unexpected end of counter snapshot",
            ));
        }
        let next_idx = NextIdx::new(data.get_u64_le());

        if data.remaining() < size_of::<u64>() {
            return Err(CountersError::invalid_snapshot(
                "unexpected end of counter snapshot",
            ));
        }
        let live_cells = data.get_u64_le();

        let mut small_rc_bits = std::array::from_fn(|_| Bitmap64::new());
        for set in &mut small_rc_bits {
            if data.remaining() < size_of::<u64>() {
                return Err(CountersError::invalid_snapshot(
                    "unexpected end of counter snapshot",
                ));
            }
            let len = data.get_u64_le() as usize;
            let Some((payload, rest)) = data.split_at_checked(len) else {
                return Err(CountersError::invalid_snapshot(
                    "unexpected end of counter snapshot",
                ));
            };
            data = rest;

            *set = Bitmap64::try_deserialize::<Portable>(payload).ok_or_else(|| {
                CountersError::invalid_snapshot("failed to deserialize counter bitmap")
            })?;
        }

        let mut present_in_any_small_tier = Bitmap64::new();
        for set in &small_rc_bits {
            present_in_any_small_tier.or_inplace(set);
        }

        let big_entry_len = size_of::<u64>() * 2;
        if !data.remaining().is_multiple_of(big_entry_len) {
            return Err(CountersError::invalid_snapshot(
                "big entries payload is not aligned",
            ));
        }

        let mut large_rc = FastHashMap::with_capacity_and_hasher(
            data.remaining() / big_entry_len,
            Default::default(),
        );
        while data.has_remaining() {
            let idx = Idx::new(data.get_u64_le());
            let raw = idx.get();
            let count = NonZeroU64::new(data.get_u64_le())
                .map(RefCount)
                .ok_or_else(|| CountersError::invalid_snapshot("big contains zero value"))?;

            if count < RefCount::BIG_MIN {
                return Err(CountersError::invalid_snapshot(
                    "big contains value outside big tier",
                ));
            }
            let _ = large_rc.insert(idx, count);
            present_in_any_small_tier.add(raw);
        }

        Ok(Self {
            next_idx,
            live_cells,
            present_in_any_small_tier,
            small_rc_bits,
            large_rc,
            batch: CounterBatchScratch::default(),
            maintenance: CounterMaintenance::default(),
            worker_pool,
        })
    }

    pub fn shrink_if_needed(&mut self) {
        let mut stats = CounterShrinkStats::default();
        let mut shrunk = false;
        let mut alive_shrunk = false;

        let alive_removed = self.maintenance.alive_removed_since_shrink;
        if should_shrink(
            self.present_in_any_small_tier.cardinality(),
            alive_removed,
            self.maintenance.min_alive_removed_before_shrink,
        ) {
            stats.alive_bytes_saved = self.present_in_any_small_tier.shrink_to_fit();
            self.maintenance.alive_removed_since_shrink = 0;
            shrunk = true;
            alive_shrunk = true;
        }

        for ((set, removed), bytes_saved) in self
            .small_rc_bits
            .iter_mut()
            .zip(&mut self.maintenance.small_removed_since_shrink)
            .zip(&mut stats.small_bytes_saved)
        {
            if should_shrink(set.cardinality(), *removed, 64 * 1024) {
                *bytes_saved = set.shrink_to_fit();
                *removed = 0;
                shrunk = true;
            }
        }

        if !shrunk {
            return;
        }

        let bytes_saved = stats.total_bytes_saved();
        if bytes_saved != 0 {
            tracing::info!(
                alive_saved = stats.alive_bytes_saved,
                small_saved = ?stats.small_bytes_saved,
                total_saved = bytes_saved,
                "shrunk counter bitmaps"
            );
        }

        if alive_shrunk {
            self.maintenance
                .adjust_shrink_threshold(stats.alive_bytes_saved);
        }
    }

    /// Applies the currently checked-out counter batch.
    ///
    /// Caller contract:
    /// - each idx may appear at most once per batch;
    /// - `old_raw` must describe the state before the batch;
    /// - `new_raw` must describe the final state after the batch.
    ///
    /// `RefCount::ONE` is the implicit default, so setting it clears any stored
    /// override instead of taking a dedicated branch.
    fn apply_batch(&mut self, scratch: &mut CounterBatchScratch) {
        let pool = self.worker_pool.clone();
        let max_idx = pool.install(|| scratch.prepare_order());
        let Some(max_idx) = max_idx else {
            return;
        };

        debug_assert!(
            max_idx.get() < self.next_idx.get(),
            "counter batch idx must be allocated: idx={max_idx:?}, next_idx={:?}",
            self.next_idx
        );

        let live_cells = self.live_cells + scratch.total_cells_added;
        debug_assert!(live_cells >= scratch.total_cells_removed);
        let live_cells = live_cells - scratch.total_cells_removed;

        if scratch.transitions.is_empty() {
            self.live_cells = live_cells;
            return;
        }

        scratch.deltas.clear();
        scratch.override_add.clear();
        scratch.override_remove.clear();
        for &transition in &scratch.transitions {
            let CounterTransition {
                idx,
                old_raw,
                new_raw,
            } = transition;

            if new_raw >= RefCount::BIG_MIN.get() {
                self.large_rc.insert(idx, RefCount::new(new_raw));
            } else if old_raw >= RefCount::BIG_MIN.get() {
                self.large_rc.remove(&idx);
            }

            let raw = idx.get();
            match (old_raw > 1, new_raw > 1) {
                (false, true) => scratch.override_add.push(raw),
                (true, false) => scratch.override_remove.push(raw),
                _ => {}
            }

            let old_code = Self::encode_small_raw(old_raw);
            let new_code = Self::encode_small_raw(new_raw);
            if old_code != new_code {
                scratch.deltas.push_sorted(raw, old_code, new_code);
            }
        }

        pool.install(|| {
            rayon::join(
                || {
                    Self::apply_alive_ids(
                        &mut self.present_in_any_small_tier,
                        &scratch.override_remove,
                        &scratch.override_add,
                    );
                },
                || {
                    scratch.deltas.flush_sorted_into(&mut self.small_rc_bits);
                },
            )
        });

        self.maintenance.alive_removed_since_shrink += scratch.override_remove.len() as u64;
        for (removed_since_shrink, removed) in self
            .maintenance
            .small_removed_since_shrink
            .iter_mut()
            .zip(&scratch.deltas.remove)
        {
            *removed_since_shrink += removed.len() as u64;
        }
        self.live_cells = live_cells;
    }

    fn get_allocated(&self, idx: Idx) -> RefCount {
        let raw = idx.get();
        if !self.present_in_any_small_tier.contains(raw) {
            return RefCount::ONE;
        }

        if let Some(count) = self.large_rc.get(&idx).copied() {
            return count;
        }

        let mut code = 0u8;
        for (bit, set) in self.small_rc_bits.iter().enumerate() {
            if set.contains(raw) {
                code |= 1u8 << bit;
            }
        }

        RefCount::new(u64::from(code) + 1)
    }

    fn apply_alive_ids(alive_ids: &mut Bitmap64, removes: &[u64], adds: &[u64]) {
        if !removes.is_empty() {
            alive_ids.remove_many(removes);
        }

        if !adds.is_empty() {
            alive_ids.add_many(adds);
        }
    }

    #[inline]
    fn encode_small_raw(raw: u64) -> u8 {
        // Small-tier values 2..=256 are stored as `rc - 1` bits spread across
        // roaring sets. `0` means no small-tier override for implicit one,
        // removed rows, and big-tier values.
        match raw {
            2..=256 => (raw - 1) as u8,
            _ => 0,
        }
    }
}

pub struct CounterBatch<'a> {
    counters: &'a mut Counters,
    scratch: CounterBatchScratch,
}

impl CounterBatch<'_> {
    pub fn reserve(&mut self, additional: usize) {
        self.scratch.reserve(additional);
    }

    pub fn alloc_idx(&mut self) -> Idx {
        self.counters.alloc_idx()
    }

    pub fn update_raw(&mut self, idx: Idx, old_raw: u64, new_raw: u64) {
        // Caller owns aggregation: each idx must appear once in the batch and
        // `old_raw` must match committed state. Store/remove build transactions
        // by hash before applying counter transitions.
        self.scratch.push(CounterTransition {
            idx,
            old_raw,
            new_raw,
        });
    }

    pub fn apply(mut self) {
        self.counters.apply_batch(&mut self.scratch);
        self.scratch.clear();
        self.counters.batch = self.scratch;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Idx(u64);

impl Idx {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RefCount(NonZeroU64);

impl RefCount {
    pub const ONE: Self = Self(NonZeroU64::MIN);
    pub const BIG_MIN: Self = Self(NonZeroU64::new(257).unwrap());

    /// value must be `NonZero`. Will panic otherwise
    pub const fn new(value: u64) -> Self {
        Self(NonZeroU64::new(value).expect("refcount must be nonzero"))
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NextIdx(u64);

impl NextIdx {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CountersError {
    #[error("invalid snapshot data: {message}")]
    InvalidSnapshot { message: &'static str },
}

impl CountersError {
    fn invalid_snapshot(message: &'static str) -> Self {
        Self::InvalidSnapshot { message }
    }
}

#[derive(Debug, Clone, Copy)]
struct CounterTransition {
    idx: Idx,
    old_raw: u64,
    new_raw: u64,
}

#[derive(Debug, Default)]
struct CounterShrinkStats {
    alive_bytes_saved: usize,
    small_bytes_saved: [usize; 8],
}

impl CounterShrinkStats {
    fn total_bytes_saved(&self) -> usize {
        self.alive_bytes_saved + self.small_bytes_saved.iter().sum::<usize>()
    }
}

#[derive(Debug, Clone)]
struct CounterMaintenance {
    alive_removed_since_shrink: u64,
    small_removed_since_shrink: [u64; 8],
    min_alive_removed_before_shrink: u64,
}

impl CounterMaintenance {
    fn adjust_shrink_threshold(&mut self, bytes_saved: usize) {
        const BASE: u64 = 128 * 1024;
        const MAX: u64 = 2 * 1024 * 1024;
        const GOOD_SAVE: usize = 1024 * 1024;

        if bytes_saved >= GOOD_SAVE {
            self.min_alive_removed_before_shrink = BASE;
        } else {
            self.min_alive_removed_before_shrink = self
                .min_alive_removed_before_shrink
                .saturating_mul(2)
                .min(MAX);
        }
    }
}

impl Default for CounterMaintenance {
    fn default() -> Self {
        Self {
            alive_removed_since_shrink: 0,
            small_removed_since_shrink: [0; 8],
            min_alive_removed_before_shrink: 128 * 1024,
        }
    }
}

fn should_shrink(alive: u64, removed_since_shrink: u64, min_removed: u64) -> bool {
    if removed_since_shrink == 0 {
        return false;
    }

    if alive == 0 {
        return true;
    }

    removed_since_shrink >= min_removed && removed_since_shrink.saturating_mul(3) >= alive
}

#[derive(Debug, Clone)]
struct CounterBatchScratch {
    transitions: Vec<CounterTransition>,
    deltas: BitDeltas,
    override_add: Vec<u64>,
    override_remove: Vec<u64>,
    total_cells_added: u64,
    total_cells_removed: u64,
    is_sorted: bool,
    last_idx: Option<Idx>,
    max_idx: Option<Idx>,
}

impl CounterBatchScratch {
    fn clear(&mut self) {
        self.transitions.clear();
        self.deltas.clear();
        self.override_add.clear();
        self.override_remove.clear();
        self.total_cells_added = 0;
        self.total_cells_removed = 0;
        self.is_sorted = true;
        self.last_idx = None;
        self.max_idx = None;
    }

    fn reserve(&mut self, additional: usize) {
        self.transitions.reserve(additional);
    }

    fn push(&mut self, transition: CounterTransition) {
        let changes_counter_storage = transition.old_raw != transition.new_raw
            && !(transition.old_raw <= 1 && transition.new_raw <= 1);
        let changes_total_cells = (transition.old_raw == 0) != (transition.new_raw == 0);
        if !changes_counter_storage && !changes_total_cells {
            return;
        }

        self.max_idx = self.max_idx.max(Some(transition.idx));

        match (transition.old_raw == 0, transition.new_raw == 0) {
            (true, false) => self.total_cells_added += 1,
            (false, true) => self.total_cells_removed += 1,
            _ => {}
        }

        if let Some(prev) = self.last_idx
            && prev >= transition.idx
        {
            self.is_sorted = false;
        }
        self.last_idx = Some(transition.idx);
        self.transitions.push(transition);
    }

    fn prepare_order(&mut self) -> Option<Idx> {
        if self.transitions.is_empty() {
            return self.max_idx;
        }
        if !self.is_sorted {
            // Sorting before demuxing makes every per-bit bitmap delta a sorted
            // subsequence of the transition buffer, so flush can call roaring bulk
            // APIs without per-bit sort/dedup fallback work.
            self.transitions
                .par_sort_unstable_by_key(|transition| transition.idx);
            self.is_sorted = true;
        }

        debug_assert!(
            self.transitions.is_sorted_by(|a, b| a.idx < b.idx),
            "duplicate counter transition idx",
        );

        self.max_idx
    }
}

impl Default for CounterBatchScratch {
    fn default() -> Self {
        Self {
            transitions: Vec::new(),
            deltas: BitDeltas::default(),
            override_add: Vec::new(),
            override_remove: Vec::new(),
            total_cells_added: 0,
            total_cells_removed: 0,
            is_sorted: true,
            last_idx: None,
            max_idx: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct BitDeltas {
    add: [Vec<u64>; 8],
    remove: [Vec<u64>; 8],
}

impl BitDeltas {
    fn clear(&mut self) {
        for items in &mut self.add {
            items.clear();
        }
        for items in &mut self.remove {
            items.clear();
        }
    }

    fn push_sorted(&mut self, raw: u64, old_code: u8, new_code: u8) {
        let diff = old_code ^ new_code;

        let mut adds = diff & new_code;
        while adds != 0 {
            let bit = adds.trailing_zeros() as usize;
            self.add[bit].push(raw);
            adds &= adds - 1;
        }

        let mut removes = diff & old_code;
        while removes != 0 {
            let bit = removes.trailing_zeros() as usize;
            self.remove[bit].push(raw);
            removes &= removes - 1;
        }
    }

    fn flush_sorted_into(&self, small_bits: &mut [Bitmap64; 8]) {
        small_bits
            .par_iter_mut()
            .enumerate()
            .for_each(|(bit, small_bits)| {
                let removes = &self.remove[bit];
                if !removes.is_empty() {
                    small_bits.remove_many(removes);
                }

                let adds = &self.add[bit];
                if !adds.is_empty() {
                    small_bits.add_many(adds);
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_ok(counters: &Counters, idx: Idx) -> RefCount {
        RefCount::new(counters.get(idx))
    }

    fn update_ok(counters: &mut Counters, idx: Idx, old_raw: u64, new_raw: u64) {
        let mut batch = counters.begin();
        batch.update_raw(idx, old_raw, new_raw);
        batch.apply();
    }

    fn set_ok(counters: &mut Counters, idx: Idx, count: RefCount) -> RefCount {
        let old = get_ok(counters, idx);
        update_ok(counters, idx, old.get(), count.get());
        old
    }

    fn transition(idx: u64, old_raw: u64, new_raw: u64) -> CounterTransition {
        CounterTransition {
            idx: Idx::new(idx),
            old_raw,
            new_raw,
        }
    }

    fn pool() -> Arc<rayon::ThreadPool> {
        Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn transitions_between_tiers_are_consistent() {
        let mut counters = Counters::new(NextIdx::new(8), pool());
        let idx = Idx::new(7);

        assert_eq!(get_ok(&counters, idx), RefCount::ONE);
        update_ok(&mut counters, idx, 0, 1);
        assert_eq!(counters.live_cells, 1);

        let steps = [1u64, 2, 256, 257, 256, 1];
        for [old, new] in steps.array_windows().cloned() {
            assert_eq!(
                set_ok(&mut counters, idx, RefCount::new(new)),
                RefCount::new(old)
            );
            assert_eq!(get_ok(&counters, idx), RefCount::new(new));
        }
    }

    #[test]
    fn snapshot_roundtrip_preserves_state() {
        let mut counters = Counters::new(NextIdx::new(11), pool());

        for (idx, count) in [(1, 1), (2, 2), (3, 3), (4, 256), (5, 257), (6, 1024)] {
            update_ok(&mut counters, Idx::new(idx), 0, count);
        }

        let data = counters.serialize();

        let loaded = Counters::deserialize_trusted(&data, pool()).expect("snapshot must be valid");

        for i in 0..8 {
            assert_eq!(get_ok(&loaded, Idx::new(i)), get_ok(&counters, Idx::new(i)));
        }
        assert_eq!(loaded.next_idx, NextIdx::new(11));
        assert_eq!(loaded.live_cells, counters.live_cells);
    }

    #[test]
    fn batched_updates_match_scalar_updates() {
        let initial = [
            (Idx::new(2), RefCount::ONE),
            (Idx::new(3), RefCount::new(2)),
            (Idx::new(4), RefCount::new(256)),
            (Idx::new(5), RefCount::new(256)),
            (Idx::new(6), RefCount::new(257)),
            (Idx::new(7), RefCount::new(2)),
            (Idx::new(8), RefCount::new(257)),
            (Idx::new(9), RefCount::ONE),
            (Idx::new(10), RefCount::new(257)),
            (Idx::new(11), RefCount::new(256)),
        ];
        let transitions = [
            transition(8, 257, 0),
            transition(0, 0, 1),
            transition(6, 257, 256),
            transition(2, 1, 2),
            transition(9, 1, 0),
            transition(5, 256, 257),
            transition(3, 2, 3),
            transition(1, 0, 2),
            transition(7, 2, 1),
            transition(4, 256, 1),
            transition(10, 257, 1),
            transition(11, 256, 0),
        ];

        let mut scalar = Counters::new(NextIdx::new(16), pool());
        let mut batched = Counters::new(NextIdx::new(16), pool());
        for (idx, count) in initial {
            update_ok(&mut scalar, idx, 0, count.get());
            update_ok(&mut batched, idx, 0, count.get());
        }

        for transition in transitions {
            update_ok(
                &mut scalar,
                transition.idx,
                transition.old_raw,
                transition.new_raw,
            );
        }
        let mut batch = batched.begin();
        for transition in transitions {
            batch.update_raw(transition.idx, transition.old_raw, transition.new_raw);
        }
        batch.apply();

        for raw in 0..16 {
            let idx = Idx::new(raw);
            assert_eq!(get_ok(&batched, idx), get_ok(&scalar, idx));
        }
        assert_eq!(batched.live_cells, scalar.live_cells);
        assert_eq!(batched.live_cells, 9);

        let scalar = Counters::deserialize_trusted(&scalar.serialize(), pool()).unwrap();
        let batched = Counters::deserialize_trusted(&batched.serialize(), pool()).unwrap();
        for raw in 0..16 {
            let idx = Idx::new(raw);
            assert_eq!(get_ok(&batched, idx), get_ok(&scalar, idx));
        }
        assert_eq!(batched.live_cells, scalar.live_cells);
    }

    #[test]
    fn deserialize_rejects_truncated_bitmap_payload() {
        let mut data = Counters::new(NextIdx::new(1), pool()).serialize();
        let len = data.len() as u64;
        let first_bitmap_len = size_of::<u8>() + size_of::<u64>() + size_of::<u64>();
        (&mut data[first_bitmap_len..first_bitmap_len + size_of::<u64>()]).put_u64_le(len);

        let err =
            Counters::deserialize_trusted(&data, pool()).expect_err("snapshot must be rejected");
        assert!(matches!(err, CountersError::InvalidSnapshot { .. }));
    }
}
