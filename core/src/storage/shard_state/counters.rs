use std::error::Error as StdError;
use std::io::{Read, Write};
use std::num::NonZeroU64;

use ahash::HashMapExt;
use roaring::RoaringTreemap;
use tycho_util::FastHashMap;

#[derive(Debug, Clone)]
pub struct Counters {
    next_idx: NextIdx,
    small_bits: [RoaringTreemap; 8],
    big: FastHashMap<Idx, RefCount>,
}

impl Counters {
    pub fn new(next_idx: NextIdx) -> Self {
        Self {
            next_idx,
            small_bits: std::array::from_fn(|_| RoaringTreemap::new()),
            big: FastHashMap::new(),
        }
    }

    pub fn next_idx(&self) -> NextIdx {
        self.next_idx
    }

    pub fn alloc_idx(&mut self) -> Idx {
        let idx = Idx::new(self.next_idx.get());
        self.next_idx = NextIdx::new(
            self.next_idx
                .get()
                .checked_add(1)
                .expect("next_idx overflow in Counters::alloc_idx"),
        );
        idx
    }

    /// Returns the live-row refcount for an allocated idx whose row still exists.
    ///
    /// `Counters` stores only non-default overrides. For a live row, absence from both
    /// `small_bits` and `big` decodes to `RefCount::ONE`.
    pub fn get(&self, idx: Idx) -> Result<RefCount, CountersError> {
        self.ensure_allocated_idx(idx)?;
        Ok(self.get_allocated(idx))
    }

    /// Updates the live-row refcount for an allocated idx whose row still exists.
    ///
    /// `RefCount::ONE` is the implicit default, so setting it clears any stored
    /// override instead of taking a dedicated branch.
    pub fn set(&mut self, idx: Idx, count: RefCount) -> Result<SetResult, CountersError> {
        self.ensure_allocated_idx(idx)?;
        let old = self.get_allocated(idx);
        if old == count {
            return Ok(SetResult { old, new: count });
        }

        if count >= RefCount::BIG_MIN {
            self.big.insert(idx, count);
        } else {
            self.clear_override(idx, old);
        }

        self.update_small_bits(
            idx.get(),
            Self::encode_small_count(old),
            Self::encode_small_count(count),
        );

        Ok(SetResult { old, new: count })
    }

    /// Clears the stored override state for `idx`. Must be called after physical delete.
    ///
    /// This does not invalidate the allocated `Idx`; callers must separately track
    /// whether the row still exists.
    pub fn remove(&mut self, idx: Idx) -> Result<(), CountersError> {
        self.ensure_allocated_idx(idx)?;
        self.clear_override(idx, self.get_allocated(idx));

        Ok(())
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<(), CountersError> {
        self.to_snapshot().serialize_into(writer)
    }

    pub fn load_from<R: Read>(reader: &mut R) -> Result<Self, CountersError> {
        CountersSnapshot::deserialize_from(reader)?.into_counters()
    }

    fn get_allocated(&self, idx: Idx) -> RefCount {
        if let Some(count) = self.big.get(&idx).copied() {
            return count;
        }

        let raw = idx.get();
        let mut code = 0u8;
        for (bit, set) in self.small_bits.iter().enumerate() {
            if set.contains(raw) {
                code |= 1u8 << bit;
            }
        }

        if code == 0 {
            RefCount::ONE
        } else {
            RefCount::new(u64::from(code) + 1)
        }
    }

    fn ensure_allocated_idx(&self, idx: Idx) -> Result<(), CountersError> {
        if idx >= Idx::new(self.next_idx.get()) {
            return Err(CountersError::IdxOutOfBounds {
                idx,
                next_idx: self.next_idx,
            });
        }
        Ok(())
    }

    fn to_snapshot(&self) -> CountersSnapshot {
        let mut big = self
            .big
            .iter()
            .map(|(idx, count)| (*idx, count.get()))
            .collect::<Vec<_>>();
        big.sort_unstable_by_key(|(idx, _)| idx.get());

        CountersSnapshot {
            next_idx: self.next_idx,
            small_bits: self.small_bits.clone(),
            big,
        }
    }

    fn update_small_bits(&mut self, raw: u64, old: u8, new: u8) {
        // Small tier encoding:
        // - For 2..=256 we store `v = rc - 1` across 8 roaring sets, one bit per set.
        // - Set i contains `idx` iff bit i of v is 1.
        //
        // Example:
        //   rc = 13 -> v = 12 -> 0b0000_1100
        //   => idx is present in small_bits[2] and small_bits[3],
        //      and absent in all other small_bits.
        //
        // `old == 0` / `new == 0` means "no small-tier override stored".
        //
        // Update rule:
        //   diff = old_v XOR new_v
        // Only changed bits are touched, so transitions like
        // default-one->small, small->big, big->small, and small->default-one all
        // reuse this path.
        let diff = old ^ new;
        if diff == 0 {
            return;
        }

        for (bit, set) in self.small_bits.iter_mut().enumerate() {
            let mask = 1u8 << bit;
            let changed = diff & mask != 0;
            if !changed {
                continue;
            }

            let should_be_set = new & mask != 0;
            if should_be_set {
                set.insert(raw);
            } else {
                set.remove(raw);
            }
        }
    }

    fn clear_override(&mut self, idx: Idx, old: RefCount) {
        self.big.remove(&idx);
        self.update_small_bits(idx.get(), Self::encode_small_count(old), 0);
    }

    fn encode_small_count(count: RefCount) -> u8 {
        // `0` means "not stored in the small tier", which covers both the implicit
        // default `RefCount::ONE` and all big-tier values.
        match count.get() {
            2..=256 => (count.get() - 1) as u8,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Idx(u64);

impl Idx {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RefCount(NonZeroU64);

impl RefCount {
    pub const ONE: Self = Self(NonZeroU64::MIN);
    pub const BIG_MIN: Self = Self(NonZeroU64::new(257).unwrap());

    /// value must be NonZero. Will panic otherwise
    pub const fn new(value: u64) -> Self {
        Self(NonZeroU64::new(value).expect("refcount must be nonzero"))
    }

    pub const fn get(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NextIdx(u64);

impl NextIdx {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetResult {
    pub old: RefCount,
    pub new: RefCount,
}

#[derive(thiserror::Error, Debug)]
pub enum CountersError {
    #[error("idx out of bounds: idx={idx:?}, next_idx={next_idx:?}")]
    IdxOutOfBounds { idx: Idx, next_idx: NextIdx },
    #[error("snapshot IO failed: {0}")]
    SnapshotIo(#[from] std::io::Error),
    #[error("invalid snapshot data: {message}")]
    InvalidSnapshot {
        message: &'static str,
        #[source]
        source: Option<Box<dyn StdError + Send + Sync>>,
    },
}

impl CountersError {
    fn invalid_snapshot(message: &'static str) -> Self {
        Self::InvalidSnapshot {
            message,
            source: None,
        }
    }

    fn invalid_snapshot_with_source(
        message: &'static str,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::InvalidSnapshot {
            message,
            source: Some(Box::new(source)),
        }
    }
}

#[derive(Debug, Clone)]
struct CountersSnapshot {
    pub next_idx: NextIdx,
    pub small_bits: [RoaringTreemap; 8],
    pub big: Vec<(Idx, u64)>,
}

impl CountersSnapshot {
    fn into_counters(self) -> Result<Counters, CountersError> {
        let Self {
            next_idx,
            small_bits,
            big: big_values,
        } = self;

        let mut max_seen = small_bits.iter().filter_map(RoaringTreemap::max).max();

        let mut big = FastHashMap::with_capacity(big_values.len());
        for (idx, count) in big_values {
            let count = match NonZeroU64::new(count) {
                Some(count) => RefCount(count),
                None => return Err(CountersError::invalid_snapshot("big contains zero value")),
            };

            if count < RefCount::BIG_MIN {
                return Err(CountersError::invalid_snapshot(
                    "big contains value outside big tier",
                ));
            }
            if big.insert(idx, count).is_some() {
                return Err(CountersError::invalid_snapshot("duplicate idx in big"));
            }

            let idx = idx.get();
            max_seen = Some(match max_seen {
                Some(max_seen) => max_seen.max(idx),
                None => idx,
            });
        }

        for idx in big.keys().copied() {
            let raw = idx.get();
            if small_bits.iter().any(|set| set.contains(raw)) {
                return Err(CountersError::invalid_snapshot(
                    "small and big tiers overlap",
                ));
            }
        }

        if let Some(max_seen) = max_seen
            && next_idx.get() <= max_seen
        {
            return Err(CountersError::invalid_snapshot(
                "next_idx must be greater than max seen idx",
            ));
        }

        Ok(Counters {
            next_idx,
            small_bits,
            big,
        })
    }

    fn serialize_into<W: Write>(&self, writer: &mut W) -> Result<(), CountersError> {
        Self::write_u64(writer, self.next_idx.get())?;

        for set in &self.small_bits {
            set.serialize_into(&mut *writer)
                .map(|_| ())
                .map_err(CountersError::SnapshotIo)?;
        }

        Self::write_u64(writer, self.big.len() as u64)?;

        for (idx, count) in &self.big {
            Self::write_u64(writer, idx.get())?;
            Self::write_u64(writer, *count)?;
        }

        Ok(())
    }

    fn deserialize_from<R: Read>(reader: &mut R) -> Result<Self, CountersError> {
        let next_idx = NextIdx::new(Self::read_u64(reader)?);
        let mut small_bits = std::array::from_fn(|_| RoaringTreemap::new());
        for set in &mut small_bits {
            *set = RoaringTreemap::deserialize_from(&mut *reader).map_err(|err| {
                CountersError::invalid_snapshot_with_source(
                    "failed to deserialize roaring bitmap",
                    err,
                )
            })?;
        }

        let big_len = Self::read_u64(reader)?;
        let big_len = usize::try_from(big_len).map_err(|err| {
            CountersError::invalid_snapshot_with_source("big len does not fit in usize", err)
        })?;
        let mut big = Vec::with_capacity(big_len);
        for _ in 0..big_len {
            let idx = Idx::new(Self::read_u64(reader)?);
            let count = Self::read_u64(reader)?;
            big.push((idx, count));
        }

        Ok(Self {
            next_idx,
            small_bits,
            big,
        })
    }

    fn read_u64<R: Read>(reader: &mut R) -> Result<u64, CountersError> {
        let mut bytes = [0u8; size_of::<u64>()];
        reader.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn write_u64<W: Write>(writer: &mut W, value: u64) -> Result<(), CountersError> {
        writer.write_all(&value.to_le_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_ok(counters: &Counters, idx: Idx) -> RefCount {
        counters
            .get(idx)
            .expect("idx must be allocated and row must exist")
    }

    fn set_ok(counters: &mut Counters, idx: Idx, count: RefCount) -> SetResult {
        counters
            .set(idx, count)
            .expect("set must succeed for allocated idx")
    }

    fn assert_invalid_snapshot(snapshot: CountersSnapshot, expected_message: &'static str) {
        let err = snapshot
            .into_counters()
            .expect_err("snapshot must be rejected");
        match err {
            CountersError::InvalidSnapshot { message, .. } => {
                assert_eq!(message, expected_message);
            }
            _ => panic!("expected invalid snapshot error"),
        }
    }

    #[test]
    fn transitions_between_tiers_are_consistent() {
        let mut counters = Counters::new(NextIdx::new(8));
        let idx = Idx::new(7);

        assert_eq!(get_ok(&counters, idx), RefCount::ONE);

        let steps = [1u64, 2, 256, 257, 256, 1];
        for [old, new] in steps.array_windows().cloned() {
            assert_eq!(set_ok(&mut counters, idx, RefCount::new(new)), SetResult {
                old: RefCount::new(old),
                new: RefCount::new(new),
            });
            assert_eq!(get_ok(&counters, idx), RefCount::new(new));
        }
    }

    #[test]
    fn set_is_idempotent_for_same_value() {
        let mut counters = Counters::new(NextIdx::new(8));
        let idx = Idx::new(7);

        set_ok(&mut counters, idx, RefCount::new(13));

        let result = set_ok(&mut counters, idx, RefCount::new(13));

        assert_eq!(result.old, RefCount::new(13));
        assert_eq!(result.new, RefCount::new(13));
        assert_eq!(get_ok(&counters, idx), RefCount::new(13));
    }

    #[test]
    fn setting_one_clears_all_stored_overrides() {
        let mut counters = Counters::new(NextIdx::new(8));
        let idx = Idx::new(7);

        counters
            .set(idx, RefCount::new(257))
            .expect("set must succeed for allocated idx");
        counters
            .set(idx, RefCount::ONE)
            .expect("set must succeed for allocated idx");

        assert!(!counters.big.contains_key(&idx));
        assert!(
            counters
                .small_bits
                .iter()
                .all(|set| !set.contains(idx.get()))
        );
        assert_eq!(get_ok(&counters, idx), RefCount::ONE);
    }

    #[test]
    fn snapshot_roundtrip_preserves_state() {
        let mut counters = Counters::new(NextIdx::new(11));

        for (idx, count) in [(1, 1), (2, 2), (3, 256), (4, 257), (5, 1024)] {
            set_ok(&mut counters, Idx::new(idx), RefCount::new(count));
        }

        let mut data = Vec::new();
        counters
            .write_to(&mut data)
            .expect("snapshot serialization must succeed");

        let mut reader = data.as_slice();
        let loaded = Counters::load_from(&mut reader).expect("snapshot must be valid");

        for i in 0..8 {
            assert_eq!(get_ok(&loaded, Idx::new(i)), get_ok(&counters, Idx::new(i)));
        }
        assert_eq!(loaded.next_idx(), NextIdx::new(11));
    }

    #[test]
    fn remove_clears_small_and_big_state() {
        let mut counters = Counters::new(NextIdx::new(11));

        set_ok(&mut counters, Idx::new(4), RefCount::new(256));
        set_ok(&mut counters, Idx::new(7), RefCount::new(257));

        counters.remove(Idx::new(4)).expect("remove must succeed");
        counters.remove(Idx::new(7)).expect("remove must succeed");

        assert!(!counters.big.contains_key(&Idx::new(4)));
        assert!(!counters.big.contains_key(&Idx::new(7)));
        assert!(counters.small_bits.iter().all(|set| !set.contains(4)));
        assert!(counters.small_bits.iter().all(|set| !set.contains(7)));
    }

    #[test]
    fn rejects_big_and_small_overlap() {
        let mut counters = Counters::new(NextIdx::new(11));
        set_ok(&mut counters, Idx::new(10), RefCount::new(257));

        let mut snapshot = counters.to_snapshot();
        snapshot.small_bits[0].insert(10);

        assert_invalid_snapshot(snapshot, "small and big tiers overlap");
    }

    #[test]
    fn rejects_non_monotonic_next_idx_on_load() {
        let mut counters = Counters::new(NextIdx::new(5));
        set_ok(&mut counters, Idx::new(4), RefCount::new(2));

        let mut snapshot = counters.to_snapshot();
        snapshot.next_idx = NextIdx::new(4);

        assert_invalid_snapshot(snapshot, "next_idx must be greater than max seen idx");
    }

    #[test]
    fn counters_api_rejects_unallocated_idx() {
        let mut counters = Counters::new(NextIdx::new(5));
        let idx = Idx::new(5);

        assert!(matches!(
            counters.get(idx),
            Err(CountersError::IdxOutOfBounds {
                idx: Idx(5),
                next_idx: NextIdx(5)
            })
        ));
        assert!(matches!(
            counters.set(idx, RefCount::ONE),
            Err(CountersError::IdxOutOfBounds {
                idx: Idx(5),
                next_idx: NextIdx(5)
            })
        ));
        assert!(matches!(
            counters.remove(idx),
            Err(CountersError::IdxOutOfBounds {
                idx: Idx(5),
                next_idx: NextIdx(5)
            })
        ));
    }

    #[test]
    fn remove_is_a_noop_for_default_one_idx() {
        let mut counters = Counters::new(NextIdx::new(5));

        counters.remove(Idx::new(4)).expect("remove must succeed");

        assert!(counters.big.is_empty());
        assert!(counters.small_bits.iter().all(RoaringTreemap::is_empty));
    }

    #[test]
    fn rejects_zero_big_value_on_load() {
        let snapshot = CountersSnapshot {
            next_idx: NextIdx::new(5),
            small_bits: std::array::from_fn(|_| RoaringTreemap::new()),
            big: vec![(Idx::new(4), 0)],
        };

        assert_invalid_snapshot(snapshot, "big contains zero value");
    }

    #[test]
    fn rejects_small_tier_value_in_big_map() {
        let snapshot = CountersSnapshot {
            next_idx: NextIdx::new(5),
            small_bits: std::array::from_fn(|_| RoaringTreemap::new()),
            big: vec![(Idx::new(4), 256)],
        };

        assert_invalid_snapshot(snapshot, "big contains value outside big tier");
    }

    #[test]
    fn rejects_duplicate_idx_in_big_map() {
        let snapshot = CountersSnapshot {
            next_idx: NextIdx::new(5),
            small_bits: std::array::from_fn(|_| RoaringTreemap::new()),
            big: vec![(Idx::new(4), 300), (Idx::new(4), 400)],
        };

        assert_invalid_snapshot(snapshot, "duplicate idx in big");
    }

    #[test]
    fn alloc_idx_advances_next_idx() {
        let mut counters = Counters::new(NextIdx::new(100));

        assert_eq!(counters.alloc_idx(), Idx::new(100));
        assert_eq!(counters.alloc_idx(), Idx::new(101));
        assert_eq!(counters.next_idx(), NextIdx::new(102));
    }
}
