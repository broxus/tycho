use std::io::{Read, Write};

use ahash::HashMapExt;
use roaring::RoaringTreemap;
use tycho_util::FastHashMap;

#[derive(Debug, Clone)]
pub struct Counters {
    next_idx: NextIdx,
    snapshot_height: SnapshotHeight,
    present: RoaringTreemap,
    small_bits: [RoaringTreemap; 8],
    big: FastHashMap<Idx, RefCount>,
}

impl Counters {
    pub fn new(next_idx: NextIdx, snapshot_height: SnapshotHeight) -> Self {
        Self {
            next_idx,
            snapshot_height,
            present: RoaringTreemap::new(),
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

    pub fn snapshot_height(&self) -> SnapshotHeight {
        self.snapshot_height
    }

    pub fn get(&self, idx: Idx) -> Result<RefCount, CountersError> {
        self.ensure_allocated_idx(idx)?;
        Ok(self.get_allocated(idx))
    }

    pub fn set(&mut self, idx: Idx, count: RefCount) -> Result<SetResult, CountersError> {
        self.ensure_allocated_idx(idx)?;
        let old = self.get_allocated(idx);
        if old == count {
            return Ok(SetResult { old, new: count });
        }

        let raw = idx.get();
        let old_small = Self::small_code(old);
        let new_small = Self::small_code(count);

        if count.is_zero() {
            self.present.remove(raw);
        } else {
            self.present.insert(raw);
        }

        if count >= RefCount::BIG_MIN {
            self.big.insert(idx, count);
        } else {
            self.big.remove(&idx);
        }

        self.update_small_bits(raw, old_small, new_small);

        Ok(SetResult { old, new: count })
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<(), CountersError> {
        self.to_snapshot().serialize_into(writer)
    }

    pub fn load_from<R: Read>(reader: &mut R) -> Result<Self, CountersError> {
        let snapshot = CountersSnapshot::deserialize_from(reader)?;
        Self::from_snapshot(snapshot)
    }

    fn get_allocated(&self, idx: Idx) -> RefCount {
        let raw = idx.get();
        if !self.present.contains(raw) {
            return RefCount::ZERO;
        }

        if let Some(count) = self.big.get(&idx).copied() {
            return count;
        }

        let mut code = 0u8;
        for (bit, set) in self.small_bits.iter().enumerate() {
            if set.contains(raw) {
                code |= 1u8 << bit;
            }
        }

        RefCount::new(u64::from(code) + 1)
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
            .map(|(idx, count)| (*idx, *count))
            .collect::<Vec<_>>();
        big.sort_unstable_by_key(|(idx, _)| idx.get());

        CountersSnapshot {
            next_idx: self.next_idx,
            snapshot_height: self.snapshot_height,
            present: self.present.clone(),
            small_bits: self.small_bits.clone(),
            big,
        }
    }

    fn from_snapshot(snapshot: CountersSnapshot) -> Result<Self, CountersError> {
        let CountersSnapshot {
            next_idx,
            snapshot_height,
            present,
            small_bits,
            big: big_values,
        } = snapshot;

        let mut big = FastHashMap::with_capacity(big_values.len());
        for (idx, count) in big_values {
            if count < RefCount::BIG_MIN {
                return Err(CountersError::InvalidSnapshot(
                    "big contains value outside big tier",
                ));
            }
            if big.insert(idx, count).is_some() {
                return Err(CountersError::InvalidSnapshot("duplicate idx in big"));
            }
        }

        for idx in big.keys().copied() {
            let raw = idx.get();
            if !present.contains(raw) {
                return Err(CountersError::InvalidSnapshot(
                    "big contains idx absent from present",
                ));
            }
            if small_bits.iter().any(|set| set.contains(raw)) {
                return Err(CountersError::InvalidSnapshot(
                    "small and big tiers overlap",
                ));
            }
        }

        for set in &small_bits {
            for raw in set {
                if !present.contains(raw) {
                    return Err(CountersError::InvalidSnapshot(
                        "small tier contains idx absent from present",
                    ));
                }
            }
        }

        if let Some(max_seen) = present.max()
            && next_idx.get() <= max_seen
        {
            return Err(CountersError::InvalidSnapshot(
                "next_idx must be greater than max seen idx",
            ));
        }

        Ok(Self {
            next_idx,
            snapshot_height,
            present,
            small_bits,
            big,
        })
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
        // Update rule:
        //   diff = old_v XOR new_v
        // Only changed bits are touched, so transitions like
        // small->big, big->small, 1->small, and small->1 all reuse this path.
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

    fn small_code(count: RefCount) -> u8 {
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

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RefCount(u64);

impl RefCount {
    pub const ZERO: Self = Self(0);
    pub const BIG_MIN: Self = Self(257);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotHeight(u32);

impl SnapshotHeight {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
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
    #[error("invalid snapshot data: {0}")]
    InvalidSnapshot(&'static str),
}

#[derive(Debug, Clone)]
struct CountersSnapshot {
    pub next_idx: NextIdx,
    pub snapshot_height: SnapshotHeight,
    pub present: RoaringTreemap,
    pub small_bits: [RoaringTreemap; 8],
    pub big: Vec<(Idx, RefCount)>,
}

impl CountersSnapshot {
    fn serialize_into<W: Write>(&self, writer: &mut W) -> Result<(), CountersError> {
        writer.write_all(&self.next_idx.get().to_le_bytes())?;
        writer.write_all(&self.snapshot_height.get().to_le_bytes())?;

        Self::serialize_roaring(&self.present, writer)?;
        for set in &self.small_bits {
            Self::serialize_roaring(set, writer)?;
        }

        let big_len = u64::try_from(self.big.len())
            .map_err(|_| CountersError::InvalidSnapshot("big tier is too large to serialize"))?;
        writer.write_all(&big_len.to_le_bytes())?;

        for (idx, count) in &self.big {
            writer.write_all(&idx.get().to_le_bytes())?;
            writer.write_all(&count.get().to_le_bytes())?;
        }

        Ok(())
    }

    fn deserialize_from<R: Read>(reader: &mut R) -> Result<Self, CountersError> {
        let next_idx = NextIdx::new(Self::read_u64(reader)?);
        let snapshot_height = SnapshotHeight::new(Self::read_u32(reader)?);
        let present = Self::deserialize_roaring(reader)?;
        let mut small_bits = std::array::from_fn(|_| RoaringTreemap::new());
        for set in &mut small_bits {
            *set = Self::deserialize_roaring(reader)?;
        }

        let big_len = Self::read_u64(reader)?;
        let big_len = usize::try_from(big_len)
            .map_err(|_| CountersError::InvalidSnapshot("big len does not fit in usize"))?;
        let mut big = Vec::with_capacity(big_len);
        for _ in 0..big_len {
            let idx = Idx::new(Self::read_u64(reader)?);
            let count = RefCount::new(Self::read_u64(reader)?);
            big.push((idx, count));
        }

        Ok(Self {
            next_idx,
            snapshot_height,
            present,
            small_bits,
            big,
        })
    }

    fn serialize_roaring<W: Write>(
        set: &RoaringTreemap,
        writer: &mut W,
    ) -> Result<(), CountersError> {
        set.serialize_into(writer)
            .map(|_| ())
            .map_err(CountersError::SnapshotIo)
    }

    fn deserialize_roaring<R: Read>(reader: &mut R) -> Result<RoaringTreemap, CountersError> {
        RoaringTreemap::deserialize_from(reader)
            .map_err(|_| CountersError::InvalidSnapshot("failed to deserialize roaring bitmap"))
    }

    fn read_u64<R: Read>(reader: &mut R) -> Result<u64, CountersError> {
        let mut bytes = [0u8; size_of::<u64>()];
        reader.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_u32<R: Read>(reader: &mut R) -> Result<u32, CountersError> {
        let mut bytes = [0u8; size_of::<u32>()];
        reader.read_exact(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_ok(counters: &Counters, idx: Idx) -> RefCount {
        counters.get(idx).expect("idx must be allocated")
    }

    fn set_ok(counters: &mut Counters, idx: Idx, count: RefCount) -> SetResult {
        counters
            .set(idx, count)
            .expect("set must succeed for allocated idx")
    }

    fn assert_invalid_snapshot(snapshot: CountersSnapshot) {
        let err = Counters::from_snapshot(snapshot).expect_err("snapshot must be rejected");
        assert!(matches!(err, CountersError::InvalidSnapshot(_)));
    }

    #[test]
    fn transitions_between_tiers_are_consistent() {
        let mut counters = Counters::new(NextIdx::new(8), SnapshotHeight::new(0));
        let idx = Idx::new(7);

        assert_eq!(get_ok(&counters, idx), RefCount::new(0));

        let steps = [0, 1, 2, 257, 256, 0];
        for pair in steps.windows(2) {
            let old = pair[0];
            let new = pair[1];
            assert_eq!(
                set_ok(&mut counters, idx, RefCount::new(new)),
                SetResult {
                    old: RefCount::new(old),
                    new: RefCount::new(new),
                }
            );
            assert_eq!(get_ok(&counters, idx), RefCount::new(new));
        }
    }

    #[test]
    fn set_is_idempotent_for_same_value() {
        let mut counters = Counters::new(NextIdx::new(8), SnapshotHeight::new(0));
        let idx = Idx::new(7);

        set_ok(&mut counters, idx, RefCount::new(13));

        let result = set_ok(&mut counters, idx, RefCount::new(13));

        assert_eq!(result.old, RefCount::new(13));
        assert_eq!(result.new, RefCount::new(13));
        assert_eq!(get_ok(&counters, idx), RefCount::new(13));
    }

    #[test]
    fn snapshot_roundtrip_preserves_state() {
        let mut counters = Counters::new(NextIdx::new(11), SnapshotHeight::new(42));

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
        assert_eq!(loaded.snapshot_height(), SnapshotHeight::new(42));
        assert_eq!(loaded.snapshot_height().get(), 42);
    }

    #[test]
    fn snapshot_binary_roundtrip_preserves_state() {
        let mut counters = Counters::new(NextIdx::new(11), SnapshotHeight::new(42));
        for (idx, count) in [(2, 2), (4, 257)] {
            set_ok(&mut counters, Idx::new(idx), RefCount::new(count));
        }

        let snapshot = counters.to_snapshot();
        let mut data = Vec::new();
        snapshot
            .serialize_into(&mut data)
            .expect("serialization must succeed");

        let mut cursor = data.as_slice();
        let snapshot =
            CountersSnapshot::deserialize_from(&mut cursor).expect("deserialization must succeed");
        let loaded = Counters::from_snapshot(snapshot).expect("snapshot must be valid");

        assert_eq!(get_ok(&loaded, Idx::new(2)), RefCount::new(2));
        assert_eq!(get_ok(&loaded, Idx::new(4)), RefCount::new(257));
        assert_eq!(loaded.next_idx(), NextIdx::new(11));
        assert_eq!(loaded.snapshot_height(), SnapshotHeight::new(42));
    }

    #[test]
    fn rejects_big_without_present_membership() {
        let mut counters = Counters::new(NextIdx::new(10), SnapshotHeight::new(0));
        set_ok(&mut counters, Idx::new(9), RefCount::new(257));

        let mut snapshot = counters.to_snapshot();
        snapshot.present.remove(9);

        assert_invalid_snapshot(snapshot);
    }

    #[test]
    fn rejects_small_outside_present() {
        let counters = Counters::new(NextIdx::new(10), SnapshotHeight::new(0));
        let mut snapshot = counters.to_snapshot();

        snapshot.small_bits[0].insert(77);

        assert_invalid_snapshot(snapshot);
    }

    #[test]
    fn rejects_big_and_small_overlap() {
        let mut counters = Counters::new(NextIdx::new(11), SnapshotHeight::new(0));
        set_ok(&mut counters, Idx::new(10), RefCount::new(257));

        let mut snapshot = counters.to_snapshot();
        snapshot.small_bits[0].insert(10);

        assert_invalid_snapshot(snapshot);
    }

    #[test]
    fn rejects_non_monotonic_next_idx_on_load() {
        let mut counters = Counters::new(NextIdx::new(5), SnapshotHeight::new(0));
        set_ok(&mut counters, Idx::new(4), RefCount::new(1));

        let mut snapshot = counters.to_snapshot();
        snapshot.next_idx = NextIdx::new(4);

        assert_invalid_snapshot(snapshot);
    }

    #[test]
    fn get_and_set_reject_unallocated_idx() {
        let mut counters = Counters::new(NextIdx::new(5), SnapshotHeight::new(0));
        let idx = Idx::new(5);

        assert!(matches!(
            counters.get(idx),
            Err(CountersError::IdxOutOfBounds {
                idx: Idx(5),
                next_idx: NextIdx(5)
            })
        ));
        assert!(matches!(
            counters.set(idx, RefCount::new(1)),
            Err(CountersError::IdxOutOfBounds {
                idx: Idx(5),
                next_idx: NextIdx(5)
            })
        ));
    }

    #[test]
    fn alloc_idx_and_snapshot_height_work() {
        let mut counters = Counters::new(NextIdx::new(100), SnapshotHeight::new(1));

        assert_eq!(counters.alloc_idx(), Idx::new(100));
        assert_eq!(counters.alloc_idx(), Idx::new(101));
        assert_eq!(counters.next_idx(), NextIdx::new(102));
        assert_eq!(counters.snapshot_height().get(), 1);
    }
}
