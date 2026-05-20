use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::time::Instant;

pub(super) type BuildTrustedCellHasher = BuildHasherDefault<TrustedCellHasher>;

pub(super) type CellDashMap<V> = dashmap::DashMap<HashBytesKey, V, BuildTrustedCellHasher>;
// hashbrown measured ~20% faster than std HashMap for nursery insert workloads.
pub(super) type CellHashMap<V> = hashbrown::HashMap<HashBytesKey, V, BuildTrustedCellHasher>;

pub(super) fn elapsed_us(started_at: Instant) -> u64 {
    started_at.elapsed().as_micros() as u64
}

#[cfg(test)]
pub(super) fn test_hash(value: u8) -> tycho_types::cell::HashBytes {
    tycho_types::cell::HashBytes([value; 32])
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub(super) struct HashBytesKey(pub [u8; 32]);

impl HashBytesKey {
    #[inline(always)]
    pub const fn wrap(value: &[u8; 32]) -> &Self {
        // SAFETY: HashBytesKey is #[repr(transparent)] over [u8; 32].
        unsafe { &*(value as *const [u8; 32]).cast::<Self>() }
    }
}

impl Hash for HashBytesKey {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(u64::from_le_bytes(self.0[..8].try_into().unwrap()));
    }
}

#[derive(Default)]
pub(super) struct TrustedCellHasher {
    hash: u64,
    written: bool,
}

impl Hasher for TrustedCellHasher {
    #[inline]
    fn finish(&self) -> u64 {
        if !self.written {
            panic!("TrustedCellHasher: finish() called before hashing a cell hash key");
        }
        self.hash
    }

    #[inline]
    fn write(&mut self, _bytes: &[u8]) {
        panic!("TrustedCellHasher: expected HashBytesKey::hash to call write_u64");
    }

    #[inline]
    fn write_u64(&mut self, hash: u64) {
        if self.written {
            panic!("TrustedCellHasher: expected exactly one write_u64");
        }
        self.hash = hash;
        self.written = true;
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{Hash, Hasher};

    use super::{HashBytesKey, TrustedCellHasher};

    #[derive(Default)]
    struct RecordingHasher {
        write_calls: usize,
        write_u64_calls: usize,
        hash: u64,
    }

    impl Hasher for RecordingHasher {
        fn finish(&self) -> u64 {
            self.hash
        }

        fn write(&mut self, _bytes: &[u8]) {
            self.write_calls += 1;
        }

        fn write_u64(&mut self, hash: u64) {
            self.write_u64_calls += 1;
            self.hash = hash;
        }
    }

    #[test]
    fn hash_bytes_key_hashes_first_lane_without_prefix() {
        let bytes = [7; 32];
        let mut hasher = RecordingHasher::default();
        HashBytesKey(bytes).hash(&mut hasher);

        assert_eq!(hasher.write_calls, 0);
        assert_eq!(hasher.write_u64_calls, 1);
        assert_eq!(hasher.finish(), u64::from_le_bytes([7; 8]));
    }

    #[test]
    fn hash_bytes_key_wrap_hashes_first_lane_without_prefix() {
        let bytes = [11; 32];
        let mut hasher = RecordingHasher::default();
        HashBytesKey::wrap(&bytes).hash(&mut hasher);

        assert_eq!(hasher.write_calls, 0);
        assert_eq!(hasher.write_u64_calls, 1);
        assert_eq!(hasher.finish(), u64::from_le_bytes([11; 8]));
    }

    #[test]
    fn trusted_cell_hasher_accepts_one_u64() {
        let mut hasher = TrustedCellHasher::default();
        hasher.write_u64(123);
        assert_eq!(hasher.finish(), 123);
    }

    #[test]
    #[should_panic(expected = "finish() called before hashing")]
    fn trusted_cell_hasher_panics_without_key() {
        let _ = TrustedCellHasher::default().finish();
    }

    #[test]
    #[should_panic(expected = "expected HashBytesKey::hash")]
    fn trusted_cell_hasher_rejects_raw_bytes() {
        TrustedCellHasher::default().write(&[0; 32]);
    }

    #[test]
    #[should_panic(expected = "expected exactly one write_u64")]
    fn trusted_cell_hasher_rejects_duplicate_writes() {
        let mut hasher = TrustedCellHasher::default();
        hasher.write_u64(1);
        hasher.write_u64(2);
    }
}
