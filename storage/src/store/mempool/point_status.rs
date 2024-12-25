use weedb::rocksdb::MergeOperands;

use crate::{BytesFmt, MempoolStorage};

const MEMPOOL_DB_STATUS_MERGE: &str = "MEMPOOL_DB_STATUS_MERGE";

// Flags are stored in their order from left to right
// and may be merged from `false` to `true` but not vice versa.
bitflags::bitflags! {
    #[derive(Copy, Clone, Debug)]
    pub struct StatusFlags : u8 {
        const Found = 0b_1 << 7;
        const WellFormed = 0b_1 << 6;
        const Valid = 0b_1 << 5;
        const FirstValid = 0b_1 << 4;
        const FirstResolved = 0b_1 << 3;
        const Certified = 0b_1 << 2;
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Default, Debug)]
    pub struct AnchorFlags : u8 {
        const Used = 0b_1 << 7;
        const Trigger = 0b_1 << 6;
        const Proof = 0b_1 << 5;
    }
}

impl StatusFlags {
    pub const VALID_BYTES: usize = 1 + 1 + 4;
    pub const INVALID_BYTES: usize = 1;
    pub const ILL_FORMED_BYTES: usize = 1;
    pub const NOT_FOUND_BYTES: usize = 1 + 32;

    pub fn try_from_stored(value: &[u8]) -> Result<Option<Self>, String> {
        if value.is_empty() {
            return Ok(None);
        }
        let len = value.len();
        let flags = Self::from_bits_retain(value[0]);
        let is_ok = if !flags.contains(Self::Found) {
            len == Self::NOT_FOUND_BYTES
        } else if !flags.contains(Self::WellFormed) {
            len == Self::ILL_FORMED_BYTES
        } else if !flags.contains(Self::Valid) {
            len == Self::INVALID_BYTES
        } else {
            len == Self::VALID_BYTES
        };
        if is_ok {
            Ok(Some(flags))
        } else {
            Err(format!(
                "unexpected {len} bytes for stored status: {flags:?}",
            ))
        }
    }
}

pub(crate) fn merge(
    key: &[u8],
    stored: Option<&[u8]>,
    new_status_queue: &MergeOperands,
) -> Option<Vec<u8>> {
    fn none_if_err_or_empty(key: &[u8], value: &[u8]) -> Option<StatusFlags> {
        StatusFlags::try_from_stored(value).unwrap_or_else(|msg| {
            tracing::error!(
                target: MEMPOOL_DB_STATUS_MERGE,
                "ignore {msg}, key: {}", MempoolStorage::format_key(key)
            );
            None
        })
    }

    let mut status_flags = StatusFlags::empty();
    let mut anchor_flags = 0_u8;
    stored
        .into_iter()
        .chain(new_status_queue)
        .reduce(|a, b| {
            let Some(a_flags) = none_if_err_or_empty(key, a) else {
                return b;
            };
            let Some(b_flags) = none_if_err_or_empty(key, b) else {
                return a;
            };
            // restart is definitely not reproducible if next errors occur
            if a_flags.contains(StatusFlags::FirstResolved)
                != b_flags.contains(StatusFlags::FirstResolved)
            {
                tracing::error!(
                    target: MEMPOOL_DB_STATUS_MERGE,
                    "FIRST_RESOLVED flag mismatch for {a_flags:?} and {b_flags:?}, key: {}",
                    MempoolStorage::format_key(key)
                );
            }
            if a_flags.contains(StatusFlags::FirstValid)
                != b_flags.contains(StatusFlags::FirstValid)
            {
                tracing::error!(
                    target: MEMPOOL_DB_STATUS_MERGE,
                    "FIRST_VALID flag mismatch for {a_flags:?} and {b_flags:?}, key: {}",
                    MempoolStorage::format_key(key)
                );
            }
            // try our best even if errors above occurred, favouring flags set to 'true'
            status_flags |= a_flags | b_flags;
            match (
                a_flags.contains(StatusFlags::Found),
                b_flags.contains(StatusFlags::Found),
            ) {
                (true, false) => return a,
                (false, true) => return b,
                (false, false) => {
                    if a[1..] != b[1..] {
                        tracing::error!(
                            target: MEMPOOL_DB_STATUS_MERGE,
                            "cannot merge NOT_FOUND author: use {} ignore {}, key {}",
                            BytesFmt(&a[1..]),
                            BytesFmt(&b[1..]),
                            MempoolStorage::format_key(key)
                        );
                    }
                    return a;
                }
                (true, true) => {} // continue
            }
            match (
                a_flags.contains(StatusFlags::WellFormed),
                b_flags.contains(StatusFlags::WellFormed),
            ) {
                (_, false) => return a, // only already merged flags byte is stored for ill-formed
                (false, true) => return b,
                (true, true) => {} // continue
            }
            match (
                a_flags.contains(StatusFlags::Valid),
                b_flags.contains(StatusFlags::Valid),
            ) {
                (_, false) => a, // only already merged flags byte is stored for invalid
                (false, true) => b,
                (true, true) => {
                    anchor_flags |= a[1] | b[1];
                    // take the greatest commit round, because not committed stores all zeros
                    if a[2..] >= b[2..] {
                        a
                    } else {
                        b
                    }
                }
            }
        })
        .map(|c| match none_if_err_or_empty(key, c) {
            Some(flags) => {
                let mut result = c.to_vec();
                result[0] |= flags.bits();
                let valid_combo = StatusFlags::Found | StatusFlags::WellFormed | StatusFlags::Valid;
                if flags.contains(valid_combo) {
                    result[1] |= anchor_flags;
                }
                result
            }
            None => Vec::new(), // keep empty
        })
}
