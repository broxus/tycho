use weedb::rocksdb::MergeOperands;

use crate::effects::AltFormat;
use crate::models::PointKey;

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
    pub const VALIDATED_BYTES: usize = 1 + 1 + 8;
    pub const ILL_FORMED_BYTES: usize = 1;
    pub const NOT_FOUND_BYTES: usize = 1 + 32;

    pub fn try_from_stored(value: &[u8]) -> anyhow::Result<Option<Self>> {
        if value.is_empty() {
            return Ok(None);
        }
        let len = value.len();
        let flags = Self::from_bits_retain(value[0]);
        let is_ok = if !flags.contains(Self::Found) {
            len == Self::NOT_FOUND_BYTES
        } else if !flags.contains(Self::WellFormed) {
            len == Self::ILL_FORMED_BYTES
        } else {
            len == Self::VALIDATED_BYTES
        };
        anyhow::ensure!(is_ok, "unexpected {len} bytes for stored status: {flags:?}");
        Ok(Some(flags))
    }
}

pub(super) fn merge(
    key: &[u8],
    stored: Option<&[u8]>,
    new_status_queue: &MergeOperands,
) -> Option<Vec<u8>> {
    fn none_if_err_or_empty(key: &[u8], value: &[u8]) -> Option<StatusFlags> {
        StatusFlags::try_from_stored(value).unwrap_or_else(|msg| {
            tracing::error!(
                target: MEMPOOL_DB_STATUS_MERGE,
                "ignore {msg}, key: {}", PointKey::format_loose(key)
            );
            None
        })
    }
    fn merge_validated(validated: &mut [u8; StatusFlags::VALIDATED_BYTES], other: &[u8]) {
        validated[0] = validated[0].max(other[0]); // status
        validated[1] |= other[1]; // anchor flags
        // the rest is committed info
        if validated[2..] < other[2..] {
            validated[2..].copy_from_slice(&other[2..]);
        }
    }

    let mut validated: [u8; StatusFlags::VALIDATED_BYTES] = [0; _];
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
                            (&a[1..]).alt(),
                            (&b[1..]).alt(),
                            PointKey::format_loose(key)
                        );
                    }
                    return a.max(b); // max by u8 for common length, else the longest
                }
                (true, true) => {} // continue
            }
            match (
                a_flags.contains(StatusFlags::WellFormed),
                b_flags.contains(StatusFlags::WellFormed),
            ) {
                (_, false) => a, // only already merged flags byte is stored for invalid
                (false, true) => b,
                (true, true) => {
                    merge_validated(&mut validated, a);
                    b
                }
            }
        })
        .map(|c| {
            if validated > [0; _] {
                if validated.len() == c.len() {
                    merge_validated(&mut validated, c);
                    validated.to_vec()
                } else {
                    let error_msg = "expected other status to be validated";
                    debug_assert!(false, "{error_msg}");
                    // cannot panic in production, will spam logs to gain attention
                    tracing::error!(target: MEMPOOL_DB_STATUS_MERGE, "{error_msg}");
                    Vec::new() // dismiss stored status
                }
            } else {
                c.to_vec()
            }
        })
}
