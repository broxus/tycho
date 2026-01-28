use std::fmt::{Display, Formatter};

use super::*;
use crate::models::PointKey;

/// To read from DB
#[cfg_attr(any(test, feature = "test"), derive(PartialEq))]
pub enum PointStatusStored {
    Valid(PointStatusValid),
    TransInvalid(PointStatusTransInvalid),
    Invalid(PointStatusInvalid),
    IllFormed(PointStatusIllFormed),
    NotFound(PointStatusNotFound),
    Found(PointStatusFound),
    Committable(PointStatusCommittable),
    Proven(PointStatusProven),
}

impl PointStatusStored {
    /// point is enough to be a well-formed evidence container to act as a usable cert,
    /// in par with [`crate::dag::Verifier::validate`]
    pub fn can_certify(&self) -> bool {
        match self {
            Self::Valid(_) | Self::TransInvalid(_) | Self::Invalid(_) | Self::Proven(_) => true,
            Self::IllFormed(_) | Self::NotFound(_) | Self::Found(_) | Self::Committable(_) => false,
        }
    }

    pub fn byte_size(&self) -> usize {
        match self {
            Self::Valid(_) => PointStatusValid::BYTE_SIZE,
            Self::TransInvalid(_) => PointStatusTransInvalid::BYTE_SIZE,
            Self::Invalid(_) => PointStatusInvalid::BYTE_SIZE,
            Self::IllFormed(_) => PointStatusIllFormed::BYTE_SIZE,
            Self::NotFound(_) => PointStatusNotFound::BYTE_SIZE,
            Self::Found(_) => PointStatusFound::BYTE_SIZE,
            Self::Committable(_) => PointStatusCommittable::BYTE_SIZE,
            Self::Proven(_) => PointStatusProven::BYTE_SIZE,
        }
    }

    pub fn write_to(&self, buffer: &mut Vec<u8>) {
        match self {
            Self::Valid(val) => val.write_to(buffer),
            Self::TransInvalid(val) => val.write_to(buffer),
            Self::Invalid(val) => val.write_to(buffer),
            Self::IllFormed(val) => val.write_to(buffer),
            Self::NotFound(val) => val.write_to(buffer),
            Self::Found(val) => val.write_to(buffer),
            Self::Committable(val) => val.write_to(buffer),
            Self::Proven(val) => val.write_to(buffer),
        }
    }

    pub fn read_flags(value: &[u8]) -> anyhow::Result<StatusFlags> {
        Self::read_flags_inner(value).map(|(flags, _)| flags)
    }

    fn read_flags_inner(value: &[u8]) -> anyhow::Result<(StatusFlags, u8)> {
        let len = value.len();
        anyhow::ensure!(len >= 2, "too short len {len} bytes for stored status");

        let mut raw: [u8; 2] = [0; _];
        raw.copy_from_slice(&value[..2]);
        let flags = StatusFlags::from_bits_retain(u16::from_be_bytes(raw));

        let type_byte = if flags.contains(StatusFlags::Resolved) {
            if flags.contains(PointStatusValid::DEFAULT_FLAGS) {
                PointStatusValid::TYPE
            } else if flags.contains(PointStatusTransInvalid::DEFAULT_FLAGS) {
                PointStatusTransInvalid::TYPE
            } else if flags.contains(PointStatusInvalid::DEFAULT_FLAGS) {
                PointStatusInvalid::TYPE
            } else if flags.contains(PointStatusIllFormed::DEFAULT_FLAGS) {
                PointStatusIllFormed::TYPE
            } else if flags.contains(PointStatusNotFound::DEFAULT_FLAGS) {
                PointStatusNotFound::TYPE
            } else {
                anyhow::bail!("unknown resolved flags {flags:?}")
            }
        } else if flags.contains(PointStatusFound::DEFAULT_FLAGS) {
            PointStatusFound::TYPE
        } else if flags.contains(PointStatusCommittable::DEFAULT_FLAGS) {
            PointStatusCommittable::TYPE
        } else {
            PointStatusProven::TYPE
        };

        let expected_len = match type_byte {
            PointStatusValid::TYPE => PointStatusValid::BYTE_SIZE,
            PointStatusTransInvalid::TYPE => PointStatusTransInvalid::BYTE_SIZE,
            PointStatusInvalid::TYPE => PointStatusInvalid::BYTE_SIZE,
            PointStatusIllFormed::TYPE => PointStatusIllFormed::BYTE_SIZE,
            PointStatusNotFound::TYPE => PointStatusNotFound::BYTE_SIZE,
            PointStatusFound::TYPE => PointStatusFound::BYTE_SIZE,
            PointStatusCommittable::TYPE => PointStatusCommittable::BYTE_SIZE,
            PointStatusProven::TYPE => PointStatusProven::BYTE_SIZE,
            _ => anyhow::bail!("len of unknown type for flags {flags:?}"),
        };

        let is_ok = len == expected_len;
        anyhow::ensure!(is_ok, "unexpected {len} bytes for stored status: {flags:?}");
        Ok((flags, type_byte))
    }

    pub fn decode(stored: &[u8]) -> anyhow::Result<Self> {
        let (flags, type_byte) = Self::read_flags_inner(stored)?;

        Ok(match type_byte {
            PointStatusValid::TYPE => Self::Valid(<_>::read(flags, stored)?),
            PointStatusTransInvalid::TYPE => Self::TransInvalid(<_>::read(flags, stored)?),
            PointStatusInvalid::TYPE => Self::Invalid(<_>::read(flags, stored)?),
            PointStatusIllFormed::TYPE => Self::IllFormed(<_>::read(flags, stored)?),
            PointStatusNotFound::TYPE => Self::NotFound(<_>::read(flags, stored)?),
            PointStatusFound::TYPE => Self::Found(<_>::read(flags, stored)?),
            PointStatusCommittable::TYPE => Self::Committable(<_>::read(flags, stored)?),
            PointStatusProven::TYPE => Self::Proven(<_>::read(flags, stored)?),
            _ => anyhow::bail!("read unknown type for flags {flags:?}"),
        })
    }
}

pub fn merge_bytes<'a>(key: &[u8], iter: impl Iterator<Item = &'a [u8]>) -> Option<Vec<u8>> {
    fn none_if_err_or_empty(key: &[u8], value: &[u8]) -> Option<StatusFlags> {
        match PointStatusStored::read_flags(value) {
            Ok(flags) => Some(flags),
            Err(err) => {
                tracing::error!(
                    target: "MEMPOOL_DB_STATUS_MERGE",
                    "ignore {err}, key: {}", PointKey::format_loose(key)
                );
                None
            }
        }
    }

    let mut second_flags: u8 = 0; // i = 1 for all
    let mut anchor_flags: u8 = 0; // i = 2, well-formed only
    let mut commit_part: [u8; CommitHistoryPart::BYTE_SIZE] = [0; _]; // well-formed only

    let (mut result, flags) = iter
        .filter_map(|a| {
            let flags = none_if_err_or_empty(key, a)?;

            second_flags |= a[1];

            if flags.contains(StatusFlags::Committable) {
                anchor_flags |= a[2];
                if commit_part[..] < a[CommitHistoryPart::RANGE] {
                    commit_part.copy_from_slice(&a[CommitHistoryPart::RANGE]);
                }
            }

            Some((a, flags))
        })
        .max()
        .map(|(a, flags)| (a.to_vec(), flags))?;

    result[1] |= second_flags;

    if flags.contains(StatusFlags::Committable) {
        result[2] |= anchor_flags;
        result[CommitHistoryPart::RANGE].copy_from_slice(&commit_part);
    }
    Some(result)
}

impl Display for PointStatusStored {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Valid(val) => Display::fmt(val, f),
            Self::TransInvalid(val) => Display::fmt(val, f),
            Self::Invalid(val) => Display::fmt(val, f),
            Self::IllFormed(val) => Display::fmt(val, f),
            Self::NotFound(val) => Display::fmt(val, f),
            Self::Found(val) => Display::fmt(val, f),
            Self::Committable(val) => Display::fmt(val, f),
            Self::Proven(val) => Display::fmt(val, f),
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use itertools::Itertools;

    use super::*;
    use crate::test_utils::default_test_config;

    const VALID: &str = "Valid";
    const TRANS_INVALID: &str = "TransInvalid";
    const INVALID: &str = "Invalid";
    const ILL_FORMED: &str = "IllFormed";
    const NOT_FOUND: &str = "NotFound";
    const FOUND: &str = "Found";
    const COMMITTABLE: &str = "Committable";
    const PROVEN: &str = "Proven";

    #[test]
    fn decode_one() -> Result<()> {
        for _ in 0..10 {
            let data = [
                (VALID, PointStatusValid::random().bytes()),
                (TRANS_INVALID, PointStatusTransInvalid::random().bytes()),
                (INVALID, PointStatusInvalid::random().bytes()),
                (ILL_FORMED, PointStatusIllFormed::random().bytes()),
                (NOT_FOUND, PointStatusNotFound::random().bytes()),
                (FOUND, PointStatusFound::random().bytes()),
                (COMMITTABLE, PointStatusCommittable::random().bytes()),
                (PROVEN, PointStatusProven::random().bytes()),
            ];

            for (expected, bytes) in data {
                match (PointStatusStored::decode(&bytes)?, expected) {
                    (PointStatusStored::Valid(_), VALID)
                    | (PointStatusStored::TransInvalid(_), TRANS_INVALID)
                    | (PointStatusStored::Invalid(_), INVALID)
                    | (PointStatusStored::IllFormed(_), ILL_FORMED)
                    | (PointStatusStored::NotFound(_), NOT_FOUND)
                    | (PointStatusStored::Found(_), FOUND)
                    | (PointStatusStored::Committable(_), COMMITTABLE)
                    | (PointStatusStored::Proven(_), PROVEN) => {}
                    (other, _) => anyhow::bail!("{expected} read as {other}"),
                }
            }
        }
        Ok(())
    }

    fn init_bytes() -> Result<Vec<(StatusFlags, Vec<u8>)>> {
        [
            PointStatusValid::random().bytes(),
            PointStatusTransInvalid::random().bytes(),
            PointStatusInvalid::random().bytes(),
            PointStatusIllFormed::random().bytes(),
            PointStatusNotFound::random().bytes(),
            PointStatusFound::random().bytes(),
            PointStatusCommittable::random().bytes(),
            PointStatusProven::random().bytes(),
        ]
        .into_iter()
        .enumerate()
        .map(|(pos, bytes)| {
            PointStatusStored::read_flags(&bytes)
                .map(|flags| (flags, bytes))
                .with_context(|| format!("init read flags pos {pos}"))
        })
        .collect()
    }

    #[test]
    fn decode_merged_pair_duplicates() -> Result<()> {
        let k = PointKey::random().bytes();

        for ((a_flags, a_bytes), (b_flags, b_bytes)) in init_bytes()?.into_iter().zip(init_bytes()?)
        {
            let merged = merge_bytes(&k[..], [&a_bytes[..], &b_bytes[..]].into_iter())
                .with_context(|| format!("merged to None: \n{a_flags:?} + \n{b_flags:?}"))?;

            let merged_flags = PointStatusStored::read_flags(&merged)
                .with_context(|| format!("read merged flags: \n{a_flags:?} + \n{b_flags:?}"))?;

            PointStatusStored::decode(&merged).with_context(|| {
                format!("decode merged: \n{a_flags:?} + \n{b_flags:?} = \n{merged_flags:?}")
            })?;
        }

        Ok(())
    }

    #[test]
    fn decode_merged_all_unique() -> Result<()> {
        let k = PointKey::random().bytes();

        let init_bytes = init_bytes()?;

        for size in 1..=init_bytes.len() {
            for mergee in init_bytes.iter().combinations(size) {
                let flags = || {
                    (mergee.iter())
                        .map(|(flags, _)| format!("\n{flags:?}"))
                        .join(" +")
                };

                let merged = merge_bytes(&k[..], mergee.iter().map(|(_, bytes)| &bytes[..]))
                    .with_context(|| format!("merged to None: {}", flags()))?;

                let merged_flags = PointStatusStored::read_flags(&merged)
                    .with_context(|| format!("read merged flags: {}", flags()))?;

                PointStatusStored::decode(&merged)
                    .with_context(|| format!("decode merged: {} = \n{merged_flags:?}", flags()))?;
            }
        }

        Ok(())
    }

    #[test]
    fn not_found_has_lowest_priority_for_existing_points() -> Result<()> {
        let _ = default_test_config();

        let k = PointKey::random().bytes();

        let not_found = PointStatusNotFound::random().bytes();

        let data = [
            (VALID, PointStatusValid::random().bytes()),
            (TRANS_INVALID, PointStatusTransInvalid::random().bytes()),
            (INVALID, PointStatusInvalid::random().bytes()),
            (ILL_FORMED, PointStatusIllFormed::random().bytes()),
            (FOUND, PointStatusFound::random().bytes()),
        ];
        let data = (data.iter())
            .flat_map(|(name, bytes)| [(*name, bytes, &not_found), (*name, &not_found, bytes)]);

        for (expected, lhs, rhs) in data {
            let merged = merge_bytes(&k[..], [&lhs[..], &rhs[..]].into_iter()).context(expected)?;

            match (PointStatusStored::decode(&merged)?, expected) {
                (PointStatusStored::Valid(_), VALID)
                | (PointStatusStored::TransInvalid(_), TRANS_INVALID)
                | (PointStatusStored::Invalid(_), INVALID)
                | (PointStatusStored::IllFormed(_), ILL_FORMED)
                | (PointStatusStored::Found(_), FOUND) => {}
                (other, _) => anyhow::bail!("merged into {other}, expected {expected}"),
            }
        }

        Ok(())
    }
}
