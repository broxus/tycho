use anyhow::Result;
use weedb::rocksdb::MergeOperands;

use crate::MempoolStorage;

/// Flags are stored in their order from left to right
/// and may be merged from `false` to `true` but not vice versa.
/// Point must be committed at single round.
#[derive(Default, Debug)]
pub struct PointStatus {
    // points are stored only after verified: points with sig or digest mismatch are never stored;
    // certified points are validated, and validation takes place only after verification;
    // locally created points (incl. genesis) are the only not validated before get stored;
    pub is_ill_formed: bool, // some well-formness is checked only on validate()
    pub is_validated: bool,  // need to distinguish not-yet-validated from invalid
    pub is_valid: bool,      // a point may be validated as invalid but certified afterward
    pub is_trusted: bool,    // locally decided as not equivocated (cannot change decision later)
    pub is_certified: bool,  // some points won't be marked because they are already validated
    pub anchor_chain_role: AnchorChainRole,
    pub committed_at_round: Option<u32>, // not committed are stored with impossible zero round
}

#[repr(u8)]
enum FlagsMask {
    IllFormed = 0b_1 << 7,
    Validated = 0b_1 << 6,
    Valid = 0b_1 << 5,
    Trusted = 0b_1 << 4,
    Certified = 0b_1 << 3,
    AnchorChainRole = 0b_11,
}

#[derive(Copy, Clone, Debug, Default)]
#[repr(u8)]
pub enum AnchorChainRole {
    #[default]
    None = 0b_00, // not unique
    Trigger = 0b_01, // acceptable to be non-unique in a round; has AnchorRole::ToSelf in Point
    Anchor = 0b_10,  // unique per round as referenced by Proof; doesn't have AnchorRole in Point
    Proof = 0b_11,   // unique per round as referenced by Trigger or later Anchor that has a Proof
}

impl AnchorChainRole {
    fn from_flags(value: u8) -> Result<Self> {
        match FlagsMask::AnchorChainRole as u8 & value {
            0b_00 => Ok(Self::None),
            0b_01 => Ok(Self::Trigger),
            0b_10 => Ok(Self::Anchor),
            0b_11 => Ok(Self::Proof),
            _ => anyhow::bail!("invalid AnchorChain role flag mask"),
        }
    }
}

impl PointStatus {
    const BYTES: usize = 1 + 4;
    const DEFAULT: [u8; Self::BYTES] = [0, 0, 0, 0, 0];

    pub fn encode(&self) -> [u8; Self::BYTES] {
        let mut result = Self::DEFAULT;

        if self.is_ill_formed {
            result[0] |= FlagsMask::IllFormed as u8;
        }
        if self.is_validated {
            result[0] |= FlagsMask::Validated as u8;
        }
        if self.is_valid {
            result[0] |= FlagsMask::Valid as u8;
        }
        if self.is_trusted {
            result[0] |= FlagsMask::Trusted as u8;
        }
        if self.is_certified {
            result[0] |= FlagsMask::Certified as u8;
        }
        result[0] |= self.anchor_chain_role as u8;
        if let Some(at) = self.committed_at_round {
            result[1..].copy_from_slice(&at.to_be_bytes()[..]);
        }

        result
    }

    pub fn decode(stored: &[u8]) -> Result<Self> {
        if stored.len() != Self::BYTES {
            anyhow::bail!(
                "unexpected amount of bytes for stored status: {}",
                stored.len()
            );
        }
        let flags = stored[0];
        let mut committed_at = [0_u8; Self::BYTES - 1];
        committed_at.copy_from_slice(&stored[1..]);
        let committed_at = u32::from_be_bytes(committed_at);
        Ok(PointStatus {
            is_ill_formed: (FlagsMask::IllFormed as u8 & flags) > 0,
            is_validated: (FlagsMask::Validated as u8 & flags) > 0,
            is_valid: (FlagsMask::Valid as u8 & flags) > 0,
            is_trusted: (FlagsMask::Trusted as u8 & flags) > 0,
            is_certified: (FlagsMask::Certified as u8 & flags) > 0,
            anchor_chain_role: AnchorChainRole::from_flags(flags)?,
            committed_at_round: if committed_at == 0 {
                None
            } else {
                Some(committed_at)
            },
        })
    }

    pub(crate) fn merge(
        key: &[u8],
        stored: Option<&[u8]>,
        new_status_queue: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = Self::DEFAULT;
        if let Some(stored) = stored {
            if stored.len() == Self::BYTES {
                result.copy_from_slice(stored);
            } else {
                tracing::error!(
                    "point status merge discarded unexpected stored value of {} bytes for {}",
                    stored.len(),
                    MempoolStorage::format_key(key)
                );
            }
        }

        for new_status in new_status_queue {
            if new_status.len() != Self::BYTES {
                tracing::error!(
                    "point status merge discarded unexpected new point status of {} bytes for {}",
                    new_status.len(),
                    MempoolStorage::format_key(key)
                );
                continue;
            }

            if new_status[1..] != Self::DEFAULT[1..] {
                if result[1..] == Self::DEFAULT[1..] {
                    result[1..].copy_from_slice(&new_status[1..]);
                } else if new_status[1..] != result[1..] {
                    let old = MempoolStorage::parse_round(&result[1..]);
                    let new = MempoolStorage::parse_round(&new_status[1..]);
                    tracing::error!(
                        "point status merge skipped for different commit rounds: \
                         {old:?} and {new:?} for {}",
                        MempoolStorage::format_key(key)
                    );
                    continue;
                }
            }
            result[0] |= new_status[0];
        }

        Some(Vec::from(result))
    }
}
