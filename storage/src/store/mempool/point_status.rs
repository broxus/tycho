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
    pub anchor_chain_role: Option<AnchorChainRole>,
    pub committed_at_round: Option<u32>, // not committed are stored with impossible zero round
}

#[derive(Copy, Clone, Debug)]
pub enum AnchorChainRole {
    Trigger, // not necessarily unique; contained in some point field
    Anchor,  // unique; not contained in point
    Proof,   // unique; contained in some point field
}

bitflags::bitflags! {
    struct Flags : u8 {
        const IllFormed = 0b_1 << 6;
        const Validated = 0b_1 << 5;
        const Valid = 0b_1 << 4;
        const Trusted = 0b_1 << 3;
        const Certified = 0b_1 << 2;
        const AnchorChainRoleUnique = 0b_1 << 1;
        const AnchorChainRoleInPoint = 0b_1 << 0;
    }
}

impl From<Flags> for PointStatus {
    fn from(flags: Flags) -> Self {
        Self {
            is_ill_formed: flags.contains(Flags::IllFormed),
            is_validated: flags.contains(Flags::Validated),
            is_valid: flags.contains(Flags::Valid),
            is_trusted: flags.contains(Flags::Trusted),
            is_certified: flags.contains(Flags::Certified),
            anchor_chain_role: match (
                flags.contains(Flags::AnchorChainRoleUnique),
                flags.contains(Flags::AnchorChainRoleInPoint),
            ) {
                (false, false) => None,
                (false, true) => Some(AnchorChainRole::Trigger),
                (true, false) => Some(AnchorChainRole::Anchor),
                (true, true) => Some(AnchorChainRole::Proof),
            },
            committed_at_round: None,
        }
    }
}

impl From<&PointStatus> for Flags {
    fn from(status: &PointStatus) -> Self {
        let mut flags = Flags::empty();
        flags.set(Flags::IllFormed, status.is_ill_formed);
        flags.set(Flags::Validated, status.is_validated);
        flags.set(Flags::Valid, status.is_valid);
        flags.set(Flags::Trusted, status.is_trusted);
        flags.set(Flags::Certified, status.is_certified);
        if let Some(role) = status.anchor_chain_role {
            flags.insert(match role {
                AnchorChainRole::Trigger => Flags::AnchorChainRoleInPoint,
                AnchorChainRole::Anchor => Flags::AnchorChainRoleUnique,
                AnchorChainRole::Proof => {
                    Flags::AnchorChainRoleInPoint | Flags::AnchorChainRoleUnique
                }
            });
        }
        flags
    }
}

impl PointStatus {
    const BYTES: usize = 1 + 4;
    const DEFAULT: [u8; Self::BYTES] = [0, 0, 0, 0, 0];

    pub fn encode(&self) -> [u8; Self::BYTES] {
        let mut result = Self::DEFAULT;

        result[0] = Flags::from(self).bits();

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
        let mut status = PointStatus::from(Flags::from_bits_retain(stored[0]));

        let mut committed_at = [0_u8; Self::BYTES - 1];
        committed_at.copy_from_slice(&stored[1..]);
        let committed_at = u32::from_be_bytes(committed_at);
        if committed_at != 0 {
            status.committed_at_round = Some(committed_at);
        }

        Ok(status)
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
