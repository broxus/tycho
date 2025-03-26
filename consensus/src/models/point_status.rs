use std::fmt::{Debug, Display, Formatter};

use tycho_network::PeerId;
use tycho_storage::point_status::{AnchorFlags, StatusFlags};

use crate::effects::{AltFmt, AltFormat};
use crate::models::{Digest, PointId, PointInfo, PrevPointProof, Round};

pub enum PointRestoreSelect {
    /// Note: currently all points with `Exists` statuses must repeat `verify()`,
    ///       as this status also represents an error during merge of DB statuses
    NeedsVerify(Round, Digest),
    Ready(PointRestore),
}
pub enum PointRestore {
    /// non-terminal status can be found in DB if point task was aborted before validation
    Exists(PointInfo, Option<PrevPointProof>),
    Validated(PointInfo, PointStatusValidated),
    IllFormed(PointId, PointStatusIllFormed),
    NotFound(Round, Digest, PointStatusNotFound),
}
impl PointRestore {
    /// required partial order: resolved + valid, resolved, valid, no flags, no status
    pub fn restore_order_asc(&self) -> u8 {
        /// greater value for greater priority
        fn order_desc<T: PointStatus>(status: &T) -> u8 {
            let mut priority = 1;
            priority |= (status.is_first_resolved() as u8) << 7;
            priority |= (status.is_first_valid() as u8) << 6;
            priority |= (status.is_valid() as u8) << 5;
            priority
        }
        let desc = match self {
            PointRestore::Exists(_, _) => 0,
            PointRestore::Validated(_, status) => order_desc(status),
            PointRestore::IllFormed(_, status) => order_desc(status),
            PointRestore::NotFound(_, _, status) => order_desc(status),
        };
        !desc // invert priority for ascending order
    }
    pub fn round(&self) -> Round {
        match self {
            Self::Exists(info, _) | Self::Validated(info, _) => info.round(),
            Self::IllFormed(id, _) => id.round,
            Self::NotFound(round, _, _) => *round,
        }
    }
    pub fn author(&self) -> &PeerId {
        match self {
            Self::Exists(info, _) | Self::Validated(info, _) => &info.data().author,
            Self::IllFormed(id, _) => &id.author,
            Self::NotFound(_, _, status) => &status.author,
        }
    }
    pub fn digest(&self) -> &Digest {
        match self {
            Self::Exists(info, _) | Self::Validated(info, _) => info.digest(),
            Self::IllFormed(id, _) => &id.digest,
            Self::NotFound(_, digest, _) => digest,
        }
    }
    pub fn id(&self) -> PointId {
        PointId {
            author: *self.author(),
            round: self.round(),
            digest: *self.digest(),
        }
    }
}

impl AltFormat for PointRestore {}
impl Debug for AltFmt<'_, PointRestore> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(f, "Restore {{ {:?} ", inner.id().alt())?;
        match inner {
            PointRestore::Exists(_, Some(prev)) => {
                write!(f, "Exists prev # {}", prev.digest.alt())?;
            }
            PointRestore::Exists(_, None) => {
                f.write_str("Exists prev # None")?;
            }
            PointRestore::Validated(_, status) => {
                write!(f, "{status}")?;
            }
            PointRestore::IllFormed(_, status) => {
                write!(f, "{status}")?;
            }
            PointRestore::NotFound(_, _, status) => {
                let mut tuple = f.debug_tuple("NotFound");
                if status.is_first_resolved {
                    tuple.field(&"first resolved");
                }
                if status.is_certified {
                    tuple.field(&"certified");
                }
                // author is shown in point id
                tuple.finish()?;
            }
        }
        f.write_str(" }")
    }
}

/// To read from DB
#[derive(Debug)]
pub enum PointStatusStored {
    Exists,
    Validated(PointStatusValidated),
    IllFormed(PointStatusIllFormed),
    NotFound(PointStatusNotFound),
}
impl Display for PointStatusStored {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.as_ref(), f)
    }
}

/// To pass `impl PointStatus` to DB that must not implement Clone.
pub enum PointStatusStoredRef<'a> {
    Exists,
    Validated(&'a PointStatusValidated),
    IllFormed(&'a PointStatusIllFormed),
    NotFound(&'a PointStatusNotFound),
}
impl Display for PointStatusStoredRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exists => f.write_str("Exists"),
            Self::Validated(resolved) => Display::fmt(resolved, f),
            Self::IllFormed(resolved) => Display::fmt(resolved, f),
            Self::NotFound(resolved) => Display::fmt(resolved, f),
        }
    }
}

pub trait PointStatus: Display {
    fn is_valid(&self) -> bool {
        false
    }
    fn set_first_valid(&mut self) {}
    fn is_first_valid(&self) -> bool {
        false
    }
    fn set_first_resolved(&mut self);
    fn is_first_resolved(&self) -> bool;
    fn size_hint() -> usize;
    fn write_to(&self, buffer: &mut Vec<u8>);

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::size_hint());
        self.write_to(&mut buf);
        buf
    }
}

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[derive(Debug, Default)]
pub struct PointStatusValidated {
    pub is_valid: bool,
    pub is_first_valid: bool,
    pub is_first_resolved: bool,
    pub is_certified: bool,
    anchor_flags: AnchorFlags, // TODO make public and fill, only zeros now
    pub committed_at_round: Option<u32>, // not committed are stored with impossible zero round
}
impl Display for PointStatusValidated {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = if self.is_valid {
            f.debug_tuple("Valid")
        } else {
            f.debug_tuple("Invalid")
        };
        if self.is_first_valid {
            tuple.field(&"first valid");
        }
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.is_certified {
            tuple.field(&"certified");
        }
        tuple.finish_non_exhaustive()
    }
}
impl PointStatus for PointStatusValidated {
    fn is_valid(&self) -> bool {
        self.is_valid
    }
    fn set_first_valid(&mut self) {
        self.is_first_valid = true;
    }
    fn is_first_valid(&self) -> bool {
        self.is_first_valid
    }
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
    fn size_hint() -> usize {
        StatusFlags::VALIDATED_BYTES
    }
    fn write_to(&self, buffer: &mut Vec<u8>) {
        let mut flags = StatusFlags::empty();
        flags.insert(StatusFlags::Found);
        flags.insert(StatusFlags::WellFormed);
        flags.set(StatusFlags::Valid, self.is_valid);
        flags.set(StatusFlags::FirstValid, self.is_first_valid);
        flags.set(StatusFlags::FirstResolved, self.is_first_resolved);
        flags.set(StatusFlags::Certified, self.is_certified);

        buffer.push(flags.bits());
        buffer.push(self.anchor_flags.bits());
        let at = self.committed_at_round.unwrap_or_default();
        buffer.extend_from_slice(&at.to_be_bytes());
    }
}

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[derive(Debug, Default)]
pub struct PointStatusIllFormed {
    pub is_first_resolved: bool,
    pub is_certified: bool,
}
impl Display for PointStatusIllFormed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("IllFormed");
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.is_certified {
            tuple.field(&"certified");
        }
        tuple.finish()
    }
}
impl PointStatus for PointStatusIllFormed {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
    fn size_hint() -> usize {
        StatusFlags::ILL_FORMED_BYTES
    }
    fn write_to(&self, buffer: &mut Vec<u8>) {
        let mut flags = StatusFlags::empty();
        flags.insert(StatusFlags::Found);
        flags.set(StatusFlags::FirstResolved, self.is_first_resolved);
        flags.set(StatusFlags::Certified, self.is_certified);

        buffer.push(flags.bits());
    }
}

/// Must not implement neither Copy nor Clone to prevent coding errors.
#[derive(Debug)]
pub struct PointStatusNotFound {
    pub is_first_resolved: bool,
    pub is_certified: bool,
    pub author: PeerId,
}
impl Display for PointStatusNotFound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut tuple = f.debug_tuple("NotFound");
        if self.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if self.is_certified {
            tuple.field(&"certified");
        }
        tuple.field(&format!("author: {}", self.author.alt()));
        tuple.finish()
    }
}

impl PointStatus for PointStatusNotFound {
    fn set_first_resolved(&mut self) {
        self.is_first_resolved = true;
    }
    fn is_first_resolved(&self) -> bool {
        self.is_first_resolved
    }
    fn size_hint() -> usize {
        StatusFlags::NOT_FOUND_BYTES
    }
    fn write_to(&self, buffer: &mut Vec<u8>) {
        let mut flags = StatusFlags::empty();
        flags.set(StatusFlags::FirstResolved, self.is_first_resolved);
        flags.set(StatusFlags::Certified, self.is_certified);

        buffer.push(flags.bits());
        buffer.extend_from_slice(&self.author.0);
    }
}

impl PointStatusStoredRef<'_> {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Exists => Vec::new(), // no data
            Self::Validated(resolved) => resolved.encode(),
            Self::IllFormed(resolved) => resolved.encode(),
            Self::NotFound(resolved) => resolved.encode(),
        }
    }

    pub fn write_to(&self, buffer: &mut Vec<u8>) {
        match self {
            Self::Exists => {} // no data
            Self::Validated(resolved) => resolved.write_to(buffer),
            Self::IllFormed(resolved) => resolved.write_to(buffer),
            Self::NotFound(resolved) => resolved.write_to(buffer),
        }
    }
}

impl PointStatusStored {
    pub fn as_ref(&self) -> PointStatusStoredRef<'_> {
        match self {
            Self::Exists => PointStatusStoredRef::Exists,
            Self::Validated(resolved) => PointStatusStoredRef::Validated(resolved),
            Self::IllFormed(resolved) => PointStatusStoredRef::IllFormed(resolved),
            Self::NotFound(resolved) => PointStatusStoredRef::NotFound(resolved),
        }
    }
    pub fn decode(stored: &[u8]) -> anyhow::Result<Self> {
        let Some(flags) = StatusFlags::try_from_stored(stored).map_err(anyhow::Error::msg)? else {
            return Ok(Self::Exists);
        };
        let resolved = if flags.contains(StatusFlags::Found) {
            if flags.contains(StatusFlags::WellFormed) {
                let mut committed_at = [0_u8; 4];
                committed_at.copy_from_slice(&stored[2..]);
                let committed_at = u32::from_be_bytes(committed_at);
                let committed_at_round = if committed_at == 0 {
                    None
                } else {
                    Some(committed_at)
                };
                Self::Validated(PointStatusValidated {
                    is_valid: flags.contains(StatusFlags::Valid),
                    is_first_valid: flags.contains(StatusFlags::FirstValid),
                    is_first_resolved: flags.contains(StatusFlags::FirstResolved),
                    is_certified: flags.contains(StatusFlags::Certified),
                    anchor_flags: AnchorFlags::from_bits_retain(stored[1]),
                    committed_at_round,
                })
            } else {
                Self::IllFormed(PointStatusIllFormed {
                    is_first_resolved: flags.contains(StatusFlags::FirstResolved),
                    is_certified: flags.contains(StatusFlags::Certified),
                })
            }
        } else {
            let mut author = [0_u8; 32];
            author.copy_from_slice(&stored[1..]);
            Self::NotFound(PointStatusNotFound {
                author: PeerId(author),
                is_first_resolved: flags.contains(StatusFlags::FirstResolved),
                is_certified: flags.contains(StatusFlags::Certified),
            })
        };
        Ok(resolved)
    }
}
