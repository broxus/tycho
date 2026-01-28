use std::fmt::Formatter;

use tycho_network::PeerId;

use crate::effects::{AltFmt, AltFormat};
use crate::models::point_status::{
    PointStatus, PointStatusFound, PointStatusIllFormed, PointStatusInvalid, PointStatusNotFound,
    PointStatusTransInvalid, PointStatusValid,
};
use crate::models::{Digest, PointId, PointInfo, PointKey, Round};

/// Note: all points with `Found` status must repeat `verify()`,
///       because vset may have changed after status reset on history fix
pub enum PointRestore {
    Valid(PointInfo, PointStatusValid),
    TransInvalid(PointInfo, PointStatusTransInvalid),
    Invalid(PointInfo, PointStatusInvalid),
    IllFormed(PointId, PointStatusIllFormed),
    NotFound(PointKey, PointStatusNotFound),
    Found(PointInfo, PointStatusFound),
}

impl PointRestore {
    /// required partial order: resolved + valid, resolved, valid, no flags, no status
    pub fn restore_order_asc(&self) -> u8 {
        /// greater value for greater priority
        fn order_desc<T: PointStatus>(status: &T) -> u8 {
            let mut priority = 2;
            priority |= (status.is_first_resolved() as u8) << 7;
            priority |= (status.is_first_valid() as u8) << 6;
            priority |= (T::is_valid() as u8) << 5;
            priority
        }
        let desc = match self {
            PointRestore::Valid(_, status) => order_desc(status),
            PointRestore::TransInvalid(_, status) => order_desc(status),
            PointRestore::Invalid(_, status) => order_desc(status),
            PointRestore::IllFormed(_, status) => order_desc(status),
            PointRestore::NotFound(_, status) => order_desc(status),
            PointRestore::Found(_, _) => 0,
        } + self.has_proof() as u8;
        !desc // invert priority for ascending order
    }

    pub fn round(&self) -> Round {
        match self {
            Self::Valid(info, _)
            | Self::TransInvalid(info, _)
            | Self::Invalid(info, _)
            | Self::Found(info, _) => info.round(),
            Self::IllFormed(id, _) => id.round,
            Self::NotFound(key, _) => key.round,
        }
    }

    pub fn author(&self) -> &PeerId {
        match self {
            Self::Valid(info, _)
            | Self::TransInvalid(info, _)
            | Self::Invalid(info, _)
            | Self::Found(info, _) => info.author(),
            Self::IllFormed(id, _) => &id.author,
            Self::NotFound(_, status) => &status.author,
        }
    }

    pub fn digest(&self) -> &Digest {
        match self {
            Self::Valid(info, _)
            | Self::TransInvalid(info, _)
            | Self::Invalid(info, _)
            | Self::Found(info, _) => info.digest(),
            Self::IllFormed(id, _) => &id.digest,
            Self::NotFound(key, _) => &key.digest,
        }
    }

    pub fn id(&self) -> PointId {
        PointId {
            author: *self.author(),
            round: self.round(),
            digest: *self.digest(),
        }
    }

    pub fn has_proof(&self) -> bool {
        match self {
            Self::Valid(_, status) => status.has_proof,
            Self::TransInvalid(_, status) => status.has_proof,
            Self::Invalid(_, status) => status.has_proof,
            Self::IllFormed(_, status) => status.has_proof,
            Self::NotFound(_, status) => status.has_proof,
            Self::Found(_, status) => status.has_proof,
        }
    }
}

impl AltFormat for PointRestore {}
impl std::fmt::Debug for AltFmt<'_, PointRestore> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(f, "Restore {{ {:?} ", inner.id().alt())?;
        match inner {
            PointRestore::Valid(_, status) => {
                write!(f, "{status}")?;
            }
            PointRestore::TransInvalid(_, status) => {
                write!(f, "{status}")?;
            }
            PointRestore::Invalid(_, status) => {
                write!(f, "{status}")?;
            }
            PointRestore::IllFormed(_, status) => {
                write!(f, "{status}")?;
            }
            PointRestore::NotFound(_, status) => {
                let mut tuple = f.debug_tuple("NotFound");
                if status.is_first_resolved {
                    tuple.field(&"first resolved");
                }
                if status.has_proof {
                    tuple.field(&"has proof");
                }
                // author is shown in point id
                tuple.finish()?;
            }
            PointRestore::Found(_, status) => {
                write!(f, "{status}")?;
            }
        }
        f.write_str(" }")
    }
}
