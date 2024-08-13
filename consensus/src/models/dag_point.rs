use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tycho_network::PeerId;

use crate::models::point::{Digest, PointId};
use crate::models::{PointInfo, Round};

#[derive(Clone, Debug)]
pub struct ValidPoint {
    pub info: PointInfo,
    pub is_committed: Arc<AtomicBool>,
}

impl ValidPoint {
    pub fn new(info: PointInfo) -> Self {
        Self {
            info,
            is_committed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone, Debug)]
pub enum DagPoint {
    /// valid without demur, needed to blame equivocation or graph connectivity violations
    Trusted(ValidPoint),
    /// is a valid container, but we doubt author's fairness at the moment of validating;
    /// we do not sign such a point, but others may include it without consequences;
    /// consensus will decide whether to sign its proof or not; we shall ban the author anyway
    Suspicious(ValidPoint),
    /// invalidates dependent point; needed to blame equivocation
    Invalid(PointInfo),
    /// point hash or signature mismatch, not well-formed, download failed - i.e. unusable point;
    /// invalidates dependent point; blame author of dependent point
    NotExists(Arc<PointId>),
}

impl DagPoint {
    pub fn into_valid(self) -> Option<ValidPoint> {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn valid(&self) -> Option<&'_ ValidPoint> {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn trusted(&self) -> Option<&'_ ValidPoint> {
        match self {
            Self::Trusted(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn author(&self) -> PeerId {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => valid.info.data().author,
            Self::Invalid(info) => info.data().author,
            Self::NotExists(id) => id.author,
        }
    }

    pub fn round(&self) -> Round {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => valid.info.round(),
            Self::Invalid(info) => info.round(),
            Self::NotExists(id) => id.round,
        }
    }

    pub fn digest(&self) -> &'_ Digest {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => valid.info.digest(),
            Self::Invalid(info) => info.digest(),
            Self::NotExists(id) => &id.digest,
        }
    }
}
