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
    pub fn new<T: Into<PointInfo>>(into_info: T) -> Self {
        Self {
            info: into_info.into(),
            is_committed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone, Debug)]
/// cases with point hash or signature mismatch are not represented in enum;
/// at most we are able to use [`crate::dag::DagRound::set_bad_sig_in_broadcast_exact`],
/// but any bad signatures from third party nodes:
/// * in download response - makes sender not reliable
/// * in dependency graph - cannot be used, most likely will not be downloaded, i.e. `NotExist`
pub enum DagPoint {
    /// valid without demur, needed to blame equivocation or graph connectivity violations
    Trusted(ValidPoint),
    /// is a valid container, but we doubt author's fairness at the moment of validating;
    /// we do not sign such a point, but others may include it without consequences;
    /// consensus will decide whether to sign its proof or not; we shall ban the author anyway
    Suspicious(ValidPoint),
    /// dependency issues;
    /// invalidates dependent point; needed to blame equivocation
    Invalid(PointInfo),
    /// not well-formed, unusable point;
    /// invalidates dependent point; blame author of dependent point
    IllFormed(Arc<PointId>),
    /// download failed despite multiple retries;
    /// invalidates dependent point; blame author of dependent point
    NotFound(Arc<PointId>),
}

impl DagPoint {
    pub fn trusted(&self) -> Option<&ValidPoint> {
        match self {
            Self::Trusted(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn valid(&self) -> Option<&ValidPoint> {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn into_valid(self) -> Option<ValidPoint> {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn author(&self) -> PeerId {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => valid.info.data().author,
            Self::Invalid(info) => info.data().author,
            Self::IllFormed(id) | Self::NotFound(id) => id.author,
        }
    }

    pub fn round(&self) -> Round {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => valid.info.round(),
            Self::Invalid(info) => info.round(),
            Self::IllFormed(id) | Self::NotFound(id) => id.round,
        }
    }

    pub fn digest(&self) -> &Digest {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => valid.info.digest(),
            Self::Invalid(info) => info.digest(),
            Self::IllFormed(id) | Self::NotFound(id) => &id.digest,
        }
    }
}
