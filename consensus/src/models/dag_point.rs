use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tycho_network::PeerId;

use crate::models::point::{Digest, Point, PointId};
use crate::models::Round;

#[derive(Clone, Debug)]
pub struct ValidPoint {
    pub point: Point,
    pub is_committed: Arc<AtomicBool>,
}

impl ValidPoint {
    pub fn new(point: Point) -> Self {
        Self {
            point,
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
    Invalid(Point),
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

    pub fn into_trusted(self) -> Option<ValidPoint> {
        match self {
            Self::Trusted(valid) => Some(valid),
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
            Self::Trusted(valid) | Self::Suspicious(valid) => valid.point.body().author,
            Self::Invalid(point) => point.body().author,
            Self::NotExists(id) => id.author,
        }
    }

    pub fn round(&self) -> Round {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => valid.point.body().round,
            Self::Invalid(point) => point.body().round,
            Self::NotExists(id) => id.round,
        }
    }

    pub fn digest(&self) -> &'_ Digest {
        match self {
            Self::Trusted(valid) | Self::Suspicious(valid) => valid.point.digest(),
            Self::Invalid(point) => point.digest(),
            Self::NotExists(id) => &id.digest,
        }
    }
}
