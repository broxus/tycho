use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::models::point::{Digest, Location, Point, PointId};

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
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Trusted(valid) => Some(valid),
            Self::Suspicious(valid) => Some(valid),
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
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Trusted(valid) => Some(valid),
            Self::Suspicious(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn trusted(&self) -> Option<&'_ ValidPoint> {
        match self {
            Self::Trusted(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn location(&self) -> &'_ Location {
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Trusted(valid) => &valid.point.body().location,
            Self::Suspicious(valid) => &valid.point.body().location,
            Self::Invalid(point) => &point.body().location,
            Self::NotExists(id) => &id.location,
        }
    }

    pub fn digest(&self) -> &'_ Digest {
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Trusted(valid) => valid.point.digest(),
            Self::Suspicious(valid) => valid.point.digest(),
            Self::Invalid(point) => point.digest(),
            Self::NotExists(id) => &id.digest,
        }
    }
}
