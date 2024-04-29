use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::models::point::{Digest, Location, Point, PointId};

#[derive(Clone, Debug)]
pub struct ValidPoint {
    pub point: Arc<Point>,
    pub is_committed: Arc<AtomicBool>,
}

impl ValidPoint {
    pub fn new(point: Arc<Point>) -> Self {
        Self {
            point,
            is_committed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone, Debug)]
pub enum DagPoint {
    // FIXME time skew is determined at the moment of signature response and is not reentrant
    /// valid without demur, needed to blame equivocation or graph connectivity violations
    Trusted(ValidPoint),
    /// is a valid container, but we doubt author's fairness at the moment of validating;
    /// we do not sign such a point, but others may include it without consequences;
    /// consensus will decide whether to sign its proof or not; we shall ban the author anyway
    Suspicious(ValidPoint),
    /// invalidates dependent point; needed to blame equivocation
    Invalid(Arc<Point>),
    /// invalidates dependent point; blame author of dependent point
    NotExists(Arc<PointId>),
}

impl DagPoint {
    pub fn into_valid(self) -> Option<ValidPoint> {
        match self {
            DagPoint::Trusted(valid) => Some(valid),
            DagPoint::Suspicious(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn valid(&self) -> Option<&'_ ValidPoint> {
        match self {
            DagPoint::Trusted(valid) => Some(valid),
            DagPoint::Suspicious(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn trusted(&self) -> Option<&'_ ValidPoint> {
        match self {
            DagPoint::Trusted(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn id(&self) -> PointId {
        PointId {
            location: self.location().clone(),
            digest: self.digest().clone(),
        }
    }

    pub fn location(&self) -> &'_ Location {
        match self {
            DagPoint::Trusted(valid) => &valid.point.body.location,
            DagPoint::Suspicious(valid) => &valid.point.body.location,
            DagPoint::Invalid(point) => &point.body.location,
            DagPoint::NotExists(id) => &id.location,
        }
    }

    pub fn digest(&self) -> &'_ Digest {
        match self {
            DagPoint::Trusted(valid) => &valid.point.digest,
            DagPoint::Suspicious(valid) => &valid.point.digest,
            DagPoint::Invalid(point) => &point.digest,
            DagPoint::NotExists(id) => &id.digest,
        }
    }
}
