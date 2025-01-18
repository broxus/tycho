use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::{atomic, Arc};

use tycho_network::PeerId;

use crate::dag::IllFormedReason;
use crate::effects::{AltFmt, AltFormat};
use crate::models::point::{Digest, PointId};
use crate::models::{
    PointInfo, PointStatusIllFormed, PointStatusNotFound, PointStatusValidated, Round,
};

#[derive(Clone)]
/// cases with point hash or signature mismatch are not represented in enum;
/// any mismatch from third party nodes:
/// * in broadcast or download response - makes sender not reliable
/// * in dependency graph - cannot be used, most likely will not be downloaded, i.e. `NotExist`
pub enum DagPoint {
    Valid(ValidPoint),
    /// dependency issues;
    /// invalidates dependent point; needed to blame equivocation
    Invalid(InvalidPoint),
    /// not well-formed, unusable point;
    /// invalidates dependent point; blame author of dependent point
    IllFormed(IllFormedPoint),
    /// download failed despite multiple retries;
    /// invalidates dependent point; blame author of dependent point
    NotFound(NotFoundPoint),
}

impl DagPoint {
    pub fn new_validated(info: PointInfo, status: &PointStatusValidated) -> Self {
        if status.is_valid {
            DagPoint::Valid(ValidPoint(Arc::new(ValidPointInner {
                info,
                first_valid: status.is_first_valid,
                is_first_resolved: status.is_first_resolved,
                is_certified: status.is_certified,
                is_committed: AtomicBool::new(false), // ignore status to repeat commit after reboot
            })))
        } else {
            DagPoint::Invalid(InvalidPoint(Arc::new(InvalidPointInner {
                info,
                is_first_resolved: status.is_first_resolved,
                is_certified: status.is_certified,
            })))
        }
    }

    pub fn new_ill_formed(
        id: PointId,
        status: &PointStatusIllFormed,
        reason: IllFormedReason,
    ) -> Self {
        DagPoint::IllFormed(IllFormedPoint(Arc::new(IllFormedPointInner {
            id,
            is_first_resolved: status.is_first_resolved,
            reason,
        })))
    }

    pub fn new_not_found(round: Round, digest: &Digest, status: &PointStatusNotFound) -> Self {
        DagPoint::NotFound(NotFoundPoint(Arc::new(NotFoundPointInner {
            id: PointId {
                author: status.author,
                round,
                digest: *digest,
            },
            is_first_resolved: status.is_first_resolved,
            is_certified: status.is_certified,
        })))
    }

    pub fn valid(&self) -> Option<&ValidPoint> {
        match self {
            Self::Valid(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn trusted(&self) -> Option<&PointInfo> {
        match self {
            Self::Valid(valid) => Some(valid.info()),
            Self::Invalid(invalid) if invalid.is_certified() => Some(invalid.info()),
            _ => None,
        }
    }

    pub fn id(&self) -> PointId {
        match self {
            Self::Valid(valid) => valid.info().id(),
            Self::Invalid(invalid) => invalid.info().id(),
            Self::IllFormed(ill) => *ill.id(),
            Self::NotFound(not_found) => *not_found.id(),
        }
    }

    pub fn author(&self) -> PeerId {
        match self {
            Self::Valid(valid) => valid.info().data().author,
            Self::Invalid(invalid) => invalid.info().data().author,
            Self::IllFormed(ill) => ill.id().author,
            Self::NotFound(not_found) => not_found.id().author,
        }
    }

    pub fn round(&self) -> Round {
        match self {
            Self::Valid(valid) => valid.info().round(),
            Self::Invalid(invalid) => invalid.info().round(),
            Self::IllFormed(ill) => ill.id().round,
            Self::NotFound(not_found) => not_found.id().round,
        }
    }

    pub fn digest(&self) -> &Digest {
        match self {
            Self::Valid(valid) => valid.info().digest(),
            Self::Invalid(invalid) => invalid.info().digest(),
            Self::IllFormed(ill) => &ill.id().digest,
            Self::NotFound(not_found) => &not_found.id().digest,
        }
    }

    pub fn is_first_resolved(&self) -> bool {
        match self {
            Self::Valid(valid) => valid.is_first_resolved(),
            Self::Invalid(invalid) => invalid.is_first_resolved(),
            Self::IllFormed(ill) => ill.is_first_resolved(),
            Self::NotFound(not_found) => not_found.is_first_resolved(),
        }
    }
}

#[derive(Clone)]
pub struct ValidPoint(Arc<ValidPointInner>);
struct ValidPointInner {
    info: PointInfo,
    first_valid: bool,
    is_first_resolved: bool,
    is_certified: bool,
    is_committed: AtomicBool,
}
impl ValidPoint {
    pub fn info(&self) -> &PointInfo {
        &self.0.info
    }
    pub fn is_first_valid(&self) -> bool {
        self.0.first_valid
    }
    pub fn is_first_resolved(&self) -> bool {
        self.0.is_first_resolved
    }
    #[allow(dead_code, reason = "surprisingly, flag is used only in DB operations")]
    pub fn is_certified(&self) -> bool {
        self.0.is_certified
    }
    pub fn is_committed(&self) -> &AtomicBool {
        &self.0.is_committed
    }
}

#[derive(Clone)]
pub struct InvalidPoint(Arc<InvalidPointInner>);
struct InvalidPointInner {
    info: PointInfo,
    is_first_resolved: bool,
    is_certified: bool,
}
impl InvalidPoint {
    pub fn info(&self) -> &PointInfo {
        &self.0.info
    }
    pub fn is_first_resolved(&self) -> bool {
        self.0.is_first_resolved
    }
    pub fn is_certified(&self) -> bool {
        self.0.is_certified
    }
}

#[derive(Clone)]
pub struct IllFormedPoint(Arc<IllFormedPointInner>);
struct IllFormedPointInner {
    id: PointId,
    is_first_resolved: bool,
    reason: IllFormedReason,
}
impl IllFormedPoint {
    pub fn id(&self) -> &PointId {
        &self.0.id
    }
    pub fn is_first_resolved(&self) -> bool {
        self.0.is_first_resolved
    }
}

#[derive(Clone)]
pub struct NotFoundPoint(Arc<NotFoundPointInner>);
struct NotFoundPointInner {
    id: PointId,
    is_first_resolved: bool,
    is_certified: bool,
}
impl NotFoundPoint {
    pub fn id(&self) -> &PointId {
        &self.0.id
    }
    pub fn is_first_resolved(&self) -> bool {
        self.0.is_first_resolved
    }
    pub fn is_certified(&self) -> bool {
        self.0.is_certified
    }
}

/// also see impl of [`AltFormat`] for [`PointStatus`](tycho_storage::point_status::PointStatus)
impl AltFormat for DagPoint {}
impl Display for AltFmt<'_, DagPoint> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            DagPoint::Valid(valid) => Display::fmt(&valid.alt(), f),
            DagPoint::Invalid(invalid) => Display::fmt(&invalid.alt(), f),
            DagPoint::IllFormed(ill) => Display::fmt(&ill.alt(), f),
            DagPoint::NotFound(not_found) => Display::fmt(&not_found.alt(), f),
        }
    }
}

impl AltFormat for ValidPoint {}
impl Display for AltFmt<'_, ValidPoint> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let valid = AltFormat::unpack(self);
        let mut tuple = f.debug_tuple("Valid");
        if valid.0.first_valid {
            tuple.field(&"first valid");
        }
        if valid.0.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if valid.0.is_certified {
            tuple.field(&"certified");
        }
        if valid.0.is_committed.load(atomic::Ordering::Relaxed) {
            tuple.field(&"committed");
        }
        tuple.finish()
    }
}

impl AltFormat for InvalidPoint {}
impl Display for AltFmt<'_, InvalidPoint> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let invalid = AltFormat::unpack(self);
        let mut tuple = f.debug_tuple("Invalid");
        if invalid.0.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if invalid.0.is_certified {
            tuple.field(&"certified");
        }
        tuple.finish()
    }
}

impl AltFormat for IllFormedPoint {}
impl Display for AltFmt<'_, IllFormedPoint> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ill = AltFormat::unpack(self);
        let mut tuple = f.debug_tuple("IllFormed");
        if ill.0.is_first_resolved {
            tuple.field(&"first resolved");
        }
        tuple.field(&format!("{}", &ill.0.reason));
        tuple.finish()
    }
}

impl AltFormat for NotFoundPoint {}
impl Display for AltFmt<'_, NotFoundPoint> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let not_found = AltFormat::unpack(self);
        let mut tuple = f.debug_tuple("NotFound");
        if not_found.0.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if not_found.0.is_certified {
            tuple.field(&"certified");
        }
        tuple.finish()
    }
}
