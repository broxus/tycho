use std::fmt::{Display, Formatter};
use std::sync::Arc;

use tycho_network::PeerId;

use crate::dag::{IllFormedReason, InvalidDependency, InvalidReason};
use crate::effects::{AltFmt, AltFormat};
use crate::models::cert::Cert;
use crate::models::point::{Digest, PointId};
use crate::models::point_status::{
    PointStatusIllFormed, PointStatusInvalid, PointStatusNotFound, PointStatusTransInvalid,
    PointStatusValid,
};
use crate::models::{PointInfo, PointKey, Round};

#[derive(Clone)]
/// cases with point hash or signature mismatch are not represented in enum;
/// any mismatch from third party nodes:
/// * in broadcast or download response - makes sender not reliable
/// * in dependency graph - cannot be used, most likely will not be downloaded, i.e. `NotExist`
pub enum DagPoint {
    Valid(ValidPoint),
    /// A subtree with not valid sink points has `commit_history_rounds` length of transitionally
    /// invalid points. I.e. it acts as invalid locally but quorum decision may be to commit
    /// but invalid/ill-formed/not-found points stay out of commit history anyway.
    /// Quorum decision to commit must not contradict local one because of all:
    /// * DAG length is limited and not reproducible across network
    /// * points to be signed and included must have a valid `commit_history_rounds` subtree
    /// * deeper points may be of any kind and are ignored anyway
    TransInvalid(TransInvalidPoint),
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
    pub fn new_valid(info: PointInfo, cert: Cert, status: &PointStatusValid) -> Self {
        DagPoint::Valid(ValidPoint(Arc::new(ValidPointInner {
            info,
            first_valid: status.is_first_valid,
            is_first_resolved: status.is_first_resolved,
            cert,
        })))
    }

    pub fn new_trans_invalid(
        info: PointInfo,
        cert: Cert,
        status: &PointStatusTransInvalid,
        root_cause: InvalidDependency,
    ) -> Self {
        DagPoint::TransInvalid(TransInvalidPoint(Arc::new(TransInvalidPointInner {
            info,
            is_first_resolved: status.is_first_resolved,
            cert,
            root_cause,
            is_restored: status.is_restored,
        })))
    }

    pub fn new_invalid(
        info: PointInfo,
        cert: Cert,
        status: &PointStatusInvalid,
        reason: InvalidReason,
    ) -> Self {
        DagPoint::Invalid(InvalidPoint(Arc::new(InvalidPointInner {
            info,
            is_first_resolved: status.is_first_resolved,
            cert,
            reason,
        })))
    }

    pub fn new_ill_formed(
        id: PointId,
        cert: Cert,
        status: &PointStatusIllFormed,
        reason: IllFormedReason,
    ) -> Self {
        DagPoint::IllFormed(IllFormedPoint(Arc::new(IllFormedPointInner {
            id,
            is_first_resolved: status.is_first_resolved,
            cert,
            reason,
        })))
    }

    pub fn new_not_found(id: PointId, cert: Cert, status: &PointStatusNotFound) -> Self {
        DagPoint::NotFound(NotFoundPoint(Arc::new(NotFoundPointInner {
            id,
            is_first_resolved: status.is_first_resolved,
            cert,
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
            Self::TransInvalid(invalid) if invalid.is_certified() => Some(invalid.info()),
            Self::Invalid(invalid) if invalid.is_certified() => Some(invalid.info()),
            _ => None,
        }
    }

    pub fn id(&self) -> &PointId {
        match self {
            Self::Valid(valid) => valid.info().id(),
            Self::TransInvalid(trans_invalid) => trans_invalid.info().id(),
            Self::Invalid(invalid) => invalid.info().id(),
            Self::IllFormed(ill) => ill.id(),
            Self::NotFound(not_found) => not_found.id(),
        }
    }

    pub fn author(&self) -> &PeerId {
        &self.id().author
    }

    pub fn round(&self) -> Round {
        self.id().round
    }

    pub fn digest(&self) -> &Digest {
        &self.id().digest
    }

    pub fn key(&self) -> PointKey {
        self.id().key()
    }

    pub fn prev_digest(&self) -> Option<&Digest> {
        let info = match self {
            Self::Valid(valid) => valid.info(),
            Self::TransInvalid(trans_invalid) => trans_invalid.info(),
            Self::Invalid(invalid) => invalid.info(),
            Self::IllFormed(_) | Self::NotFound(_) => return None,
        };
        info.prev_digest()
    }

    pub fn is_first_resolved(&self) -> bool {
        match self {
            Self::Valid(valid) => valid.is_first_resolved(),
            Self::TransInvalid(trans_invalid) => trans_invalid.is_first_resolved(),
            Self::Invalid(invalid) => invalid.is_first_resolved(),
            Self::IllFormed(ill) => ill.is_first_resolved(),
            Self::NotFound(not_found) => not_found.is_first_resolved(),
        }
    }

    pub fn is_certified(&self) -> bool {
        match self {
            Self::Valid(valid) => valid.is_certified(),
            Self::TransInvalid(trans_invalid) => trans_invalid.is_certified(),
            Self::Invalid(invalid) => invalid.is_certified(),
            Self::IllFormed(ill) => ill.is_certified(),
            Self::NotFound(not_found) => not_found.is_certified(),
        }
    }
}

pub struct Committable(CommittableInner);
enum CommittableInner {
    Valid(ValidPoint),
    TransInvalid(TransInvalidPoint),
}
impl Committable {
    pub fn info(&self) -> &PointInfo {
        self.0.info()
    }
    pub fn set_committed(&self) {
        self.0.cert().set_committed();
    }
    pub fn is_committed(&self) -> bool {
        self.0.cert().is_committed()
    }
}
impl CommittableInner {
    fn info(&self) -> &PointInfo {
        match self {
            Self::Valid(valid) => valid.info(),
            Self::TransInvalid(invalid) => invalid.info(),
        }
    }
    fn cert(&self) -> &Cert {
        match self {
            Self::Valid(valid) => &valid.0.cert,
            Self::TransInvalid(invalid) => &invalid.0.cert,
        }
    }
}

#[derive(Clone)]
pub struct ValidPoint(Arc<ValidPointInner>);
struct ValidPointInner {
    info: PointInfo,
    first_valid: bool,
    is_first_resolved: bool,
    cert: Cert,
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
    pub fn is_certified(&self) -> bool {
        self.0.cert.is_certified()
    }
    pub fn committable(self) -> Committable {
        Committable(CommittableInner::Valid(self))
    }
}

#[derive(Clone)]
pub struct TransInvalidPoint(Arc<TransInvalidPointInner>);
struct TransInvalidPointInner {
    info: PointInfo,
    is_first_resolved: bool,
    cert: Cert,
    root_cause: InvalidDependency,
    is_restored: bool,
}
impl TransInvalidPoint {
    pub fn info(&self) -> &PointInfo {
        &self.0.info
    }
    pub fn is_first_resolved(&self) -> bool {
        self.0.is_first_resolved
    }
    pub fn has_proof(&self) -> bool {
        self.0.cert.has_proof()
    }
    pub fn is_certified(&self) -> bool {
        self.0.cert.is_certified()
    }
    pub fn root_cause(&self) -> &InvalidDependency {
        &self.0.root_cause
    }
    pub fn committable(self) -> Committable {
        Committable(CommittableInner::TransInvalid(self))
    }
    pub fn is_restored(&self) -> bool {
        self.0.is_restored
    }
}

#[derive(Clone)]
pub struct InvalidPoint(Arc<InvalidPointInner>);
struct InvalidPointInner {
    info: PointInfo,
    is_first_resolved: bool,
    cert: Cert,
    reason: InvalidReason,
}
impl InvalidPoint {
    pub fn info(&self) -> &PointInfo {
        &self.0.info
    }
    pub fn is_first_resolved(&self) -> bool {
        self.0.is_first_resolved
    }
    pub fn is_certified(&self) -> bool {
        self.0.cert.is_certified()
    }
    pub fn reason(&self) -> &InvalidReason {
        &self.0.reason
    }
}

#[derive(Clone)]
pub struct IllFormedPoint(Arc<IllFormedPointInner>);
struct IllFormedPointInner {
    id: PointId,
    is_first_resolved: bool,
    cert: Cert,
    reason: IllFormedReason,
}
impl IllFormedPoint {
    pub fn id(&self) -> &PointId {
        &self.0.id
    }
    pub fn is_first_resolved(&self) -> bool {
        self.0.is_first_resolved
    }
    pub fn is_certified(&self) -> bool {
        self.0.cert.is_certified()
    }
    pub fn reason(&self) -> &IllFormedReason {
        &self.0.reason
    }
}

#[derive(Clone)]
pub struct NotFoundPoint(Arc<NotFoundPointInner>);
struct NotFoundPointInner {
    id: PointId,
    is_first_resolved: bool,
    cert: Cert,
}
impl NotFoundPoint {
    pub fn id(&self) -> &PointId {
        &self.0.id
    }
    pub fn is_first_resolved(&self) -> bool {
        self.0.is_first_resolved
    }
    pub fn is_certified(&self) -> bool {
        self.0.cert.is_certified()
    }
}

/// also see impl of [`AltFormat`] for [`PointStatus`](tycho_storage::point_status::PointStatus)
impl AltFormat for DagPoint {}
impl Display for AltFmt<'_, DagPoint> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            DagPoint::Valid(valid) => Display::fmt(&valid.alt(), f),
            DagPoint::TransInvalid(valid) => Display::fmt(&valid.alt(), f),
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
        if valid.0.cert.is_certified() {
            tuple.field(&"certified");
        }
        if valid.0.cert.is_committed() {
            tuple.field(&"committed");
        }
        tuple.finish()
    }
}

impl AltFormat for TransInvalidPoint {}
impl Display for AltFmt<'_, TransInvalidPoint> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let invalid = AltFormat::unpack(self);
        let mut tuple = f.debug_tuple("TransInvalidPoint");
        if invalid.0.is_first_resolved {
            tuple.field(&"first resolved");
        }
        if invalid.0.cert.is_certified() {
            tuple.field(&"certified");
        }
        tuple.field(&invalid.0.root_cause);
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
        if invalid.0.cert.is_certified() {
            tuple.field(&"certified");
        }
        tuple.field(&format!("{}", &invalid.0.reason));
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
        if ill.0.cert.is_certified() {
            tuple.field(&"certified");
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
        if not_found.0.cert.is_certified() {
            tuple.field(&"certified");
        }
        tuple.finish()
    }
}
