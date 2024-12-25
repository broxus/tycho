use std::sync::atomic::AtomicBool;
use std::sync::{atomic, Arc};

use tycho_network::PeerId;

use crate::dag::IllFormedReason;
use crate::effects::{AltFmt, AltFormat};
use crate::models::point::{Digest, PointId};
use crate::models::{
    PointInfo, PointStatusIllFormed, PointStatusInvalid, PointStatusNotFound, PointStatusValid,
    Round,
};

#[derive(Clone, Debug)]
/// cases with point hash or signature mismatch are not represented in enum;
/// at most we are able to use [`crate::dag::DagRound::set_bad_sig_in_broadcast_exact`],
/// but any bad signatures from third party nodes:
/// * in download response - makes sender not reliable
/// * in dependency graph - cannot be used, most likely will not be downloaded, i.e. `NotExist`
pub enum DagPoint {
    Valid(ValidPoint),
    /// dependency issues;
    /// invalidates dependent point; needed to blame equivocation
    Invalid(Cert<PointInfo>),
    /// not well-formed, unusable point;
    /// invalidates dependent point; blame author of dependent point
    IllFormed(IllFormedPoint),
    /// download failed despite multiple retries;
    /// invalidates dependent point; blame author of dependent point
    NotFound(Arc<Cert<PointId>>),
}

impl DagPoint {
    pub fn new_valid(info: PointInfo, status: &PointStatusValid) -> Self {
        DagPoint::Valid(ValidPoint {
            info,
            is_first_valid: status.is_first_valid,
            is_first_resolved: status.is_first_resolved,
            is_certified: status.is_certified,
            is_committed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn new_invalid(info: PointInfo, status: &PointStatusInvalid) -> Self {
        DagPoint::Invalid(Cert {
            inner: info,
            is_first_resolved: status.is_first_resolved,
            is_certified: status.is_certified,
        })
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
        DagPoint::NotFound(Arc::new(Cert {
            inner: PointId {
                author: status.author,
                round,
                digest: *digest,
            },
            is_first_resolved: status.is_first_resolved,
            is_certified: status.is_certified,
        }))
    }

    pub fn valid(&self) -> Option<&ValidPoint> {
        match self {
            Self::Valid(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn trusted(&self) -> Option<&PointInfo> {
        match self {
            Self::Valid(valid) => Some(&valid.info),
            Self::Invalid(cert) if cert.is_certified => Some(&cert.inner),
            _ => None,
        }
    }

    pub fn id(&self) -> PointId {
        PointId {
            author: self.author(),
            round: self.round(),
            digest: *self.digest(),
        }
    }

    pub fn author(&self) -> PeerId {
        match self {
            Self::Valid(valid) => valid.info.data().author,
            Self::Invalid(cert) => cert.inner.data().author,
            Self::IllFormed(ill) => ill.0.id.author,
            Self::NotFound(cert) => cert.inner.author,
        }
    }

    pub fn round(&self) -> Round {
        match self {
            Self::Valid(valid) => valid.info.round(),
            Self::Invalid(cert) => cert.inner.round(),
            Self::IllFormed(ill) => ill.0.id.round,
            Self::NotFound(cert) => cert.inner.round,
        }
    }

    pub fn digest(&self) -> &Digest {
        match self {
            Self::Valid(valid) => valid.info.digest(),
            Self::Invalid(cert) => cert.inner.digest(),
            Self::IllFormed(ill) => &ill.0.id.digest,
            Self::NotFound(cert) => &cert.inner.digest,
        }
    }

    pub fn is_first_resolved(&self) -> bool {
        match self {
            Self::Valid(valid) => valid.is_first_resolved,
            Self::Invalid(cert) => cert.is_first_resolved,
            Self::IllFormed(ill) => ill.0.is_first_resolved,
            Self::NotFound(cert) => cert.is_first_resolved,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ValidPoint {
    pub info: PointInfo,
    pub is_committed: Arc<AtomicBool>,
    pub is_certified: bool,
    pub is_first_valid: bool,
    is_first_resolved: bool,
}

#[derive(Clone)]
pub struct IllFormedPoint(Arc<IllFormedPointInner>);
struct IllFormedPointInner {
    id: PointId,
    is_first_resolved: bool,
    reason: IllFormedReason,
}
impl std::fmt::Debug for IllFormedPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IllFormedPoint")
            .field("round", &self.0.id.round.0)
            .field("author", &self.0.id.author)
            .field("digest", &self.0.id.digest)
            .field("is_first_resolved", &self.0.is_first_resolved)
            .field("reason", &format!("{}", self.0.reason))
            .finish()
    }
}

pub struct Cert<T> {
    pub inner: T,
    pub is_certified: bool,
    is_first_resolved: bool,
}

impl<T> Clone for Cert<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            is_first_resolved: self.is_first_resolved,
            is_certified: self.is_certified,
        }
    }
}

impl<T> std::fmt::Debug for Cert<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cert")
            .field("inner", &self.inner)
            .field("is_certified", &self.is_certified)
            .finish()
    }
}

/// also see impl of [`AltFormat`] for [`PointStatus`](tycho_storage::point_status::PointStatus)
impl AltFormat for DagPoint {}
impl std::fmt::Display for AltFmt<'_, DagPoint> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const FIRST_RESOLVED: &str = "first resolved";
        const CERTIFIED: &str = "certified";
        match AltFormat::unpack(self) {
            DagPoint::Valid(valid) => {
                let mut tuple = f.debug_tuple("Valid");
                if valid.is_first_valid {
                    tuple.field(&"first valid");
                }
                if valid.is_first_resolved {
                    tuple.field(&FIRST_RESOLVED);
                }
                if valid.is_certified {
                    tuple.field(&CERTIFIED);
                }
                if valid.is_committed.load(atomic::Ordering::Relaxed) {
                    tuple.field(&"committed");
                }
                tuple.finish()
            }
            DagPoint::Invalid(cert) => {
                let mut tuple = f.debug_tuple("Invalid");
                if cert.is_first_resolved {
                    tuple.field(&FIRST_RESOLVED);
                }
                if cert.is_certified {
                    tuple.field(&CERTIFIED);
                }
                tuple.finish()
            }
            DagPoint::IllFormed(ill) => {
                let mut tuple = f.debug_tuple("IllFormed");
                if ill.0.is_first_resolved {
                    tuple.field(&FIRST_RESOLVED);
                }
                tuple.field(&format!("reason: {}", ill.0.reason));
                tuple.finish()
            }
            DagPoint::NotFound(cert) => {
                let mut tuple = f.debug_tuple("NotFound");
                if cert.is_first_resolved {
                    tuple.field(&FIRST_RESOLVED);
                }
                if cert.is_certified {
                    tuple.field(&CERTIFIED);
                }
                tuple.finish()
            }
        }
    }
}
