use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tycho_network::PeerId;
use tycho_storage::point_status::PointStatus;

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
    /// (In)direct dependency of some point that gathered consensus signatures;
    /// no need to validate its dependencies, can trust consensus;
    /// may include it into own history, but cannot sign it (it's strange to receive such a request);
    /// applicable for downloaded points not directly participating in validations of current round
    Certified(ValidPoint),
    /// is a valid container, but we doubt author's fairness at the moment of validating;
    /// we neither sign nor include such a point, but others may include it without consequences;
    /// consensus will decide whether to sign its proof or not; we shall ban the author anyway
    Suspicious(ValidPoint),
    /// dependency issues;
    /// invalidates dependent point; needed to blame equivocation
    Invalid(Cert<PointInfo>),
    /// not well-formed, unusable point;
    /// invalidates dependent point; blame author of dependent point
    IllFormed(Arc<PointId>),
    /// download failed despite multiple retries;
    /// invalidates dependent point; blame author of dependent point
    NotFound(Cert<Arc<PointId>>),
}

impl DagPoint {
    pub fn trusted(&self) -> Option<&ValidPoint> {
        match self {
            Self::Trusted(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn certified(&self) -> Option<&ValidPoint> {
        match self {
            Self::Certified(valid) => Some(valid),
            _ => None,
        }
    }

    pub fn valid(&self) -> Option<&ValidPoint> {
        match self {
            Self::Trusted(valid) | Self::Certified(valid) | Self::Suspicious(valid) => Some(valid),
            _ => None,
        }
    }

    fn well_formed(&self) -> Option<&PointInfo> {
        match self {
            Self::Trusted(valid) | Self::Certified(valid) | Self::Suspicious(valid) => {
                Some(&valid.info)
            }
            Self::Invalid(cert) => Some(&cert.inner),
            _ => None,
        }
    }

    pub fn author(&self) -> PeerId {
        match self {
            Self::Trusted(valid) | Self::Certified(valid) | Self::Suspicious(valid) => {
                valid.info.data().author
            }
            Self::Invalid(cert) => cert.inner.data().author,
            Self::IllFormed(id) => id.author,
            Self::NotFound(cert) => cert.inner.author,
        }
    }

    pub fn round(&self) -> Round {
        match self {
            Self::Trusted(valid) | Self::Certified(valid) | Self::Suspicious(valid) => {
                valid.info.round()
            }
            Self::Invalid(cert) => cert.inner.round(),
            Self::IllFormed(id) => id.round,
            Self::NotFound(cert) => cert.inner.round,
        }
    }

    pub fn digest(&self) -> &Digest {
        match self {
            Self::Trusted(valid) | Self::Certified(valid) | Self::Suspicious(valid) => {
                valid.info.digest()
            }
            Self::Invalid(cert) => cert.inner.digest(),
            Self::IllFormed(id) => &id.digest,
            Self::NotFound(cert) => &cert.inner.digest,
        }
    }

    pub fn basic_status(&self) -> PointStatus {
        PointStatus {
            is_ill_formed: self.well_formed().is_none(),
            is_valid: self.valid().is_some(),
            is_trusted: self.trusted().is_some(),
            is_certified: match self {
                Self::Certified(_) => true,
                Self::Invalid(cert) => cert.is_certified,
                Self::NotFound(cert) => cert.is_certified,
                _ => false,
            },
            ..Default::default()
        }
    }
}

pub struct Cert<T> {
    pub inner: T,
    pub is_certified: bool,
}

impl<T> Clone for Cert<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
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
