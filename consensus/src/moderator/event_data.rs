use std::fmt::{Debug, Display, Formatter};

use tycho_network::PeerId;

use crate::dag::{IllFormedReason, InvalidReason};
use crate::effects::AltFormat;
use crate::models::{
    Digest, IllFormedPoint, InvalidPoint, Point, PointId, PointIntegrityError, Round,
};
use crate::storage::{EventKind, EventSeverity, ShortEventData};

pub enum EventData {
    NodeStarted(PeerId, String),
    // from Responder
    UnknownQuery(PeerId),
    BroadcastLimitReached(PeerId),
    SigRequestLimitReached(PeerId),
    UploadLimitReached(PeerId),
    // from BcastFilter and Downloader
    IllegalPoint(PeerId, PointIntegrityError),
    // from BcastFilter
    SenderNotAuthor(PeerId, Point),
    // from RoundInspector
    Equivocated(PeerId, Round, Vec<Digest>),
    /// proofs contain signatures for their prev points which are skipped in blamed points
    EvidenceNoInclusion {
        signer: PeerId,
        blamed: Vec<PointId>,
        proofs: Vec<PointId>,
    },
    IllFormed(IllFormedPoint),
    Invalid(InvalidPoint),
}

impl EventData {
    pub fn to_short(&self) -> Option<ShortEventData> {
        Some(ShortEventData {
            kind: self.kind(),
            severity: self.severity()?,
            peer_id: *self.peer_id(),
        })
    }

    pub fn kind(&self) -> EventKind {
        match self {
            Self::NodeStarted(_, _) => EventKind::NodeStarted,
            Self::UnknownQuery(_) => EventKind::UnknownQuery,
            Self::BroadcastLimitReached(_) => EventKind::BroadcastLimitReached,
            Self::SigRequestLimitReached(_) => EventKind::SigRequestLimitReached,
            Self::UploadLimitReached(_) => EventKind::UploadLimitReached,
            Self::IllegalPoint(_, _) => EventKind::IllegalPoint,
            Self::SenderNotAuthor(_, _) => EventKind::SenderNotAuthor,
            Self::Equivocated(_, _, _) => EventKind::Equivocated,
            Self::EvidenceNoInclusion { .. } => EventKind::EvidenceNoInclusion,
            Self::IllFormed(_) => EventKind::IllFormed,
            Self::Invalid(_) => EventKind::Invalid,
        }
    }

    pub fn peer_id(&self) -> &PeerId {
        #[allow(clippy::match_same_arms, reason = "common order")]
        match self {
            Self::NodeStarted(peer_id, _)
            | Self::UnknownQuery(peer_id)
            | Self::BroadcastLimitReached(peer_id)
            | Self::SigRequestLimitReached(peer_id)
            | Self::UploadLimitReached(peer_id)
            | Self::IllegalPoint(peer_id, _)
            | Self::SenderNotAuthor(peer_id, _)
            | Self::Equivocated(peer_id, _, _) => peer_id,
            Self::EvidenceNoInclusion { signer, .. } => signer,
            Self::IllFormed(ill) => &ill.id().author,
            Self::Invalid(invalid) => invalid.info().author(),
        }
    }

    pub fn severity(&self) -> Option<EventSeverity> {
        #[allow(clippy::match_same_arms, reason = "common order")]
        match self {
            EventData::NodeStarted(_, _) => Some(EventSeverity::Debug),
            EventData::UnknownQuery(_)
            | EventData::BroadcastLimitReached(_)
            | EventData::SigRequestLimitReached(_)
            | EventData::UploadLimitReached(_)
            | EventData::IllegalPoint(_, _)
            | EventData::SenderNotAuthor(_, _)
            | EventData::Equivocated(_, _, _)
            | EventData::EvidenceNoInclusion { .. } => Some(EventSeverity::Warn),
            EventData::IllFormed(ill) => match ill.reason() {
                IllFormedReason::AfterLoadFromDb => None, // already stored
                IllFormedReason::BeforeGenesis
                | IllFormedReason::TooLargePayload(_)
                | IllFormedReason::LinksAcrossGenesis
                | IllFormedReason::LinksSameRound
                | IllFormedReason::MustBeEmpty(_)
                | IllFormedReason::UnknownPeers(_)
                | IllFormedReason::LackOfPeers(_)
                | IllFormedReason::AnchorTime
                | IllFormedReason::SelfAnchorStage(_)
                | IllFormedReason::SelfLink
                | IllFormedReason::AnchorLink(_) => Some(EventSeverity::Warn),
            },
            EventData::Invalid(invalid) => match invalid.reason() {
                InvalidReason::AfterLoadFromDb => None, // already stored
                InvalidReason::NoRoundInDag(_) | InvalidReason::DependencyRoundDropped => {
                    Some(EventSeverity::Debug)
                }
                InvalidReason::TimeNotGreaterThanInPrevPoint(_)
                | InvalidReason::AnchorProofDoesntInheritAnchorTime(_)
                | InvalidReason::AnchorTimeNotInheritedFromProof(_)
                | InvalidReason::MustHaveReferencedPrevPoint(_)
                | InvalidReason::MustHaveSkippedRound(_)
                | InvalidReason::InvalidDependency(_)
                | InvalidReason::DependencyTimeTooFarInFuture(_)
                | InvalidReason::NewerAnchorInDependency(_)
                | InvalidReason::AnchorLinkBadPath(_) => Some(EventSeverity::Warn),
            },
        }
    }

    pub fn fill_points<'a>(
        &'a self,
        points: &mut Vec<&'a Point>,
        point_keys: &mut Vec<(Round, &'a Digest)>,
    ) {
        match self {
            EventData::NodeStarted(_, _)
            | EventData::UnknownQuery(_)
            | EventData::BroadcastLimitReached(_)
            | EventData::SigRequestLimitReached(_)
            | EventData::UploadLimitReached(_) => {}
            EventData::IllegalPoint(_, integrity_error) => match integrity_error {
                PointIntegrityError::BadHash | PointIntegrityError::BadSig => {}
                PointIntegrityError::EvidenceSig(point) | PointIntegrityError::BadMaps(point) => {
                    points.push(point);
                }
            },
            EventData::SenderNotAuthor(_, point) => {
                points.push(point);
            }
            EventData::Equivocated(_, round, digests) => {
                for digest in digests {
                    point_keys.push((*round, digest));
                }
            }
            EventData::EvidenceNoInclusion { blamed, proofs, .. } => {
                for point_id in blamed.iter().chain(proofs) {
                    point_keys.push((point_id.round, &point_id.digest));
                }
            }
            EventData::IllFormed(ill) => {
                point_keys.push((ill.id().round, &ill.id().digest));
            }
            EventData::Invalid(invalid) => match invalid.reason() {
                InvalidReason::AfterLoadFromDb // point already stored, but we reference it
                // for self-diagnostics only, because this round was successfully commited
                | InvalidReason::NoRoundInDag(_) | InvalidReason::DependencyRoundDropped => {
                    point_keys.push((invalid.info().round(), invalid.info().digest()));
                }
                InvalidReason::TimeNotGreaterThanInPrevPoint(point_id)
                | InvalidReason::AnchorProofDoesntInheritAnchorTime(point_id)
                | InvalidReason::AnchorTimeNotInheritedFromProof(point_id)
                | InvalidReason::MustHaveReferencedPrevPoint(point_id)
                | InvalidReason::MustHaveSkippedRound(point_id)
                | InvalidReason::InvalidDependency(point_id)
                | InvalidReason::DependencyTimeTooFarInFuture(point_id)
                | InvalidReason::NewerAnchorInDependency((_, point_id))
                | InvalidReason::AnchorLinkBadPath((_, point_id)) => {
                    point_keys.push((invalid.info().round(), invalid.info().digest()));
                    point_keys.push((point_id.round, &point_id.digest));
                }
            },
        }
    }
}

impl Debug for EventData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}({})", &self.kind(), self)
    }
}

impl Display for EventData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeStarted(_, msg) => f.write_str(msg),
            Self::UnknownQuery(_) => {
                write!(f, "unknown query")
            }
            Self::BroadcastLimitReached(_) => {
                write!(f, "bcast limit reached")
            }
            Self::SigRequestLimitReached(_) => {
                write!(f, "sig request limit reached")
            }
            Self::UploadLimitReached(_) => {
                write!(f, "upload limit reached")
            }
            Self::IllegalPoint(_, reason) => {
                write!(f, "illegal {reason}")
            }
            Self::SenderNotAuthor(_, point) => {
                write!(f, "sent point of {}", point.info().author().alt())
            }
            Self::Equivocated(_, round, digests) => {
                write!(f, "{} forks at {} round", digests.len(), round.0)
            }
            Self::EvidenceNoInclusion { blamed, proofs, .. } => {
                write!(f, "{} points skipped {} proofs", blamed.len(), proofs.len())
            }
            Self::IllFormed(ill) => {
                write!(f, "ill-formed: {}", ill.reason())
            }
            Self::Invalid(invalid) => {
                write!(f, "invalid: {}", invalid.reason())
            }
        }
    }
}
