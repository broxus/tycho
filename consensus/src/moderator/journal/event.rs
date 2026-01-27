use std::fmt::{Display, Formatter};

use tl_proto::TlError;
use tycho_network::PeerId;

use crate::dag::{IllFormedReason, InvalidReason};
use crate::effects::AltFormat;
use crate::intercom::QueryRequestTag;
use crate::models::{
    Digest, IllFormedPoint, InvalidPoint, Point, PointId, PointIntegrityError, PointKey, Round,
};
use crate::moderator::EventTag;
use crate::moderator::journal::item::JournalAction;

pub enum JournalEvent {
    // from Responder
    UnknownQuery(PeerId, TlError),
    BadRequest(PeerId, QueryRequestTag, TlError),
    BadResponse(PeerId, QueryRequestTag, TlError),
    QueryLimitReached(PeerId, QueryRequestTag),
    // from BcastFilter and Downloader
    PointIntegrityError(PeerId, QueryRequestTag, PointIntegrityError),
    // from BcastFilter
    SenderNotAuthor(PeerId, PointId),
    // from Downloader
    ReplacedPoint {
        peer_id: PeerId,
        requested: PointId,
        received: PointId,
    },
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

impl JournalEvent {
    pub fn tag(&self) -> EventTag {
        match self {
            Self::UnknownQuery(_, _) | Self::BadRequest(_, _, _) | Self::BadResponse(_, _, _) => {
                EventTag::BadQuery
            }
            Self::QueryLimitReached(_, _) => EventTag::QueryLimitReached,
            Self::PointIntegrityError(_, _, _) => EventTag::PointIntegrityError,
            Self::SenderNotAuthor(_, _) | Self::ReplacedPoint { .. } => EventTag::ReplacedPoint,
            Self::Equivocated(_, _, _) => EventTag::ForkedPoint,
            Self::EvidenceNoInclusion { .. } => EventTag::EvidenceNoInclusion,
            Self::IllFormed(_) => EventTag::IllFormedPoint,
            Self::Invalid(_) => EventTag::InvalidPoint,
        }
    }

    pub fn peer_id(&self) -> &PeerId {
        #[allow(clippy::match_same_arms, reason = "common order")]
        match self {
            Self::UnknownQuery(peer_id, _)
            | Self::BadRequest(peer_id, _, _)
            | Self::BadResponse(peer_id, _, _)
            | Self::QueryLimitReached(peer_id, _)
            | Self::PointIntegrityError(peer_id, _, _)
            | Self::SenderNotAuthor(peer_id, _)
            | Self::ReplacedPoint { peer_id, .. }
            | Self::Equivocated(peer_id, _, _) => peer_id,
            Self::EvidenceNoInclusion { signer, .. } => signer,
            Self::IllFormed(ill) => &ill.id().author,
            Self::Invalid(invalid) => invalid.info().author(),
        }
    }

    pub fn action(&self) -> JournalAction {
        #[allow(clippy::match_same_arms, reason = "common order")]
        match self {
            Self::UnknownQuery(_, _)
            | Self::BadRequest(_, _, _)
            | Self::BadResponse(_, _, _)
            | Self::QueryLimitReached(_, _)
            | Self::PointIntegrityError(_, _, _)
            | Self::SenderNotAuthor(_, _)
            | Self::ReplacedPoint { .. }
            | Self::Equivocated(_, _, _)
            | Self::EvidenceNoInclusion { .. } => JournalAction::StoreAndCheckBan,
            Self::IllFormed(ill) => match ill.reason() {
                IllFormedReason::AfterLoadFromDb => JournalAction::Ignore, // already (re)stored
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
                | IllFormedReason::AnchorLink(_)
                | IllFormedReason::Structure(_) => JournalAction::StoreAndCheckBan,
            },
            Self::Invalid(invalid) => match invalid.reason() {
                InvalidReason::AfterLoadFromDb { .. } => JournalAction::Ignore, // (re)stored
                InvalidReason::NoRoundInDag(_) | InvalidReason::DependencyRoundDropped => {
                    JournalAction::Ignore
                }
                InvalidReason::TimeNotGreaterThanInPrevPoint(_)
                | InvalidReason::AnchorProofDoesntInheritAnchorTime(_)
                | InvalidReason::AnchorTimeNotInheritedFromProof(_)
                | InvalidReason::MustHaveReferencedPrevPoint(_)
                | InvalidReason::MustHaveSkippedRound(_)
                | InvalidReason::DependencyTimeTooFarInFuture(_)
                | InvalidReason::NewerAnchorInDependency(_)
                | InvalidReason::AnchorLinkBadPath(_)
                | InvalidReason::Dependency(_, _) => JournalAction::StoreAndCheckBan,
            },
        }
    }

    pub fn fill_points<'a>(&'a self, _points: &mut Vec<&'a Point>, point_keys: &mut Vec<PointKey>) {
        #[allow(clippy::match_same_arms, reason = "common order")]
        match self {
            Self::UnknownQuery(_, _)
            | Self::BadRequest(_, _, _)
            | Self::BadResponse(_, _, _)
            | Self::QueryLimitReached(_, _)
            | Self::PointIntegrityError(_, _, _) => {}
            Self::SenderNotAuthor(_, point_id) => {
                point_keys.push(point_id.key());
            }
            Self::ReplacedPoint {
                requested,
                received,
                ..
            } => {
                point_keys.push(requested.key());
                point_keys.push(received.key());
            }
            Self::Equivocated(_, round, digests) => {
                for digest in digests {
                    point_keys.push(PointKey::new(*round, *digest));
                }
            }
            Self::EvidenceNoInclusion { blamed, proofs, .. } => {
                for point_id in blamed.iter().chain(proofs) {
                    point_keys.push(point_id.key());
                }
            }
            Self::IllFormed(ill) => {
                point_keys.push(ill.id().key());
            }
            Self::Invalid(invalid) => match invalid.reason() {
                InvalidReason::AfterLoadFromDb { .. } // point already stored, but we reference it
                // for self-diagnostics only, because this round was successfully commited
                | InvalidReason::NoRoundInDag(_) | InvalidReason::DependencyRoundDropped => {
                    point_keys.push(invalid.info().key());
                }
                InvalidReason::TimeNotGreaterThanInPrevPoint(point_id)
                | InvalidReason::AnchorProofDoesntInheritAnchorTime(point_id)
                | InvalidReason::AnchorTimeNotInheritedFromProof(point_id)
                | InvalidReason::MustHaveReferencedPrevPoint(point_id)
                | InvalidReason::MustHaveSkippedRound(point_id)
                | InvalidReason::DependencyTimeTooFarInFuture(point_id)
                | InvalidReason::NewerAnchorInDependency((_, point_id))
                | InvalidReason::AnchorLinkBadPath((_, point_id)) => {
                    point_keys.push(invalid.info().key());
                    point_keys.push(point_id.key());
                }
                InvalidReason::Dependency(cause, through) => {
                    point_keys.push(invalid.info().key());
                    point_keys.push(cause.point_id().key());
                    if let Some(through) = through {
                        point_keys.push(through.key());
                    }
                }
            },
        }
    }
}

impl Display for JournalEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownQuery(_, tl_err) => {
                write!(f, "unknown query: {tl_err:?}")
            }
            Self::BadRequest(_, tag, tl_err) => {
                write!(f, "{tag:?} request tl error: {tl_err}")
            }
            Self::BadResponse(_, tag, tl_err) => {
                write!(f, "{tag:?} response tl error: {tl_err}")
            }
            Self::QueryLimitReached(_, tag) => {
                write!(f, "{tag:?} queries limit reached")
            }
            Self::PointIntegrityError(_, tag, reason) => {
                write!(f, "{tag:?} bad point: {reason}")
            }
            Self::SenderNotAuthor(_, point_id) => {
                write!(f, "broadcasted other's point: {:?}", point_id.alt())
            }
            Self::ReplacedPoint {
                requested,
                received,
                ..
            } => {
                let rq = requested.alt();
                let rc = received.alt();
                write!(f, "wrong point: requested {rq:?} received {rc:?}")
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
