use std::fmt::{Display, Formatter};

use tl_proto::TlError;
use tycho_network::PeerId;

use crate::dag::{IllFormedReason, InvalidReason};
use crate::effects::AltFormat;
use crate::intercom::{QueryLimitError, QueryRequestTag};
use crate::models::{
    Digest, IllFormedPoint, InvalidPoint, Point, PointId, PointIntegrityError, Round,
};
use crate::moderator::stored::{EventTag, RecordAction};

pub enum JournalEvent {
    // from Responder
    UnknownQuery(PeerId),
    QueryLimitReached(PeerId, QueryRequestTag, QueryLimitError),
    BadRequest(PeerId, QueryRequestTag, TlError),
    BadResponse(PeerId, QueryRequestTag, TlError),
    // from BcastFilter and Downloader
    BadPoint(PeerId, PointIntegrityError),
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

impl JournalEvent {
    pub fn tag(&self) -> EventTag {
        match self {
            Self::UnknownQuery(_) => EventTag::UnknownQuery,
            Self::QueryLimitReached(_, _, _) => EventTag::QueryLimitReached,
            Self::BadRequest(_, _, _) => EventTag::BadRequest,
            Self::BadResponse(_, _, _) => EventTag::BadResponse,
            Self::BadPoint(_, _) => EventTag::BadPoint,
            Self::SenderNotAuthor(_, _) => EventTag::SenderNotAuthor,
            Self::Equivocated(_, _, _) => EventTag::Equivocated,
            Self::EvidenceNoInclusion { .. } => EventTag::EvidenceNoInclusion,
            Self::IllFormed(_) => EventTag::IllFormed,
            Self::Invalid(_) => EventTag::Invalid,
        }
    }

    pub fn peer_id(&self) -> &PeerId {
        #[allow(clippy::match_same_arms, reason = "common order")]
        match self {
            Self::UnknownQuery(peer_id)
            | Self::QueryLimitReached(peer_id, _, _)
            | Self::BadRequest(peer_id, _, _)
            | Self::BadResponse(peer_id, _, _)
            | Self::BadPoint(peer_id, _)
            | Self::SenderNotAuthor(peer_id, _)
            | Self::Equivocated(peer_id, _, _) => peer_id,
            Self::EvidenceNoInclusion { signer, .. } => signer,
            Self::IllFormed(ill) => &ill.id().author,
            Self::Invalid(invalid) => invalid.info().author(),
        }
    }

    pub fn action(&self) -> RecordAction {
        #[allow(clippy::match_same_arms, reason = "common order")]
        match self {
            Self::UnknownQuery(_)
            | Self::QueryLimitReached(_, _, _)
            | Self::BadRequest(_, _, _)
            | Self::BadResponse(_, _, _)
            | Self::BadPoint(_, _)
            | Self::SenderNotAuthor(_, _)
            | Self::Equivocated(_, _, _)
            | Self::EvidenceNoInclusion { .. } => RecordAction::StoreAndCountPenalty,
            Self::IllFormed(ill) => match ill.reason() {
                IllFormedReason::AfterLoadFromDb => RecordAction::CountPenalty, // already stored
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
                | IllFormedReason::AnchorLink(_) => RecordAction::StoreAndCountPenalty,
            },
            Self::Invalid(invalid) => match invalid.reason() {
                InvalidReason::AfterLoadFromDb => RecordAction::CountPenalty, // already stored
                InvalidReason::NoRoundInDag(_) | InvalidReason::DependencyRoundDropped => {
                    RecordAction::Store
                }
                InvalidReason::TimeNotGreaterThanInPrevPoint(_)
                | InvalidReason::AnchorProofDoesntInheritAnchorTime(_)
                | InvalidReason::AnchorTimeNotInheritedFromProof(_)
                | InvalidReason::MustHaveReferencedPrevPoint(_)
                | InvalidReason::MustHaveSkippedRound(_)
                | InvalidReason::InvalidDependency(_)
                | InvalidReason::DependencyTimeTooFarInFuture(_)
                | InvalidReason::NewerAnchorInDependency(_)
                | InvalidReason::AnchorLinkBadPath(_) => RecordAction::StoreAndCountPenalty,
            },
        }
    }

    pub fn fill_points<'a>(
        &'a self,
        points: &mut Vec<&'a Point>,
        point_keys: &mut Vec<(Round, &'a Digest)>,
    ) {
        match self {
            Self::UnknownQuery(_)
            | Self::QueryLimitReached(_, _, _)
            | Self::BadRequest(_, _, _)
            | Self::BadResponse(_, _, _) => {}
            Self::BadPoint(_, integrity_error) => match integrity_error {
                PointIntegrityError::BadHash | PointIntegrityError::BadSig => {}
                PointIntegrityError::EvidenceSig(point) | PointIntegrityError::BadMaps(point) => {
                    points.push(point);
                }
            },
            Self::SenderNotAuthor(_, point) => {
                points.push(point);
            }
            Self::Equivocated(_, round, digests) => {
                for digest in digests {
                    point_keys.push((*round, digest));
                }
            }
            Self::EvidenceNoInclusion { blamed, proofs, .. } => {
                for point_id in blamed.iter().chain(proofs) {
                    point_keys.push((point_id.round, &point_id.digest));
                }
            }
            Self::IllFormed(ill) => {
                point_keys.push((ill.id().round, &ill.id().digest));
            }
            Self::Invalid(invalid) => match invalid.reason() {
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

impl Display for JournalEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownQuery(_) => {
                write!(f, "unknown query")
            }
            Self::QueryLimitReached(_, tag, error) => {
                write!(f, "{tag:?} queries limit reached: {error:?}")
            }
            Self::BadRequest(_, tag, tl_err) => {
                write!(f, "{tag:?} request tl error: {tl_err}")
            }
            Self::BadResponse(_, tag, tl_err) => {
                write!(f, "{tag:?} response tl error: {tl_err}")
            }
            Self::BadPoint(_, reason) => {
                write!(f, "bad point: {reason}")
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
