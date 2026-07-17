use std::fmt::{Display, Formatter};

use tl_proto::TlError;
use tycho_network::PeerId;

use crate::dag::{IllFormedReason, InvalidReason};
use crate::effects::AltFormat;
use crate::intercom::QueryRequestTag;
use crate::models::{
    Digest, IllFormedPoint, InvalidPoint, Point, PointId, PointIntegrityError, PointKey, Round,
    TransInvalidPoint,
};
use crate::moderator::EventTag;
use crate::moderator::journal::item::JournalAction;

pub enum JournalEvent {
    Network(JournalNetworkEvent),
    Dag(JournalDagEvent),
}

pub enum JournalNetworkEvent {
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
}

pub enum JournalDagEvent {
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
    TransInvalid(TransInvalidPoint),
}

impl From<JournalNetworkEvent> for JournalEvent {
    fn from(event: JournalNetworkEvent) -> Self {
        Self::Network(event)
    }
}

impl From<JournalDagEvent> for JournalEvent {
    fn from(event: JournalDagEvent) -> Self {
        Self::Dag(event)
    }
}

impl JournalEvent {
    pub fn tag(&self) -> EventTag {
        match self {
            Self::Network(event) => event.tag(),
            Self::Dag(event) => event.tag(),
        }
    }

    pub fn peer_id(&self) -> &PeerId {
        match self {
            Self::Network(event) => event.peer_id(),
            Self::Dag(event) => event.peer_id(),
        }
    }

    pub fn action(&self) -> JournalAction {
        match self {
            Self::Network(event) => event.action(),
            Self::Dag(event) => event.action(),
        }
    }

    pub fn fill_points<'a>(&'a self, _points: &mut Vec<&'a Point>, point_keys: &mut Vec<PointKey>) {
        match self {
            Self::Network(event) => event.fill_points(_points, point_keys),
            Self::Dag(event) => event.fill_points(_points, point_keys),
        }
    }
}

impl JournalNetworkEvent {
    pub fn tag(&self) -> EventTag {
        match self {
            Self::UnknownQuery(_, _) | Self::BadRequest(_, _, _) | Self::BadResponse(_, _, _) => {
                EventTag::BadQuery
            }
            Self::QueryLimitReached(_, _) => EventTag::QueryLimitReached,
            Self::PointIntegrityError(_, _, _) => EventTag::PointIntegrityError,
            Self::SenderNotAuthor(_, _) | Self::ReplacedPoint { .. } => EventTag::ReplacedPoint,
        }
    }

    fn peer_id(&self) -> &PeerId {
        match self {
            Self::UnknownQuery(peer_id, _)
            | Self::BadRequest(peer_id, _, _)
            | Self::BadResponse(peer_id, _, _)
            | Self::QueryLimitReached(peer_id, _)
            | Self::PointIntegrityError(peer_id, _, _)
            | Self::SenderNotAuthor(peer_id, _)
            | Self::ReplacedPoint { peer_id, .. } => peer_id,
        }
    }

    fn action(&self) -> JournalAction {
        match self {
            Self::UnknownQuery(_, _)
            | Self::BadRequest(_, _, _)
            | Self::BadResponse(_, _, _)
            | Self::QueryLimitReached(_, _)
            | Self::PointIntegrityError(_, _, _)
            | Self::SenderNotAuthor(_, _)
            | Self::ReplacedPoint { .. } => JournalAction::StoreAndCheckBan,
        }
    }

    fn fill_points<'a>(&'a self, _points: &mut Vec<&'a Point>, point_keys: &mut Vec<PointKey>) {
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
        };
    }
}

impl JournalDagEvent {
    pub fn tag(&self) -> EventTag {
        match self {
            Self::Equivocated(_, _, _) => EventTag::ForkedPoint,
            Self::EvidenceNoInclusion { .. } => EventTag::EvidenceNoInclusion,
            Self::IllFormed(_) => EventTag::IllFormedPoint,
            Self::Invalid(_) | Self::TransInvalid(_) => EventTag::InvalidPoint,
        }
    }

    fn peer_id(&self) -> &PeerId {
        match self {
            Self::Equivocated(peer_id, _, _) => peer_id,
            Self::EvidenceNoInclusion { signer, .. } => signer,
            Self::IllFormed(ill) => &ill.id().author,
            Self::Invalid(invalid) => invalid.info().author(),
            Self::TransInvalid(invalid) => invalid.info().author(),
        }
    }

    fn action(&self) -> JournalAction {
        // Note: there's an inevitable lag between storing point statuses and corresponding events,
        //   so in general a restored status doesn't mean that event is also (re)stored,
        //   but a possibility to lose an event at restart is more preferable than double accounting
        match self {
            Self::Equivocated(_, _, _) | Self::EvidenceNoInclusion { .. } => {
                JournalAction::StoreAndCheckBan
            }
            Self::IllFormed(ill) => match ill.reason() {
                IllFormedReason::AfterLoadFromDb { .. } => JournalAction::Ignore, // (re)stored
                _ => JournalAction::StoreAndCheckBan,
            },
            Self::Invalid(invalid) => match invalid.reason() {
                InvalidReason::AfterLoadFromDb { .. } // already (re)stored
                | InvalidReason::NoRoundInDag(_)
                | InvalidReason::DependencyRoundDropped => JournalAction::Ignore,
                _ => JournalAction::StoreAndCheckBan,
            },
            Self::TransInvalid(invalid) => {
                if invalid.is_restored() || !invalid.root_cause().reason.has_dag_round() {
                    JournalAction::Ignore
                } else {
                    JournalAction::StoreAndCheckBan
                }
            }
        }
    }

    fn fill_points<'a>(&'a self, _points: &mut Vec<&'a Point>, point_keys: &mut Vec<PointKey>) {
        let invalid_reason = match self {
            Self::Equivocated(_, round, digests) => {
                for digest in digests {
                    point_keys.push(PointKey::new(*round, *digest));
                }
                return;
            }
            Self::EvidenceNoInclusion { blamed, proofs, .. } => {
                for point_id in blamed.iter().chain(proofs) {
                    point_keys.push(point_id.key());
                }
                return;
            }
            Self::IllFormed(ill) => {
                point_keys.push(ill.id().key());
                return;
            }
            Self::Invalid(invalid) => {
                point_keys.push(invalid.info().key());
                invalid.reason()
            }
            Self::TransInvalid(invalid) => {
                point_keys.push(invalid.info().key());
                let link = &invalid.root_cause().link;
                if let Some(through) = invalid.info().through_id(&link.path) {
                    point_keys.push(through.key()); // also may panic if not found
                }
                point_keys.push(link.to.key());
                &invalid.root_cause().reason
            }
        };
        match invalid_reason {
            InvalidReason::AfterLoadFromDb { .. } // point already stored, but we reference it
            // for self-diagnostics only, because this round was successfully commited
            | InvalidReason::NoRoundInDag(_) | InvalidReason::DependencyRoundDropped
            | InvalidReason::DepNotFound(_) => {},
            InvalidReason::NotTrigger(point_id)
            | InvalidReason::TimeNotGreaterThanInPrevPoint(point_id)
            | InvalidReason::AnchorProofDoesntInheritAnchorTime(point_id)
            | InvalidReason::AnchorTimeNotInheritedFromProof(point_id)
            | InvalidReason::MustHaveReferencedPrevPoint(point_id)
            | InvalidReason::MustHaveSkippedRound(point_id)
            | InvalidReason::DependencyTimeTooFarInFuture(point_id)
            | InvalidReason::NewerAnchorInDependency((_, point_id))
            | InvalidReason::AnchorLink((_, point_id))
            | InvalidReason::AnchorLinkRole((_, point_id))
            | InvalidReason::AnchorLinkBadPath((_, point_id))
            | InvalidReason::ChainedProofRole(point_id)
            | InvalidReason::ChainedProofBadPath(point_id)
            | InvalidReason::BadStickySequence((point_id, _, _))
            | InvalidReason::NewerProofToChainInDependency(point_id)
            | InvalidReason::TriggerProofMismatch((point_id, _))
            | InvalidReason::DepIllFormed((point_id, _)) => {
                point_keys.push(point_id.key());
            }
        }
    }
    pub fn index(&self) -> JournalDagEventIndex {
        JournalDagEventIndex::new(self)
    }
}

/// Semantically unique and ordered bytes to deduplicate events on mempool restarts
#[derive(Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct JournalDagEventIndex(Box<[u8]>);

impl JournalDagEventIndex {
    fn new(event: &JournalDagEvent) -> Self {
        Self(Self::unique_bytes(event).into_boxed_slice())
    }

    fn unique_bytes(event: &JournalDagEvent) -> Vec<u8> {
        const POINT_ID_WITH_LABEL_BYTES: usize = 4 + 1 + 32 + 32;
        fn write_point_id_with_label(to: &mut Vec<u8>, label: u8, point_id: &PointId) {
            to.extend_from_slice(&point_id.round.0.to_be_bytes());
            to.push(label);
            to.extend_from_slice(point_id.author.as_bytes());
            to.extend_from_slice(point_id.digest.inner());
        }

        match event {
            JournalDagEvent::Equivocated(peer_id, round, digests) => {
                let mut vec = Vec::with_capacity(4 + 1 + 32);
                vec.extend_from_slice(&round.0.to_be_bytes());
                vec.push(1);
                vec.extend_from_slice(peer_id.as_bytes());
                _ = digests; // explicit: reasons make no difference
                vec
            }
            JournalDagEvent::EvidenceNoInclusion {
                signer,
                blamed,
                proofs,
            } => {
                let mut vec = Vec::with_capacity(4 + 1 + 32);
                let min_blamed_round = blamed.iter().map(|id| id.round.0).min().unwrap_or_default();
                vec.extend_from_slice(&min_blamed_round.to_be_bytes());
                vec.push(2);
                vec.extend_from_slice(signer.as_bytes());
                _ = proofs; // explicit: reasons make no difference
                vec
            }
            JournalDagEvent::IllFormed(ill) => {
                let mut vec = Vec::with_capacity(POINT_ID_WITH_LABEL_BYTES);
                write_point_id_with_label(&mut vec, 3, ill.id());
                _ = ill.reason(); // explicit: reasons make no difference
                vec
            }
            JournalDagEvent::Invalid(invalid) => {
                let mut vec = Vec::with_capacity(POINT_ID_WITH_LABEL_BYTES);
                write_point_id_with_label(&mut vec, 4, invalid.info().id());
                _ = invalid.reason(); // explicit: reasons make no difference
                vec
            }
            JournalDagEvent::TransInvalid(invalid) => {
                let mut vec = Vec::with_capacity(POINT_ID_WITH_LABEL_BYTES);
                write_point_id_with_label(&mut vec, 5, invalid.info().id());
                _ = invalid.root_cause(); // explicit: reasons make no difference
                vec
            }
        }
    }
}

impl Display for JournalEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalEvent::Network(event) => event.fmt(f),
            JournalEvent::Dag(event) => event.fmt(f),
        }
    }
}

impl Display for JournalNetworkEvent {
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
        }
    }
}

impl Display for JournalDagEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
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
            Self::TransInvalid(invalid) => {
                write!(f, "transitional {:?}", invalid.root_cause())
            }
        }
    }
}
