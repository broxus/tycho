use std::fmt::Debug;

use serde::Serialize;
use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use super::link::*;
use crate::models::point::proto_utils::{digests_map, signatures_map};
use crate::models::point::{Digest, Round, UnixTime, proto_utils};
use crate::models::{PeerCount, PointId, Signature};

#[derive(Clone, Debug, TlRead, TlWrite, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[tl(boxed, id = "consensus.pointData", scheme = "proto.tl")]
pub struct PointData {
    /// `>= 2F+1` points @ r-1,
    /// signed by author @ r-1 with some additional points just mentioned;
    /// mandatory includes author's own vertex iff proof is given.
    /// Repeatable order on every node is needed for commit; map is used during validation
    #[tl(with = "digests_map")]
    pub includes: FastHashMap<PeerId, Digest>,
    /// `>= 0` points @ r-2, signed by author @ r-1
    /// Repeatable order on every node needed for commit; map is used during validation
    #[tl(with = "digests_map")]
    pub witness: FastHashMap<PeerId, Digest>,
    /// signatures for own point from previous round (if one exists, else empty map):
    /// the node may prove its vertex@r-1 with its point@r+0 only; contains signatures from
    /// `>= 2F` neighbours @ r+0 (inside point @ r+0), order does not matter, author is excluded;
    #[tl(with = "signatures_map")]
    pub evidence: FastHashMap<PeerId, Signature>,
    pub role: PointRole,
    /// local peer time at the moment of point creation, cannot be less than `anchor_time`
    pub time: UnixTime,
    /// time of previous anchor candidate, linked through its proof
    pub anchor_time: UnixTime,
}

/// | property \ role   | regular      | proof      | trigger      | genesis      |
/// |-------------------|--------------|------------|--------------|--------------|
/// | anchor proof      | (in)direct   | self       | prev point   | self         |
/// | anchor trigger    | (in)direct   | (in)direct | self         | self         |
/// | chained an. proof | inapplicable | -3+ rounds | inapplicable | inapplicable |
#[derive(Clone, Debug, TlRead, TlWrite, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[tl(boxed, scheme = "proto.tl")]
pub enum PointRole {
    #[tl(id = "consensus.pointRole.regular")]
    Regular {
        /// last included by author; defines author's last committed anchor
        anchor_proof: AnchorLink,
        /// last included by author; maintains anchor chain linked without explicit DAG traverse
        anchor_trigger: AnchorLink,
    },
    #[tl(id = "consensus.pointRole.proof")]
    AnchorProof {
        /// previous proof for anchor chain
        anchor_proof: IndirectLink,
        /// same as for Regular point
        anchor_trigger: AnchorLink,
    },
    #[tl(id = "consensus.pointRole.trigger")]
    AnchorTrigger,
    #[tl(id = "consensus.pointRole.genesis")]
    Genesis,
}

impl PointRole {
    pub(super) const MAX_BYTE_SIZE: usize = 4 + 2 * AnchorLink::MAX_TL_BYTES;

    pub fn anchor_proof(&self, author: &PeerId) -> AnyLink<'_> {
        (self._anchor_proof()).unwrap_or(AnyLink::Direct(Through::Includes(*author)))
    }

    /// `None` must be replaced for `AnyLink::Direct(Through::Includes(author))`
    fn _anchor_proof(&self) -> Option<AnyLink<'_>> {
        match self {
            Self::Regular { anchor_proof, .. } => match anchor_proof {
                AnchorLink::Direct(through) => Some(AnyLink::Direct(*through)),
                AnchorLink::Indirect(link) => Some(AnyLink::Indirect(link)),
            },
            Self::AnchorProof { .. } | Self::Genesis => Some(AnyLink::ToSelf),
            Self::AnchorTrigger => None,
        }
    }

    pub fn anchor_trigger(&self) -> AnyLink<'_> {
        match self {
            Self::Regular { anchor_trigger, .. } | Self::AnchorProof { anchor_trigger, .. } => {
                match anchor_trigger {
                    AnchorLink::Direct(through) => AnyLink::Direct(*through),
                    AnchorLink::Indirect(link) => AnyLink::Indirect(link),
                }
            }
            Self::AnchorTrigger | Self::Genesis => AnyLink::ToSelf,
        }
    }

    pub fn chained_anchor_proof(&self) -> ChainedProofLink<'_> {
        match self {
            Self::Regular { .. } | Self::AnchorTrigger | Self::Genesis => {
                ChainedProofLink::Inapplicable
            }
            Self::AnchorProof { anchor_proof, .. } => ChainedProofLink::Indirect(anchor_proof),
        }
    }

    fn requires_prev_point(&self) -> bool {
        match self {
            Self::Regular { .. } | Self::Genesis => false,
            Self::AnchorProof { .. } | Self::AnchorTrigger => true,
        }
    }
}

#[derive(Debug, Copy, Clone, thiserror::Error)]
pub enum StructureIssue {
    #[error("{}expected genesis", if *.0 { "" } else { "Un" })]
    ExpectedGenesis(bool),
    #[error("{0:?} map must not contain author")]
    AuthorInMap(PointMap),
    #[error("bad {0:?} link through {1:?} map")]
    Link(AnchorStageRole, PointMap),
    #[error("Too close chained proof at round {}", .0.0)]
    TooCloseChainedProof(Round),
    #[error("bad chained proof through {0:?} map")]
    ChainedProof(PointMap),
    #[error("anchor stage role {0:?}")]
    SelfAnchorStage(AnchorStageRole),
    #[error("anchor time")]
    AnchorTime,
    #[error("must have prev point")]
    NoPrevPoint,
}

#[derive(Debug, Copy, Clone)]
pub enum PointMap {
    Evidence, // r+0
    Includes, // r-1
    Witness,  // r-2
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnchorStageRole {
    Trigger,
    Proof,
}

impl PointData {
    pub(super) const MAX_BYTE_SIZE: usize = {
        let max_possible_maps: usize = PeerCount::MAX.full()
            * ((PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES) // includes map
                + (PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES) // evidence map
                + (PeerId::MAX_TL_BYTES + Signature::MAX_TL_BYTES)) // signatures map
            + 3 * proto_utils::MAP_LEN_BYTES; // maps lengths

        4 + max_possible_maps + PointRole::MAX_BYTE_SIZE + 2 * UnixTime::MAX_TL_BYTES
    };

    pub(super) fn is_proof_link_ok(
        &self,
        is_leader: bool,
        has_prev_point: bool,
        round: Round,
    ) -> bool {
        match &self.role {
            PointRole::Regular { .. } => !{
                is_leader
                    && has_prev_point // optional for Regular and encoded for AnchorProof
                    && (self.anchor_round(AnchorStageRole::Proof, round) < round.prev().prev())
            },
            PointRole::AnchorProof { anchor_proof, .. } => {
                is_leader && (anchor_proof.to.round < round.prev().prev())
            }
            // role encodes links
            PointRole::AnchorTrigger | PointRole::Genesis => true,
        }
    }

    /// counterpart of [`crate::dag::Verifier::verify`] that must be called earlier,
    /// does not require config and allows to use [`crate::models::Point`] methods
    pub(super) fn check_non_genesis_structure(
        &self,
        author: &PeerId,
        round: Round,
    ) -> Result<(), StructureIssue> {
        let has_prev_point = self.includes.contains_key(author);

        if self.role.requires_prev_point() && !has_prev_point {
            return Err(StructureIssue::NoPrevPoint);
        };

        // proof for previous point consists of digest and 2F++ evidences
        // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
        ((self.evidence.is_empty() == has_prev_point).then_some(PointMap::Includes))
            // evidence must contain only signatures of others
            .or((self.evidence.contains_key(author)).then_some(PointMap::Evidence))
            // also cannot witness own point
            .or((self.witness.contains_key(author)).then_some(PointMap::Witness))
            .map(StructureIssue::AuthorInMap)
            .map_or(Ok(()), Err)?;

        match &self.role.chained_anchor_proof() {
            ChainedProofLink::Inapplicable => {}
            ChainedProofLink::Indirect(indirect) => {
                if indirect.to.round >= round.prev().prev() {
                    return Err(StructureIssue::TooCloseChainedProof(indirect.to.round));
                }
                if let Some(map) = self.indirect_link_error(indirect, round) {
                    return Err(StructureIssue::ChainedProof(map));
                }
            }
        }

        for role in [AnchorStageRole::Proof, AnchorStageRole::Trigger] {
            if let Some(map) = match self.anchor_link(role, author) {
                AnyLink::ToSelf => (!has_prev_point).then_some(PointMap::Includes),
                AnyLink::Direct(Through::Includes(peer)) => {
                    (!self.includes.contains_key(&peer)).then_some(PointMap::Includes)
                }
                AnyLink::Direct(Through::Witness(peer)) => {
                    (!self.witness.contains_key(&peer)).then_some(PointMap::Witness)
                }
                AnyLink::Indirect(indirect) => self.indirect_link_error(indirect, round),
            } {
                return Err(StructureIssue::Link(role, map));
            }
            // leader must maintain its chain of proofs,
            // while others must link to previous points (checked at the end of this method)
            if self.evidence.is_empty() && self.anchor_link(role, author) == AnyLink::ToSelf {
                return Err(StructureIssue::SelfAnchorStage(role));
            }
        }

        if self.time <= self.anchor_time {
            // point time must be greater than anchor time
            return Err(StructureIssue::AnchorTime);
        }
        Ok(())
    }

    fn indirect_link_error(&self, indirect: &IndirectLink, round: Round) -> Option<PointMap> {
        let IndirectLink { to, path } = indirect;
        match path {
            Through::Includes(peer) => (!self.includes.contains_key(peer)
                || to.round.next() >= round)
                .then_some(PointMap::Includes),
            Through::Witness(peer) => (!self.witness.contains_key(peer)
                || to.round.next().next() >= round)
                .then_some(PointMap::Witness),
        }
    }

    /// should be disclosed by wrapping point
    pub(super) fn anchor_link(&self, link_field: AnchorStageRole, author: &PeerId) -> AnyLink<'_> {
        match link_field {
            AnchorStageRole::Trigger => self.role.anchor_trigger(),
            AnchorStageRole::Proof => self.role.anchor_proof(author),
        }
    }

    /// param round - should come from wrapping point
    /// resulting None should be replaced with id of wrapping point
    pub(super) fn anchor_round(&self, link_field: AnchorStageRole, round: Round) -> Round {
        match link_field {
            AnchorStageRole::Trigger => match self.role.anchor_trigger() {
                AnyLink::ToSelf => round,
                AnyLink::Direct(Through::Includes(_)) => round.prev(),
                AnyLink::Direct(Through::Witness(_)) => round.prev().prev(),
                AnyLink::Indirect(IndirectLink { to, .. }) => to.round,
            },
            AnchorStageRole::Proof => match self.role._anchor_proof() {
                Some(AnyLink::ToSelf) => round,
                None | Some(AnyLink::Direct(Through::Includes(_))) => round.prev(),
                Some(AnyLink::Direct(Through::Witness(_))) => round.prev().prev(),
                Some(AnyLink::Indirect(IndirectLink { to, .. })) => to.round,
            },
        }
    }

    /// param round - should come from wrapping point
    /// resulting None should be replaced with id of wrapping point
    pub(super) fn anchor_id(
        &self,
        link_field: AnchorStageRole,
        author: &PeerId,
        round: Round,
    ) -> Option<PointId> {
        match self.anchor_link(link_field, author) {
            AnyLink::Indirect(IndirectLink { to, .. }) => Some(*to),
            _direct => self.anchor_link_id(link_field, author, round),
        }
    }

    /// param round - should come from wrapping point
    /// resulting None should be replaced with id of wrapping point
    pub(super) fn anchor_link_id(
        &self,
        link_field: AnchorStageRole,
        author: &PeerId,
        round: Round,
    ) -> Option<PointId> {
        let path = match self.anchor_link(link_field, author) {
            AnyLink::ToSelf => return None,
            AnyLink::Direct(path) => path,
            AnyLink::Indirect(IndirectLink { path, .. }) => *path,
        };
        let point_id = self
            .through_id(&path, round)
            .expect("Coding error: usage of ill-formed point");
        Some(point_id)
    }

    pub(super) fn through_id(&self, through: &Through, round: Round) -> Option<PointId> {
        let (map, author, round) = match through {
            Through::Includes(peer) => (&self.includes, *peer, round.prev()),
            Through::Witness(peer) => (&self.witness, *peer, round.prev().prev()),
        };
        Some(PointId {
            author,
            round,
            digest: *map.get(&author)?,
        })
    }
}
