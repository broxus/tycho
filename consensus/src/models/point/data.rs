use std::fmt::Debug;

use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use super::link::*;
use crate::models::point::proto_utils::{digests_map, signatures_map};
use crate::models::point::{Digest, Round, UnixTime, proto_utils};
use crate::models::{AnchorStageRole, PeerCount, PointKey, PointMap, Signature, StructureIssue};

#[derive(Clone, Copy, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, id = "consensus.pointId", scheme = "proto.tl")]
pub struct PointId {
    pub round: Round,
    pub digest: Digest,
    pub author: PeerId,
}

impl PointId {
    pub const MAX_TL_BYTES: usize =
        4 + PeerId::MAX_TL_BYTES + Round::MAX_TL_SIZE + Digest::MAX_TL_BYTES;

    pub fn key(&self) -> PointKey {
        PointKey::new(self.round, self.digest)
    }
}

#[derive(Clone, Debug, TlWrite, TlRead)]
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
    /// every anchor proof has a link to previous anchor proof
    pub chained_anchor_proof: ChainedAnchorProof,
    /// last included by author; defines author's last committed anchor
    pub anchor_trigger: AnchorLink,
    /// last included by author; maintains anchor chain linked without explicit DAG traverse
    pub anchor_proof: AnchorLink,
    /// local peer time at the moment of point creation, cannot be less than `anchor_time`
    pub time: UnixTime,
    /// time of previous anchor candidate, linked through its proof
    pub anchor_time: UnixTime,
}

impl PointData {
    pub(super) const MAX_BYTE_SIZE: usize = {
        let max_possible_maps: usize = PeerCount::MAX.full()
            * ((PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES) // includes map
                + (PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES) // evidence map
                + (PeerId::MAX_TL_BYTES + Signature::MAX_TL_BYTES)) // signatures map
            + 3 * proto_utils::MAP_LEN_BYTES; // maps lengths

        4 + max_possible_maps
            + ChainedAnchorProof::MAX_TL_BYTES
            + 2 * AnchorLink::MAX_TL_BYTES
            + 2 * UnixTime::MAX_TL_BYTES
    };

    pub(super) fn check_genesis_except_maps(&self) -> Result<(), StructureIssue> {
        if matches!(self.chained_anchor_proof, ChainedAnchorProof::Chained(_)) {
            return Err(StructureIssue::ChainedProofMustUse(false));
        }
        if self.anchor_trigger != AnchorLink::ToSelf {
            return Err(StructureIssue::SelfAnchorStage(AnchorStageRole::Trigger));
        }
        // evidence map is required to be empty during other peer sets checks
        if self.anchor_proof != AnchorLink::ToSelf {
            return Err(StructureIssue::SelfAnchorStage(AnchorStageRole::Proof));
        }
        if self.time != self.anchor_time {
            return Err(StructureIssue::AnchorTime);
        }
        Ok(())
    }

    /// counterpart of [`crate::dag::Verifier::verify`] that must be called earlier,
    /// does not require config and allows to use [`crate::models::Point`] methods
    pub(super) fn check_regular_structure(
        &self,
        author: &PeerId,
        round: Round,
    ) -> Result<(), StructureIssue> {
        // proof for previous point consists of digest and 2F++ evidences
        // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
        ((self.evidence.is_empty() == self.includes.contains_key(author))
            .then_some(PointMap::Includes))
        // evidence must contain only signatures of others
        .or((self.evidence.contains_key(author)).then_some(PointMap::Evidence))
        // also cannot witness own point
        .or((self.witness.contains_key(author)).then_some(PointMap::Witness))
        .map(StructureIssue::AuthorInMap)
        .map_or(Ok(()), Err)?;

        let must_use_chained_proof = self.anchor_proof == AnchorLink::ToSelf;
        if matches!(self.chained_anchor_proof, ChainedAnchorProof::Chained(_))
            != must_use_chained_proof
        {
            return Err(StructureIssue::ChainedProofMustUse(must_use_chained_proof));
        }
        match &self.chained_anchor_proof {
            ChainedAnchorProof::Inapplicable => {}
            ChainedAnchorProof::Chained(indirect) => {
                if let Some(map) = self.indirect_link_error(indirect, round) {
                    return Err(StructureIssue::ChainedProof(map));
                }
            }
        }

        for role in [AnchorStageRole::Proof, AnchorStageRole::Trigger] {
            if let Some(map) = self.link_error(role, author, round) {
                return Err(StructureIssue::Link(role, map));
            }
            // leader must maintain its chain of proofs,
            // while others must link to previous points (checked at the end of this method)
            if self.evidence.is_empty() && self.anchor_link(role) == &AnchorLink::ToSelf {
                return Err(StructureIssue::SelfAnchorStage(role));
            }
        }

        if self.time <= self.anchor_time {
            // point time must be greater than anchor time
            return Err(StructureIssue::AnchorTime);
        }
        Ok(())
    }

    fn link_error(
        &self,
        link_field: AnchorStageRole,
        author: &PeerId,
        round: Round,
    ) -> Option<PointMap> {
        match self.anchor_link(link_field) {
            AnchorLink::ToSelf => {
                (!self.includes.contains_key(author)).then_some(PointMap::Includes)
            }
            AnchorLink::Direct(Through::Includes(peer)) => {
                (!self.includes.contains_key(peer)).then_some(PointMap::Includes)
            }
            AnchorLink::Direct(Through::Witness(peer)) => {
                (!self.witness.contains_key(peer)).then_some(PointMap::Witness)
            }
            AnchorLink::Indirect(indirect) => self.indirect_link_error(indirect, round),
        }
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
    pub(super) fn anchor_link(&self, link_field: AnchorStageRole) -> &AnchorLink {
        match link_field {
            AnchorStageRole::Trigger => &self.anchor_trigger,
            AnchorStageRole::Proof => &self.anchor_proof,
        }
    }

    /// param round - should come from wrapping point
    /// resulting None should be replaced with id of wrapping point
    pub(super) fn anchor_round(&self, link_field: AnchorStageRole, round: Round) -> Round {
        match self.anchor_link(link_field) {
            AnchorLink::ToSelf => round,
            AnchorLink::Direct(Through::Includes(_)) => round.prev(),
            AnchorLink::Direct(Through::Witness(_)) => round.prev().prev(),
            AnchorLink::Indirect(IndirectLink { to, .. }) => to.round,
        }
    }

    /// param round - should come from wrapping point
    /// resulting None should be replaced with id of wrapping point
    pub(super) fn anchor_id(&self, link_field: AnchorStageRole, round: Round) -> Option<PointId> {
        match self.anchor_link(link_field) {
            AnchorLink::Indirect(IndirectLink { to, .. }) => Some(*to),
            _direct => self.anchor_link_id(link_field, round),
        }
    }

    /// param round - should come from wrapping point
    /// resulting None should be replaced with id of wrapping point
    pub(super) fn anchor_link_id(
        &self,
        link_field: AnchorStageRole,
        round: Round,
    ) -> Option<PointId> {
        let path = match self.anchor_link(link_field) {
            AnchorLink::ToSelf => return None,
            AnchorLink::Direct(path) | AnchorLink::Indirect(IndirectLink { path, .. }) => path,
        };
        let point_id = self
            .through_id(path, round)
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

#[cfg(any(test, feature = "test"))]
impl PointId {
    pub fn random() -> Self {
        Self {
            author: PeerId(rand::random()),
            round: Round(rand::random()),
            digest: Digest::random(),
        }
    }
}
