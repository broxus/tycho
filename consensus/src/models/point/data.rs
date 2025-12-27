use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::models::point::proto_utils::{digests_map, signatures_map};
use crate::models::point::{Digest, Round, UnixTime, proto_utils};
use crate::models::{AnchorStageRole, PeerCount, PointKey, PointMap, Signature, StructureIssue};

#[derive(Clone, Copy, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, id = "consensus.pointId", scheme = "proto.tl")]
pub struct PointId {
    pub author: PeerId,
    pub round: Round,
    pub digest: Digest,
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
    /// every anchor proof has a link to previous anchor proof; otherwise i
    pub chained_anchor_proof: ChainedAnchorProof,
    /// last included by author; defines author's last committed anchor
    pub anchor_trigger: Link,
    /// last included by author; maintains anchor chain linked without explicit DAG traverse
    pub anchor_proof: Link,
    /// local peer time at the moment of point creation, cannot be less than `anchor_time`
    pub time: UnixTime,
    /// time of previous anchor candidate, linked through its proof
    pub anchor_time: UnixTime,
}

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum ChainedAnchorProof {
    #[tl(id = "point.chainedAnchorProof.inapplicable")]
    Inapplicable,
    #[tl(id = "point.chainedAnchorProof.chained")]
    Chained(IndirectLink),
}

impl ChainedAnchorProof {
    pub const MAX_TL_BYTES: usize = 4 + IndirectLink::MAX_TL_BYTES;
}

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, id = "consensus.indirectLink", scheme = "proto.tl")]
pub struct IndirectLink {
    pub to: PointId,
    pub path: Through,
}

impl IndirectLink {
    pub const MAX_TL_BYTES: usize = 4 + PointId::MAX_TL_BYTES + 4 + PeerId::MAX_TL_BYTES;
}

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum Link {
    #[tl(id = "point.link.to_self")]
    ToSelf,
    #[tl(id = "point.link.direct")]
    Direct(Through),
    #[tl(id = "point.link.indirect")]
    Indirect(IndirectLink),
}

impl Link {
    pub const MAX_TL_BYTES: usize = 4 + IndirectLink::MAX_TL_BYTES;
}

#[derive(Clone, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum Through {
    #[tl(id = "link.through.witness")]
    Witness(PeerId),
    #[tl(id = "link.through.includes")]
    Includes(PeerId),
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
            + 2 * Link::MAX_TL_BYTES
            + 2 * UnixTime::MAX_TL_BYTES
    };

    /// counterpart of [`crate::dag::Verifier::verify`] that must be called earlier,
    /// does not require config and allows to use [`crate::models::Point`] methods
    pub(super) fn check_maps(&self, author: &PeerId, round: Round) -> Result<(), StructureIssue> {
        // proof for previous point consists of digest and 2F++ evidences
        // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
        ((self.evidence.is_empty() == self.includes.contains_key(author))
            .then_some(PointMap::Includes))
        // evidence must contain only signatures of others
        .or((self.evidence.contains_key(author)).then_some(PointMap::Evidence))
        // also cannot witness own point
        .or((self.witness.contains_key(author)).then_some(PointMap::Witness))
        .map(StructureIssue::AuthorInMap)
        .or(self.chained_proof_error(round))
        .map_or(Ok(()), Err)?;
        for role in [AnchorStageRole::Proof, AnchorStageRole::Trigger] {
            self.link_error(role, round)
                .map(|map| StructureIssue::Link(role, map))
                .map_or(Ok(()), Err)?;
        }
        Ok(())
    }

    fn chained_proof_error(&self, round: Round) -> Option<StructureIssue> {
        match &self.chained_anchor_proof {
            ChainedAnchorProof::Inapplicable => None,
            ChainedAnchorProof::Chained(indirect) => {
                (self.indirect_link_error(indirect, round)).map(StructureIssue::ChainedProof)
            }
        }
    }

    fn link_error(&self, link_field: AnchorStageRole, round: Round) -> Option<PointMap> {
        match self.anchor_link(link_field) {
            Link::ToSelf => None,
            Link::Direct(Through::Includes(peer)) => {
                (!self.includes.contains_key(peer)).then_some(PointMap::Includes)
            }
            Link::Direct(Through::Witness(peer)) => {
                (!self.witness.contains_key(peer)).then_some(PointMap::Witness)
            }
            Link::Indirect(indirect) => self.indirect_link_error(indirect, round),
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
    pub(super) fn anchor_link(&self, link_field: AnchorStageRole) -> &Link {
        match link_field {
            AnchorStageRole::Trigger => &self.anchor_trigger,
            AnchorStageRole::Proof => &self.anchor_proof,
        }
    }

    /// param round - should come from wrapping point
    /// resulting None should be replaced with id of wrapping point
    pub(super) fn anchor_round(&self, link_field: AnchorStageRole, round: Round) -> Round {
        match self.anchor_link(link_field) {
            Link::ToSelf => round,
            Link::Direct(Through::Includes(_)) => round.prev(),
            Link::Direct(Through::Witness(_)) => round.prev().prev(),
            Link::Indirect(IndirectLink { to, .. }) => to.round,
        }
    }

    /// param round - should come from wrapping point
    /// resulting None should be replaced with id of wrapping point
    pub(super) fn anchor_id(&self, link_field: AnchorStageRole, round: Round) -> Option<PointId> {
        match self.anchor_link(link_field) {
            Link::Indirect(IndirectLink { to, .. }) => Some(*to),
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
        let (map, author, round) = match self.anchor_link(link_field) {
            Link::ToSelf => return None,
            Link::Direct(Through::Includes(peer))
            | Link::Indirect(IndirectLink {
                path: Through::Includes(peer),
                ..
            }) => (&self.includes, *peer, round.prev()),
            Link::Direct(Through::Witness(peer))
            | Link::Indirect(IndirectLink {
                path: Through::Witness(peer),
                ..
            }) => (&self.witness, *peer, round.prev().prev()),
        };
        Some(PointId {
            author,
            round,
            digest: *map
                .get(&author)
                .expect("Coding error: usage of ill-formed point"),
        })
    }

    pub(super) fn indirect_link_through(&self, indirect: &IndirectLink, round: Round) -> PointId {
        let (map, author, round) = match &indirect.path {
            Through::Includes(peer) => (&self.includes, *peer, round.prev()),
            Through::Witness(peer) => (&self.witness, *peer, round.prev().prev()),
        };
        PointId {
            author,
            round,
            digest: *map
                .get(&author)
                .expect("Coding error: usage of ill-formed point"),
        }
    }
}
