use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::models::point::proto_utils::{digests_map, signatures_map};
use crate::models::point::{Digest, Round, UnixTime};
use crate::models::{AnchorStageRole, PeerCount, PointMap, Signature, StructureIssue};

#[derive(Clone, Copy, Debug, PartialEq, TlRead, TlWrite)]
#[tl(boxed, id = "consensus.pointId", scheme = "proto.tl")]
pub struct PointId {
    pub author: PeerId,
    pub round: Round,
    pub digest: Digest,
}

impl PointId {
    const MAX_TL_BYTES: usize = 68;
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
pub enum Link {
    #[tl(id = "point.link.to_self")]
    ToSelf,
    #[tl(id = "point.link.direct")]
    Direct(Through),
    #[tl(id = "point.link.indirect")]
    Indirect { to: PointId, path: Through },
}

impl Link {
    pub const MAX_TL_BYTES: usize = 4 + PointId::MAX_TL_BYTES + 4 + 32;
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
        // 4 bytes of PointData tag
        // Max peer count * (32 + 32) of includes
        // Max peer count * (32 + 32) of witness
        // Max peer count * (32 + 64) of evidence
        // 4 + (32 + 32 + 32) + 4 + 32 of MAX possible anchor_trigger Link
        // 4 + (32 + 32 + 32) + 4 + 32 of MAX possible anchor proof Link
        // 8 bytes of time
        // 8 bytes of anchor time

        let max_possible_maps: usize = PeerCount::MAX.full()
            * ((PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES)
                + (PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES)
                + (PeerId::MAX_TL_BYTES + Signature::MAX_TL_BYTES));

        4 + max_possible_maps + 2 * Link::MAX_TL_BYTES + 2 * UnixTime::MAX_TL_BYTES
    };

    /// counterpart of [`crate::dag::Verifier::verify`] that must be called earlier,
    /// does not require config and allows to use [`crate::models::Point`] methods
    pub(super) fn check_maps(&self, author: PeerId, round: Round) -> Result<(), StructureIssue> {
        // proof for previous point consists of digest and 2F++ evidences
        // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
        ((self.evidence.is_empty() == self.includes.contains_key(&author))
            .then_some(PointMap::Includes))
        // evidence must contain only signatures of others
        .or((self.evidence.contains_key(&author)).then_some(PointMap::Evidence))
        // also cannot witness own point
        .or((self.witness.contains_key(&author)).then_some(PointMap::Witness))
        .map(StructureIssue::AuthorInMap)
        .map_or(Ok(()), Err)?;
        for role in [AnchorStageRole::Proof, AnchorStageRole::Trigger] {
            self.link_error(role, round)
                .map(|map| StructureIssue::Link(role, map))
                .map_or(Ok(()), Err)?;
        }
        Ok(())
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
            Link::Indirect {
                path: Through::Includes(peer),
                to,
            } => (!self.includes.contains_key(peer) || to.round.next() >= round)
                .then_some(PointMap::Includes),
            Link::Indirect {
                path: Through::Witness(peer),
                to,
            } => (!self.witness.contains_key(peer) || to.round.next().next() >= round)
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
            Link::Indirect { to, .. } => to.round,
        }
    }

    /// param round - should come from wrapping point
    /// resulting None should be replaced with id of wrapping point
    pub(super) fn anchor_id(&self, link_field: AnchorStageRole, round: Round) -> Option<PointId> {
        match self.anchor_link(link_field) {
            Link::Indirect { to, .. } => Some(*to),
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
            | Link::Indirect {
                path: Through::Includes(peer),
                ..
            } => (&self.includes, *peer, round.prev()),
            Link::Direct(Through::Witness(peer))
            | Link::Indirect {
                path: Through::Witness(peer),
                ..
            } => (&self.witness, *peer, round.prev().prev()),
        };
        Some(PointId {
            author,
            round,
            digest: *map
                .get(&author)
                .expect("Coding error: usage of ill-formed point"),
        })
    }
}
