use std::collections::BTreeMap;

use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use crate::models::point::{Digest, Round, UnixTime};
use crate::models::proto_utils::points_btree_map;

#[derive(Clone, Copy, TlRead, TlWrite, PartialEq, Debug)]
#[tl(boxed, id = "consensus.pointId", scheme = "proto.tl")]
pub struct PointId {
    pub author: PeerId,
    pub round: Round,
    pub digest: Digest,
}

#[derive(Clone, TlWrite, TlRead, Debug)]
#[cfg_attr(test, derive(PartialEq))]
#[tl(boxed, id = "consensus.pointData", scheme = "proto.tl")]
pub struct PointData {
    pub author: PeerId,
    #[tl(with = "points_btree_map")]
    /// `>= 2F+1` points @ r-1,
    /// signed by author @ r-1 with some additional points just mentioned;
    /// mandatory includes author's own vertex iff proof is given.
    /// Repeatable order on every node is needed for commit; map is used during validation
    pub includes: BTreeMap<PeerId, Digest>,

    #[tl(with = "points_btree_map")]
    /// `>= 0` points @ r-2, signed by author @ r-1
    /// Repeatable order on every node needed for commit; map is used during validation
    pub witness: BTreeMap<PeerId, Digest>,
    /// last included by author; defines author's last committed anchor
    pub anchor_trigger: Link,
    /// last included by author; maintains anchor chain linked without explicit DAG traverse
    pub anchor_proof: Link,
    /// local peer time at the moment of point creation, cannot be less than `anchor_time`
    pub time: UnixTime,
    /// time of previous anchor candidate, linked through its proof
    pub anchor_time: UnixTime,
}

#[derive(TlWrite)]
#[tl(boxed, id = "consensus.pointData", scheme = "proto.tl")]
/// Note: fields and their order must be the same with [`PointData`]
pub struct PointDataRef<'a> {
    author: &'a PeerId,
    #[tl(with = "points_btree_map")]
    includes: &'a BTreeMap<PeerId, Digest>,
    #[tl(with = "points_btree_map")]
    witness: &'a BTreeMap<PeerId, Digest>,
    anchor_trigger: &'a Link,
    anchor_proof: &'a Link,
    time: &'a UnixTime,
    anchor_time: &'a UnixTime,
}

impl<'a> From<&'a PointData> for PointDataRef<'a> {
    fn from(data: &'a PointData) -> Self {
        Self {
            author: &data.author,
            includes: &data.includes,
            witness: &data.witness,
            anchor_trigger: &data.anchor_trigger,
            anchor_proof: &data.anchor_proof,
            time: &data.time,
            anchor_time: &data.anchor_time,
        }
    }
}

#[derive(Clone, TlRead, TlWrite, PartialEq, Debug)]
#[tl(boxed, scheme = "proto.tl")]
pub enum Link {
    #[tl(id = "point.link.to_self")]
    ToSelf,
    #[tl(id = "point.link.direct")]
    Direct(Through),
    #[tl(id = "point.link.indirect")]
    Indirect { to: PointId, path: Through },
}

#[derive(Clone, TlRead, TlWrite, PartialEq, Debug)]
#[tl(boxed, scheme = "proto.tl")]
pub enum Through {
    #[tl(id = "link.through.witness")]
    Witness(PeerId),
    #[tl(id = "link.through.includes")]
    Includes(PeerId),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AnchorStageRole {
    Trigger,
    Proof,
}

impl PointData {
    pub fn prev_digest(&self) -> Option<&Digest> {
        self.includes.get(&self.author)
    }

    /// should be disclosed by wrapping point
    pub(super) fn anchor_link(&self, link_field: AnchorStageRole) -> &'_ Link {
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
