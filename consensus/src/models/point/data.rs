use std::collections::BTreeMap;

use tl_proto::{TlRead, TlWrite};
use tycho_network::PeerId;

use crate::engine::MempoolConfig;
use crate::models::point::{Digest, Round, UnixTime};
use crate::models::proto_utils::points_btree_map;
use crate::models::{PeerCount, Signature};

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
pub struct PointDataWrite<'a> {
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

impl<'a> From<&'a PointData> for PointDataWrite<'a> {
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AnchorStageRole {
    Trigger,
    Proof,
}

impl PointData {
    pub(super) const MAX_BYTE_SIZE: usize = {
        // 4 bytes of PointData tag
        // 32 bytes of author
        // Max peer count * (32 + 32) of includes
        // Max peer count * (32 + 32) of witness
        // 4 + (32 + 32 + 32) + 4 + 32 of MAX possible anchor_trigger Link
        // 4 + (32 + 32 + 32) + 4 + 32 of MAX possible anchor proof Link
        // 8 bytes of time
        // 8 bytes of anchor time

        let max_possible_includes_witness: usize =
            PeerCount::MAX.full() * (PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES);

        4 + PeerId::MAX_TL_BYTES
            + (2 * max_possible_includes_witness)
            + 2 * Link::MAX_TL_BYTES
            + 2 * UnixTime::MAX_TL_BYTES
    };

    pub fn prev_digest(&self) -> Option<&Digest> {
        self.includes.get(&self.author)
    }

    pub fn is_well_formed(
        &self,
        self_round: Round,
        payload_len: u32,
        evidence: &BTreeMap<PeerId, Signature>,
        conf: &MempoolConfig,
    ) -> bool {
        // check for being earlier than genesis takes place with other peer checks
        #[allow(clippy::nonminimal_bool, reason = "independent logical checks")]
        let is_special_ok = if self_round == conf.genesis_round {
            payload_len == 0
                && self.anchor_trigger == Link::ToSelf
                && self.anchor_proof == Link::ToSelf
                && self.time == self.anchor_time
        } else {
            // leader must maintain its chain of proofs,
            // while others must link to previous points (checked at the end of this method);
            // its decided later (using dag round data) whether current point belongs to leader
            !(self.anchor_proof == Link::ToSelf && evidence.is_empty())
                && !(self.anchor_trigger == Link::ToSelf && evidence.is_empty())
                && self.time > self.anchor_time
        };
        is_special_ok
            // proof for previous point consists of digest and 2F++ evidences
            // proof is listed in includes - to count for 2/3+1, verify and commit dependencies
            && evidence.is_empty() != self.includes.contains_key(&self.author)
            // evidence must contain only signatures of others
            && !evidence.contains_key(&self.author)
            // also cannot witness own point
            && !self.witness.contains_key(&self.author)
            && self.is_link_well_formed(self_round, AnchorStageRole::Trigger)
            && self.is_link_well_formed(self_round, AnchorStageRole::Proof)
    }

    fn is_link_well_formed(&self, self_round: Round, link_field: AnchorStageRole) -> bool {
        match self.anchor_link(link_field) {
            Link::ToSelf => true,
            Link::Direct(Through::Includes(peer)) => self.includes.contains_key(peer),
            Link::Direct(Through::Witness(peer)) => self.witness.contains_key(peer),
            Link::Indirect {
                path: Through::Includes(peer),
                to,
            } => self.includes.contains_key(peer) && to.round.next() < self_round,
            Link::Indirect {
                path: Through::Witness(peer),
                to,
            } => self.witness.contains_key(peer) && to.round.next().next() < self_round,
        }
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
