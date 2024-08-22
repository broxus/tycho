use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tycho_network::PeerId;

use crate::models::{Digest, Round, UnixTime};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct PointId {
    pub author: PeerId,
    pub round: Round,
    pub digest: Digest,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct PointData {
    pub author: PeerId,
    pub time: UnixTime,
    // until weak links are supported,
    /// any node may proof its vertex@r-1 with its point@r+0 only;
    /// `evidence` is kept in a wrapping struct
    pub prev_digest: Option<Digest>,
    /// `>= 2F+1` points @ r-1,
    /// signed by author @ r-1 with some additional points just mentioned;
    /// mandatory includes author's own vertex iff proof is given.
    /// Repeatable order on every node is needed for commit; map is used during validation
    pub includes: BTreeMap<PeerId, Digest>,
    /// `>= 0` points @ r-2, signed by author @ r-1
    /// Repeatable order on every node needed for commit; map is used during validation
    pub witness: BTreeMap<PeerId, Digest>,
    /// last included by author; defines author's last committed anchor
    pub anchor_trigger: Link,
    /// last included by author; maintains anchor chain linked without explicit DAG traverse
    pub anchor_proof: Link,
    /// time of previous anchor candidate, linked through its proof
    pub anchor_time: UnixTime,
}

#[derive(Serialize)]
/// Note: fields and their order must be the same with [`PointData`]
pub(crate) struct PointDataRef<'a> {
    author: &'a PeerId,
    time: &'a UnixTime,
    prev_digest: Option<&'a Digest>,
    includes: &'a BTreeMap<PeerId, Digest>,
    witness: &'a BTreeMap<PeerId, Digest>,
    anchor_trigger: &'a Link,
    anchor_proof: &'a Link,
    anchor_time: &'a UnixTime,
}

impl<'a> From<&'a PointData> for PointDataRef<'a> {
    fn from(data: &'a PointData) -> Self {
        Self {
            author: &data.author,
            time: &data.time,
            prev_digest: data.prev_digest.as_ref(),
            includes: &data.includes,
            witness: &data.witness,
            anchor_trigger: &data.anchor_trigger,
            anchor_proof: &data.anchor_proof,
            anchor_time: &data.anchor_time,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum Link {
    ToSelf,
    Direct(Through),
    Indirect { to: PointId, path: Through },
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum Through {
    Witness(PeerId),
    Includes(PeerId),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AnchorStageRole {
    Trigger,
    Proof,
}

impl PointData {
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
            Link::Indirect { to, .. } => Some(to.clone()),
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
            digest: map
                .get(&author)
                .expect("Coding error: usage of ill-formed point")
                .clone(),
        })
    }
}
