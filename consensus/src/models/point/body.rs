use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tycho_network::PeerId;

use crate::models::{Digest, PrevPoint, Round, UnixTime};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct PointId {
    pub author: PeerId,
    pub round: Round,
    pub digest: Digest,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PointBody {
    pub author: PeerId,
    pub round: Round, // let it be @ r+0
    pub time: UnixTime,
    pub payload: Vec<Bytes>,
    /// by the same author
    pub proof: Option<PrevPoint>,
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
