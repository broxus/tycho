use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tycho_util::FastHashMap;

pub const POINT_DIGEST_SIZE: usize = 32;
pub const SIGNATURE_SIZE: usize = 64;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Digest(pub Bytes);
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Signature(pub Bytes);
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(pub u8);
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RoundId(pub u32);

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Location {
    round: RoundId,
    author: NodeId,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PointId {
    location: Location,
    digest: Digest,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PrevPoint {
    round: RoundId,
    digest: Digest,
    // >= 2F witnesses, point author excluded
    evidence: FastHashMap<NodeId, Signature>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PointData {
    location: Location,
    local_time: u64,
    payload: Vec<Bytes>,
    // >= 2F+1 vertices from the round before last,
    // optionally including author's own vertex
    includes: FastHashMap<NodeId, Digest>,
    anchor: PointId,
    proposed_leader: Option<PointId>,
    // any vertices the leader adds to its diff-graph
    // beyond its direct inclusions
    leader_deep_includes: Vec<PointId>,
    // of the same author
    prev_point: Option<PrevPoint>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Point {
    data: PointData,
    // author's
    signature: Signature,
    // of both data and author's signature
    digest: Digest,
}
