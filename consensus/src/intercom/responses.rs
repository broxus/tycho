use serde::{Deserialize, Serialize};

use crate::models::{Point, RoundId, Signature};

#[derive(Serialize, Deserialize, Debug)]
pub struct BroadcastResponse {
    pub current_round: RoundId,
    // for requested point
    pub signature: Signature,
    // at the same round, if it was not skipped
    pub signer_point: Option<Point>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct PointResponse {
    pub current_round: RoundId,
    pub point: Option<Point>,
}
// PointLast(Option<Point>),
#[derive(Serialize, Deserialize, Debug)]
pub struct VertexResponse {
    pub current_round: RoundId,
    pub vertex: Option<Point>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct EvidenceResponse {
    pub current_round: RoundId,
    pub point: Option<Point>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct VerticesResponse {
    pub vertices: Vec<Point>,
}
