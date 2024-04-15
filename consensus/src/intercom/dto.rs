use serde::{Deserialize, Serialize};

use crate::models::{Point, Signature};

#[derive(Serialize, Deserialize, Debug)]
pub struct PointByIdResponse(pub Option<Point>);

#[derive(Serialize, Deserialize, Debug)]
pub enum BroadcastResponse {
    /// peer will verify and maybe sign the point
    Accepted,
    // TimeOut (disconnect) is a reason to retry also
    /// peer did not reach the point's round yet
    TryLater,
    /// malformed point or peer is on a later round
    Rejected,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum SignatureResponse {
    Signature(Signature),
    /// peer dropped its state or just reached point's round
    NoPoint,
    // TimeOut (still verifying or disconnect) is also a reason to retry
    /// * signer did not reach the point's round yet - lighter weight broadcast retry loop;
    /// * signer still validates the point;
    /// * clock skew: signer's wall time lags the time from point's body
    TryLater,
    /// * malformed point
    /// * equivocation
    /// * invalid dependency
    /// * signer is on a future round
    /// * signer's clock are too far in the future (probably consensus stalled for long)
    Rejected,
}

#[derive(Clone, PartialEq, Debug)]
pub enum PeerState {
    Added,    // not yet ready to connect; always includes local peer id
    Resolved, // remote peer ready to connect
    Removed,  // remote peer will not be added again
}
