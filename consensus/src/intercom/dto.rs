use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::models::{Point, Signature};

#[derive(Debug)]
pub struct PointByIdResponse(pub Option<Arc<Point>>);
impl Serialize for PointByIdResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_deref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PointByIdResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<Point>::deserialize(deserializer)?;
        Ok(PointByIdResponse(opt.map(|point| Arc::new(point))))
    }
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

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum PeerState {
    /// Not yet ready to connect or already disconnected; always includes local peer id.
    Unknown,
    /// remote peer ready to connect
    Resolved,
}
