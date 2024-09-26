use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use tl_proto::{TlRead, TlWrite};
use crate::effects::{AltFmt, AltFormat};
use crate::models::{Point, Signature};

#[derive(Debug, TlWrite, TlRead)]
pub enum PointByIdResponse {
    Defined(Option<Point>),
    TryLater,
}

/// Denotes that broadcasts should be done via network query, not send message.
/// Because initiator must not duplicate its broadcasts, thus should wait for receiver to respond.
pub struct BroadcastResponse;

#[derive(TlWrite, TlRead, Debug)]
pub enum SignatureRejectedReason {
    TooOldRound,
    NoDagRound,
    CannotSign,
}

#[derive(TlWrite, TlRead, Debug)]
#[tl]
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
    /// * signer is more than 1 round in front of us
    /// * signer's clock are too far in the future (probably consensus stalled for long)
    Rejected(SignatureRejectedReason),
}

impl AltFormat for SignatureResponse {}
impl Display for AltFmt<'_, SignatureResponse> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            SignatureResponse::Signature(_) => f.write_str("Signature"),
            SignatureResponse::NoPoint => f.write_str("NoPoint"),
            SignatureResponse::TryLater => f.write_str("TryLater"),
            SignatureResponse::Rejected(reason) => f.debug_tuple("Rejected").field(reason).finish(),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum PeerState {
    /// Not yet ready to connect or already disconnected; always includes local peer id.
    Unknown,
    /// remote peer ready to connect
    Resolved,
}
