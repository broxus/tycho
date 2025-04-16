use std::fmt::{Debug, Display, Formatter};

use tl_proto::{TlRead, TlWrite};

use crate::effects::{AltFmt, AltFormat};
use crate::models::Signature;

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
pub enum PointByIdResponse<T> {
    #[tl(id = "intercom.pointByIdResponse.defined")]
    Defined(T),
    #[tl(id = "intercom.pointByIdResponse.definedNone")]
    DefinedNone,
    #[tl(id = "intercom.pointByIdResponse.tryLater")]
    TryLater,
}

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "intercom.broadcastResponse", scheme = "proto.tl")]
pub struct BroadcastResponse;

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, scheme = "proto.tl")]
pub enum SignatureRejectedReason {
    #[tl(id = "intercom.signatureRejectedReason.tooOldRound")]
    TooOldRound,
    #[tl(id = "intercom.signatureRejectedReason.cannotSign")]
    CannotSign,
    #[tl(id = "intercom.signatureRejectedReason.unknownPeer")]
    UnknownPeer,
}

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, scheme = "proto.tl")]
pub enum SignatureResponse {
    #[tl(id = "intercom.signatureResponse.signature")]
    Signature(Signature),
    #[tl(id = "intercom.signatureResponse.noPoint")]
    /// peer dropped its state or just reached point's round
    NoPoint,
    // TimeOut (still verifying or disconnect) is also a reason to retry
    #[tl(id = "intercom.signatureResponse.tryLater")]
    /// * signer did not reach the point's round yet - lighter weight broadcast retry loop;
    /// * signer still validates the point;
    /// * clock skew: signer's wall time lags the time from point's body
    TryLater,

    #[tl(id = "intercom.signatureResponse.rejected")]
    /// * malformed point
    /// * equivocation
    /// * invalid dependency
    /// * signer is more than 1 round in front of us
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

impl<T> AltFormat for PointByIdResponse<T> {}
impl<T> Display for AltFmt<'_, PointByIdResponse<T>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            PointByIdResponse::Defined(_) => f.write_str("Some"),
            PointByIdResponse::DefinedNone => f.write_str("None"),
            PointByIdResponse::TryLater => f.write_str("TryLater"),
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
