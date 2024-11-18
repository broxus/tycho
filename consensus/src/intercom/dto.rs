use std::fmt::{Debug, Display, Formatter};

use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

use crate::effects::{AltFmt, AltFormat};
use crate::models::{Point, Signature};

#[derive(Debug, Clone)]
pub enum PointByIdResponse<T> {
    Defined(T),
    DefinedNone,
    TryLater,
}

impl<T> PointByIdResponse<T> {
    pub(crate) const DEFINED_TL_ID: u32 =
        tl_proto::id!("intercom.pointByIdResponse.defined", scheme = "proto.tl");
    pub(crate) const DEFINED_NONE_TL_ID: u32 = tl_proto::id!(
        "intercom.pointByIdResponse.definedNone",
        scheme = "proto.tl"
    );
    pub(crate) const TRY_LATER_TL_ID: u32 =
        tl_proto::id!("intercom.pointByIdResponse.tryLater", scheme = "proto.tl");
}

impl<T: AsRef<[u8]>> TlWrite for PointByIdResponse<T> {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + match self {
            Self::Defined(t) => t.as_ref().len(),
            Self::DefinedNone | Self::TryLater => 0,
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: TlPacket,
    {
        match self {
            Self::Defined(t) => {
                packet.write_u32(Self::DEFINED_TL_ID);
                packet.write_raw_slice(t.as_ref());
            }
            Self::DefinedNone => packet.write_u32(Self::DEFINED_NONE_TL_ID),
            Self::TryLater => packet.write_u32(Self::TRY_LATER_TL_ID),
        }
    }
}

impl TlWrite for PointByIdResponse<Point> {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + match self {
            Self::Defined(t) => t.max_size_hint(),
            Self::DefinedNone | Self::TryLater => 0,
        }
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: TlPacket,
    {
        match self {
            Self::Defined(t) => {
                packet.write_u32(Self::DEFINED_TL_ID);
                t.write_to(packet);
            }
            Self::DefinedNone => packet.write_u32(Self::DEFINED_NONE_TL_ID),
            Self::TryLater => packet.write_u32(Self::TRY_LATER_TL_ID),
        }
    }
}

impl<'a> TlRead<'a> for PointByIdResponse<Point> {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &'a [u8], offset: &mut usize) -> TlResult<Self> {
        let id = u32::read_from(packet, offset)?;
        match id {
            Self::DEFINED_TL_ID => Ok(PointByIdResponse::Defined(Point::read_from(
                packet, offset,
            )?)),
            Self::DEFINED_NONE_TL_ID => Ok(PointByIdResponse::DefinedNone),
            Self::TRY_LATER_TL_ID => Ok(PointByIdResponse::TryLater),
            _ => Err(TlError::InvalidData),
        }
    }
}

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
