use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

use crate::engine::CachedConfig;
use crate::intercom::dto::{PointByIdResponse, SignatureResponse};
use crate::models::{Point, PointId, Round};

#[derive(Debug)]
pub struct BroadcastQuery(pub Point);

impl BroadcastQuery {
    pub(crate) const TL_ID: u32 = tl_proto::id!("core.broadcastQuery", scheme = "proto.tl");
}

impl<'a> TlRead<'a> for BroadcastQuery {
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &'a [u8], offset: &mut usize) -> TlResult<Self> {
        if u32::read_from(packet, offset)? != Self::TL_ID {
            return Err(TlError::UnknownConstructor);
        }

        let size = packet.len();
        if size > CachedConfig::point_max_bytes() + 4usize {
            tracing::error!(size = %size, "Point max size exceeded");
            return Err(TlError::InvalidData);
        }

        if size < 8 {
            tracing::error!(size = %size, "Point does not contain any useful data");
            return Err(TlError::InvalidData);
        }

        // skip 4+4 bytes of BroadcastQuery tag and Point tag
        if !Point::verify_hash_inner(&packet[*offset + 4..]) {
            tracing::error!("Point hash is invalid");
            return Err(TlError::InvalidData);
        }

        Point::read_from(packet, offset).map(Self)
    }
}

impl TlWrite for BroadcastQuery {
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + self.0.max_size_hint()
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: TlPacket,
    {
        packet.write_u32(Self::TL_ID);
        self.0.write_to(packet);
    }
}

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "core.pointQuery", scheme = "proto.tl")]
pub struct PointQuery(pub PointId);

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "core.signatureQuery", scheme = "proto.tl")]
pub struct SignatureQuery(pub Round);

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "core.mpresponse.broadcast", scheme = "proto.tl")]
pub struct BroadcastMpResponse;

#[derive(Debug)]
pub struct PointMpResponse<T>(pub PointByIdResponse<T>);

impl<T> PointMpResponse<T> {
    pub const TL_ID: u32 = tl_proto::id!("core.mpresponse.point", scheme = "proto.tl");
}

impl<T> TlWrite for PointMpResponse<T>
where
    PointByIdResponse<T>: TlWrite,
{
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + self.0.max_size_hint()
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: TlPacket,
    {
        packet.write_u32(Self::TL_ID);
        self.0.write_to(packet);
    }
}

impl<'tl, T> TlRead<'tl> for PointMpResponse<T>
where
    PointByIdResponse<T>: TlRead<'tl>,
{
    type Repr = tl_proto::Boxed;

    fn read_from(packet: &'tl [u8], offset: &mut usize) -> TlResult<Self> {
        if u32::read_from(packet, offset)? != Self::TL_ID {
            return Err(TlError::UnknownConstructor);
        }

        let size = packet.len();
        if size > CachedConfig::point_max_bytes() + 4usize {
            tracing::error!(size = %size, "Point max size exceeded");
            return Err(TlError::InvalidData);
        }

        if packet.len() < *offset + 4usize {
            tracing::error!(size = %size, "PointByIdResponse size is too low");
            return Err(TlError::InvalidData);
        }

        let point_by_id_response_tag = {
            let mut prefix = [0_u8; 4];
            prefix.copy_from_slice(&packet[*offset..*offset + 4usize]);
            u32::from_be_bytes(prefix)
        };

        match point_by_id_response_tag {
            PointByIdResponse::<T>::DEFINED_TL_ID => {
                // skip 4+4 bytes of PointByIdResponse and Point tag prefixes
                if !Point::verify_hash_inner(&packet[8..]) {
                    tracing::error!("Point hash is invalid");
                    return Err(TlError::InvalidData);
                }
            }
            PointByIdResponse::<T>::DEFINED_NONE_TL_ID
            | PointByIdResponse::<T>::TRY_LATER_TL_ID => (),
            _ => {
                tracing::error!(tag = %point_by_id_response_tag, "Unknown PointByIdResponse tag id");
                return Err(TlError::UnknownConstructor);
            }
        }

        PointByIdResponse::<T>::read_from(packet, offset).map(Self)
    }
}

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "core.mpresponse.signature", scheme = "proto.tl")]
pub struct SignatureMpResponse(pub SignatureResponse);
