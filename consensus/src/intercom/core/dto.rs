use std::sync::LazyLock;

use blake3::Hash;
use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

use crate::engine::MempoolConfig;
use crate::intercom::dto::{PointByIdResponse, SignatureResponse};
use crate::models::{Point, PointId, Round};

// 65535 bytes is a rough estimate for the largest point with more than 250 validators in set,
// as it contains 2 mappings of 32 (peer_id) to 32 (digest) valuable bytes (includes and witness),
// and 1 mapping of 32 (peer_id) to 64 (signature) valuable bytes (evidence);
// the size of other data is fixed, and estimate is more than enough to handle `Bytes` encoding
static LARGEST_DATA_BYTES: LazyLock<usize> = LazyLock::new(|| {
    // size of BOC of least possible ExtIn message
    const EXT_IN_BOC_MIN: usize = 48;

    let boc = vec![0_u8; EXT_IN_BOC_MIN];
    let payload = vec![boc; 1 + MempoolConfig::PAYLOAD_BATCH_BYTES / EXT_IN_BOC_MIN];
    u16::MAX as usize + payload.max_size_hint()
});

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
        if size - 4usize > *LARGEST_DATA_BYTES {
            tracing::error!(size = %size, "Point max size exceeded");
            return Err(TlError::InvalidData);
        }

        // skip 4+4 bytes of BroadcastQuery tag and Point tag
        if !verify_hash(&packet[8..]) {
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
        if size - 4usize > *LARGEST_DATA_BYTES {
            tracing::error!(size = %size, "Point max size exceeded");
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
                if !verify_hash(&packet[8..]) {
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

pub fn verify_hash(data: &[u8]) -> bool {
    if data.len() < 32 + 64 {
        tracing::error!(len = %data.len(), "Data is too short");
        return false;
    }
    let hash_slice = &data[0..32];
    // skip 64 bytes of signature
    let point_bytes = &data[96..];
    let mut present_hash_bytes = [0_u8; 32];
    present_hash_bytes.copy_from_slice(hash_slice);
    Hash::from_bytes(present_hash_bytes) == blake3::hash(point_bytes)
}

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "core.mpresponse.signature", scheme = "proto.tl")]
pub struct SignatureMpResponse(pub SignatureResponse);
