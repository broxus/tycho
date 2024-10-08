use std::sync::LazyLock;

use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};
use tycho_network::PeerId;

use crate::engine::MempoolConfig;
use crate::intercom::dto::{PointByIdResponse, SignatureResponse};
use crate::models::{Digest, Link, PeerCount, Point, PointId, Round, Signature, UnixTime};

// rough estimate for the largest point
// as it contains 2 mappings of 32 (peer_id) to 32 (digest) valuable bytes (includes and witness),
// and 1 mapping of 32 (peer_id) to 64 (signature) valuable bytes (evidence);
// the size of other data is fixed, and estimate is more than enough to handle `Bytes` encoding

// 4 bytes of Point tag
// 32 bytes of Digest
// 64 bytes of Signature

// 4 bytes of PointBody tag
// 4 bytes of Round
// payload max_size_hint is calculated separately

// 4 bytes of PointData tag
// 32 bytes of author
// Max peer count * (32 + 32) of includes
// Max peer count * (32 + 32) of witness
// 4 + (32 + 32 + 32) + 4 + 32 of MAX possible anchor_trigger Link
// 4 + (32 + 32 + 32) + 4 + 32 of MAX possible anchor proof Link
// 8 bytes of time
// 8 bytes of anchor time

// Max peer size * (32 + 64) bytes of evidence

static LARGEST_POINT_BODY_BYTES: LazyLock<usize> = LazyLock::new(|| {
    // size of BOC of least possible ExtIn message
    const EXT_IN_BOC_MIN: usize = 48;

    let boc = vec![0_u8; EXT_IN_BOC_MIN];
    let payload = vec![boc; 1 + MempoolConfig::PAYLOAD_BATCH_BYTES / EXT_IN_BOC_MIN];

    let max_possible_includes_witness: usize =
        PeerCount::MAX.full() * (PeerId::MAX_TL_BYTES + Digest::MAX_TL_BYTES);

    let evidence_size: usize =
        PeerCount::MAX.full() * (PeerId::MAX_TL_BYTES + Signature::MAX_TL_BYTES);

    let point_data_size: usize = 4
        + PeerId::MAX_TL_BYTES
        + (2 * max_possible_includes_witness)
        + 2 * Link::MAX_TL_BYTES
        + 2 * UnixTime::MAX_TL_BYTES;

    4 + Round::MAX_TL_SIZE + payload.max_size_hint() + point_data_size + evidence_size
});

static LARGEST_POINT_BYTES: LazyLock<usize> = LazyLock::new(|| {
    4 + Digest::MAX_TL_BYTES + Signature::MAX_TL_BYTES + *LARGEST_POINT_BODY_BYTES
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
        if size - 4usize > *LARGEST_POINT_BYTES {
            tracing::error!(size = %size, "Point max size exceeded");
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
        if size - 4usize > *LARGEST_POINT_BYTES {
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
