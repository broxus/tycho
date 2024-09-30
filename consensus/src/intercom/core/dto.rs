use std::sync::LazyLock;

use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

use crate::engine::MempoolConfig;
use crate::intercom::dto::{PointByIdResponse, SignatureResponse,
};
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



#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "core.mpquery.broadcast", scheme = "proto.tl")]
pub struct BroadcastQuery(pub Point);

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "core.mpquery.point", scheme = "proto.tl")]
pub struct PointQuery(pub PointId);

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "core.mpquery.signature", scheme = "proto.tl")]
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
    PointByIdResponse<T>: TlWrite
{
    type Repr = tl_proto::Boxed;

    fn max_size_hint(&self) -> usize {
        4 + self.0.max_size_hint()
    }

    fn write_to<P>(&self, packet: &mut P)
    where
        P: TlPacket
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
        PointByIdResponse::<T>::read_from(packet, offset).map(Self)
    }
}

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, id = "core.mpresponse.point", scheme = "proto.tl")]
pub struct SignatureMpResponse(pub SignatureResponse);

