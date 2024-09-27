use std::sync::LazyLock;

use anyhow::anyhow;
use bytes::{BytesMut};
use tl_proto::{TlRead, TlWrite};
use tycho_network::{Response, ServiceRequest, Version};

use crate::engine::MempoolConfig;
use crate::intercom::dto::{BroadcastResponse, PointByIdResponse, SignatureResponse};
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

// broadcast uses simple send_message with () return value
impl From<&Point> for tycho_network::Request {
    fn from(value: &Point) -> Self {
        let mut data = BytesMut::with_capacity(value.max_size_hint());
        value.write_to(&mut data);
        tycho_network::Request {
            version: Version::V1,
            body: data.freeze(),
        }
    }
}

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, scheme = "proto.tl")]
pub enum MPQuery {
    #[tl(id = "core.mpquery.broadcast")]
    Broadcast(Point),
    #[tl(id = "core.mpquery.pointById")]
    PointById(PointId),
    #[tl(id = "core.mpquery.signature")]
    Signature(Round),
}

impl From<&MPQuery> for tycho_network::Request {
    fn from(value: &MPQuery) -> Self {
        let mut data = BytesMut::new();
        value.write_to(&mut data);
        tycho_network::Request {
            version: Version::V1,
            body: data.freeze(),
        }
    }
}

impl TryFrom<&ServiceRequest> for MPQuery {
    type Error = anyhow::Error;

    fn try_from(request: &ServiceRequest) -> Result<Self, Self::Error> {
        if request.body.len() > *LARGEST_DATA_BYTES {
            anyhow::bail!("too large request: {} bytes", request.body.len())
        }
        let data = <MPQuery>::read_from(&request.body, &mut 0)?;
        Ok(data)
    }
}

#[derive(TlWrite, TlRead, Debug)]
#[tl(boxed, scheme="proto.tl")]
pub enum MPResponse {
    #[tl(id = "core.mpresponse.broadcast")]
    Broadcast,
    #[tl(id = "core.mpresponse.pointById")]
    PointById(PointByIdResponse),
    #[tl(id = "core.mpresponse.signature")]
    Signature(SignatureResponse),
}

impl TryFrom<&MPResponse> for Response {
    type Error = anyhow::Error;

    fn try_from(value: &MPResponse) -> Result<Self, Self::Error> {
        let mut data = BytesMut::with_capacity(value.max_size_hint());
        value.write_to(&mut data);
        let body = data.freeze();
        Ok(Response {
            version: Version::default(),
            body,
        })
    }
}

impl TryFrom<&Response> for MPResponse {
    type Error = anyhow::Error;

    fn try_from(response: &Response) -> Result<Self, Self::Error> {
        if response.body.len() > *LARGEST_DATA_BYTES {
            anyhow::bail!("too large response: {} bytes", response.body.len())
        }

        match <MPResponse>::read_from(&response.body, &mut 0) {
            Ok(response) => Ok(response),
            Err(e) => Err(anyhow!("failed to deserialize: {e:?}")),
        }
    }
}

impl TryFrom<MPResponse> for PointByIdResponse {
    type Error = anyhow::Error;

    fn try_from(response: MPResponse) -> Result<Self, Self::Error> {
        match response {
            MPResponse::PointById(response) => Ok(response),
            _ => Err(anyhow!("wrapper mismatch, expected PointById")),
        }
    }
}

impl TryFrom<MPResponse> for SignatureResponse {
    type Error = anyhow::Error;

    fn try_from(response: MPResponse) -> Result<Self, Self::Error> {
        match response {
            MPResponse::Signature(response) => Ok(response),
            _ => Err(anyhow!("wrapper mismatch, expected Signature")),
        }
    }
}

impl TryFrom<MPResponse> for BroadcastResponse {
    type Error = anyhow::Error;

    fn try_from(response: MPResponse) -> Result<Self, Self::Error> {
        match response {
            MPResponse::Broadcast => Ok(BroadcastResponse),
            _ => Err(anyhow!("wrapper mismatch, expected Broadcast")),
        }
    }
}
