use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tycho_network::{Response, ServiceRequest, Version};

use crate::engine::MempoolConfig;
use crate::intercom::dto::{BroadcastResponse, PointByIdResponse, SignatureResponse};
use crate::models::{Point, PointId, Round};

// 65535 bytes is a rough estimate for the largest point with more than 250 validators in set,
// as it contains 2 mappings of 32 (peer_id) to 32 (digest) valuable bytes (includes and witness),
// and 1 mapping of 32 (peer_id) to 64 (signature) valuable bytes (evidence);
// the size of other data is fixed, and estimate is more than enough to handle `Bytes` encoding
const LARGEST_DATA_BYTES: usize = u16::MAX as usize + MempoolConfig::PAYLOAD_BATCH_BYTES;

// broadcast uses simple send_message with () return value
impl From<&Point> for tycho_network::Request {
    fn from(value: &Point) -> Self {
        tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(value).expect("shouldn't happen")),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MPQuery {
    Broadcast(Point),
    PointById(PointId),
    Signature(Round),
}

impl From<&MPQuery> for tycho_network::Request {
    fn from(value: &MPQuery) -> Self {
        tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(value).expect("shouldn't happen")),
        }
    }
}

impl TryFrom<&ServiceRequest> for MPQuery {
    type Error = anyhow::Error;

    fn try_from(request: &ServiceRequest) -> Result<Self, Self::Error> {
        if request.body.len() > LARGEST_DATA_BYTES {
            anyhow::bail!("too large request: {} bytes", request.body.len())
        }
        Ok(bincode::deserialize::<MPQuery>(&request.body)?)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MPResponse {
    Broadcast,
    PointById(PointByIdResponse),
    Signature(SignatureResponse),
}

impl TryFrom<&MPResponse> for Response {
    type Error = anyhow::Error;

    fn try_from(value: &MPResponse) -> Result<Self, Self::Error> {
        let body = Bytes::from(bincode::serialize(value)?);
        Ok(Response {
            version: Version::default(),
            body,
        })
    }
}

impl TryFrom<&Response> for MPResponse {
    type Error = anyhow::Error;

    fn try_from(response: &Response) -> Result<Self, Self::Error> {
        if response.body.len() > LARGEST_DATA_BYTES {
            anyhow::bail!("too large response: {} bytes", response.body.len())
        }
        match bincode::deserialize::<MPResponse>(&response.body) {
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
