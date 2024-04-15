use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use tycho_network::Version;

use crate::intercom::dto::{BroadcastResponse, PointByIdResponse, SignatureResponse};
use crate::models::{Point, PointId, Round};

#[derive(Serialize, Deserialize, Debug)]
pub enum MPRemoteResult {
    Ok(MPResponse),
    Err(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MPRequest {
    PointById(PointId),
    Broadcast(Point),
    Signature(Round),
}

impl From<&MPRequest> for tycho_network::Request {
    // TODO: move MPRequest et al to TL - won't need to copy Point
    fn from(value: &MPRequest) -> Self {
        tycho_network::Request {
            version: Version::V1,
            body: Bytes::from(bincode::serialize(value).expect("shouldn't happen")),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MPResponse {
    PointById(PointByIdResponse),
    Broadcast(BroadcastResponse),
    Signature(SignatureResponse),
}

impl TryFrom<&tycho_network::Response> for MPResponse {
    type Error = anyhow::Error;

    fn try_from(response: &tycho_network::Response) -> Result<Self, Self::Error> {
        match bincode::deserialize::<MPRemoteResult>(&response.body) {
            Ok(MPRemoteResult::Ok(response)) => Ok(response),
            Ok(MPRemoteResult::Err(e)) => Err(anyhow::Error::msg(e)),
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

impl TryFrom<MPResponse> for BroadcastResponse {
    type Error = anyhow::Error;

    fn try_from(response: MPResponse) -> Result<Self, Self::Error> {
        match response {
            MPResponse::Broadcast(response) => Ok(response),
            _ => Err(anyhow!("wrapper mismatch, expected Broadcast")),
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
