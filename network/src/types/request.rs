use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;

use crate::types::{Direction, PeerId};

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u16)]
pub enum Version {
    #[default]
    V1 = 1,
}

impl TryFrom<u16> for Version {
    type Error = anyhow::Error;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            _ => Err(anyhow::anyhow!("invalid version: {value}")),
        }
    }
}

impl Version {
    pub fn to_u16(self) -> u16 {
        self as u16
    }
}

pub struct Request {
    pub version: Version,
    pub body: Bytes,
}

impl Request {
    pub fn from_tl<T>(body: T) -> Self
    where
        T: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        Self {
            version: Default::default(),
            body: tl_proto::serialize(body).into(),
        }
    }
}

impl AsRef<[u8]> for Request {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.body.as_ref()
    }
}

pub struct Response {
    pub version: Version,
    pub body: Bytes,
}

impl Response {
    pub fn from_tl<T>(body: T) -> Self
    where
        T: tl_proto::TlWrite<Repr = tl_proto::Boxed>,
    {
        Self {
            version: Default::default(),
            body: tl_proto::serialize(body).into(),
        }
    }

    pub fn parse_tl<T>(self) -> tl_proto::TlResult<T>
    where
        for<'a> T: tl_proto::TlRead<'a, Repr = tl_proto::Boxed>,
    {
        tl_proto::deserialize(self.body.as_ref())
    }
}

impl AsRef<[u8]> for Response {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.body.as_ref()
    }
}

pub struct ServiceRequest {
    pub metadata: Arc<InboundRequestMeta>,
    pub body: Bytes,
}

impl AsRef<[u8]> for ServiceRequest {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.body.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct InboundRequestMeta {
    pub peer_id: PeerId,
    pub origin: Direction,
    pub remote_address: SocketAddr,
}
