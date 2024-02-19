use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::PeerId;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u16)]
pub enum Version {
    #[default]
    V1 = 1,
}

impl Version {
    pub fn to_u16(self) -> u16 {
        self as u16
    }
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

impl Serialize for Version {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u16(self.to_u16())
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        u16::deserialize(deserializer).and_then(|v| Self::try_from(v).map_err(Error::custom))
    }
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub version: Version,
    #[serde(with = "serde_body")]
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

#[derive(Serialize, Deserialize)]
pub struct Response {
    pub version: Version,
    #[serde(with = "serde_body")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundRequestMeta {
    pub peer_id: PeerId,
    pub origin: Direction,
    #[serde(with = "tycho_util::serde_helpers::socket_addr")]
    pub remote_address: SocketAddr,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Inbound,
    Outbound,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        })
    }
}

mod serde_body {
    use base64::engine::Engine as _;
    use base64::prelude::BASE64_STANDARD;
    use tycho_util::serde_helpers::BorrowedStr;

    use super::*;

    pub fn serialize<S>(data: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&BASE64_STANDARD.encode(data))
        } else {
            data.serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        if deserializer.is_human_readable() {
            <BorrowedStr<'_> as Deserialize>::deserialize(deserializer).and_then(
                |BorrowedStr(s)| {
                    BASE64_STANDARD
                        .decode(s.as_ref())
                        .map(Bytes::from)
                        .map_err(Error::custom)
                },
            )
        } else {
            Bytes::deserialize(deserializer)
        }
    }
}
