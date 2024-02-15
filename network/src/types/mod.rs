use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;

pub use self::address::Address;
pub use self::peer_id::{Direction, PeerId};
pub use self::peer_info::{PeerAffinity, PeerInfo};
pub use self::rpc::RpcQuery;
pub use self::service::{
    service_datagram_fn, service_message_fn, service_query_fn, BoxCloneService, BoxService,
    Service, ServiceDatagramFn, ServiceExt, ServiceMessageFn, ServiceQueryFn,
};

mod address;
mod peer_id;
mod peer_info;
mod rpc;
mod service;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerEvent {
    NewPeer(PeerId),
    LostPeer(PeerId, DisconnectReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason {
    Requested,
    VersionMismatch,
    TransportError,
    ConnectionClosed,
    ApplicationClosed,
    Reset,
    TimedOut,
    LocallyClosed,
}

impl From<quinn::ConnectionError> for DisconnectReason {
    #[inline]
    fn from(value: quinn::ConnectionError) -> Self {
        Self::from(&value)
    }
}

impl From<&quinn::ConnectionError> for DisconnectReason {
    fn from(value: &quinn::ConnectionError) -> Self {
        match value {
            quinn::ConnectionError::VersionMismatch => Self::VersionMismatch,
            quinn::ConnectionError::TransportError(_) => Self::TransportError,
            quinn::ConnectionError::ConnectionClosed(_) => Self::ConnectionClosed,
            quinn::ConnectionError::ApplicationClosed(_) => Self::ApplicationClosed,
            quinn::ConnectionError::Reset => Self::Reset,
            quinn::ConnectionError::TimedOut => Self::TimedOut,
            quinn::ConnectionError::LocallyClosed => Self::LocallyClosed,
        }
    }
}
