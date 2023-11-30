use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub use self::address::{Address, AddressList};
pub use self::peer_id::{Direction, PeerId};

mod address;
mod peer_id;

pub type FastDashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;
pub type FastHashMap<K, V> = HashMap<K, V, ahash::RandomState>;

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

pub struct Request<T> {
    pub version: Version,
    pub body: T,
}

pub struct Response<T> {
    pub version: Version,
    pub body: T,
}

pub struct InboundServiceRequest<T> {
    pub metadata: Arc<InboundRequestMeta>,
    pub body: T,
}

#[derive(Debug, Clone)]
pub struct InboundRequestMeta {
    pub peer_id: PeerId,
    pub origin: Direction,
    pub remote_address: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PeerAffinity {
    High,
    Allowed,
    Never,
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub affinity: PeerAffinity,
    // TODO: change to address list
    pub address: Address,
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
