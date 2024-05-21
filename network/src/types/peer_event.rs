use serde::{Deserialize, Serialize};

use crate::types::PeerId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerEvent {
    NewPeer(PeerId),
    LostPeer(PeerId, DisconnectReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DisconnectReason {
    Requested,
    VersionMismatch,
    TransportError,
    ConnectionClosed,
    ApplicationClosed,
    Reset,
    TimedOut,
    LocallyClosed,
    CidsExhausted,
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
            quinn::ConnectionError::CidsExhausted => Self::CidsExhausted,
        }
    }
}
