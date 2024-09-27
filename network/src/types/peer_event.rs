use serde::{Deserialize, Serialize};

use crate::types::PeerId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerEvent {
    pub peer_id: PeerId,
    pub data: PeerEventData,
}

impl PeerEvent {
    pub(crate) fn new_peer(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            data: PeerEventData::New,
        }
    }

    pub(crate) fn lost_peer(peer_id: PeerId, reason: DisconnectReason) -> Self {
        Self {
            peer_id,
            data: PeerEventData::Lost(reason),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerEventData {
    New,
    Lost(DisconnectReason),
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
