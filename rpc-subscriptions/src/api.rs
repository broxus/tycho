use thiserror::Error;
use uuid::Uuid;

use crate::types::ClientId;

pub const MAX_ADDRS_PER_CLIENT: u8 = 255;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriberManagerConfig {
    /// Maximum unique addresses that can be interned at once.
    /// Exceeding this returns `SubscribeError::MaxAddrs`.
    pub max_addrs: u32,
    /// Maximum distinct clients tracked concurrently.
    /// Exceeding this returns `SubscribeError::MaxClients`.
    pub max_clients: u32,
}

impl SubscriberManagerConfig {
    pub const fn new(max_addrs: u32, max_clients: u32) -> Self {
        Self {
            max_addrs,
            max_clients,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubscribeError {
    #[error("maximum clients reached (limit {max_clients})")]
    MaxClients { max_clients: u32 },

    #[error("maximum addresses reached (limit {max_addrs})")]
    MaxAddrs { max_addrs: u32 },

    #[error("client {client_id} reached per-client address capacity ({max_per_client})")]
    ClientAtCapacity {
        client_id: ClientId,
        max_per_client: u8,
    },

    #[error("client {client_id} is already registered")]
    Collision { client_id: Uuid },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum UnsubscribeError {
    #[error("client not registered")]
    UnknownClient,

    #[error("address not tracked")]
    UnknownAddress,

    #[error("client not subscribed to address")]
    NotSubscribed,
}
