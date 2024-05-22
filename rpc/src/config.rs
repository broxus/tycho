use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tycho_util::serde_helpers;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RpcConfig {
    /// TCP socket address to listen for incoming RPC connections.
    ///
    /// Default: `0.0.0.0:80`
    pub listen_addr: SocketAddr,

    /// Whether to generate a stub keyblock from zerostate.
    ///
    /// Default: `false`.
    pub generate_stub_keyblock: bool,

    /// Transactions garbage collector configuration.
    ///
    /// Default: clear all transactions older than `1 week` every `1 hour`.
    ///
    /// `None` to disable garbage collection.
    pub transactions_gc: Option<TransactionsGcConfig>,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            listen_addr: (Ipv4Addr::UNSPECIFIED, 80).into(),
            generate_stub_keyblock: false,
            transactions_gc: Some(Default::default()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionsGcConfig {
    /// Minimum transaction TTL.
    ///
    /// Default: `1 week`.
    #[serde(with = "serde_helpers::humantime")]
    pub tx_ttl: Duration,

    /// Interval between garbage collection runs.
    ///
    /// Default: `1 hour`.
    #[serde(with = "serde_helpers::humantime")]
    pub interval: Duration,
}

impl Default for TransactionsGcConfig {
    fn default() -> Self {
        Self {
            tx_ttl: Duration::from_secs(60 * 60 * 24 * 7),
            interval: Duration::from_secs(60 * 60),
        }
    }
}
