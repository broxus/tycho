use std::sync::Arc;
use std::time::Duration;

use futures_util::future::Future;
use serde::{Deserialize, Serialize};
use tycho_network::PeerId;
use tycho_util::serde_helpers;

pub use self::client::ValidatorClient;
pub use self::service::ValidatorService;
use crate::validator::proto;

mod client;
mod service;

// TODO: Add jitter as well?
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExchangeSignaturesBackoff {
    #[serde(with = "serde_helpers::humantime")]
    pub min_interval: Duration,
    #[serde(with = "serde_helpers::humantime")]
    pub max_interval: Duration,
    pub factor: f32,
}

impl Default for ExchangeSignaturesBackoff {
    fn default() -> Self {
        Self {
            min_interval: Duration::from_millis(50),
            max_interval: Duration::from_secs(1),
            factor: 1.5,
        }
    }
}

pub trait ExchangeSignatures: Send + Sync + 'static {
    type Err: std::fmt::Debug + Send;

    fn exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: Arc<[u8; 64]>,
    ) -> impl Future<Output = Result<proto::Exchange, Self::Err>> + Send;
}
