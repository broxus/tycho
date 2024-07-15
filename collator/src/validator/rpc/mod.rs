use std::sync::Arc;

use tycho_network::PeerId;

pub use self::client::{ExchangeSignaturesBackoff, ValidatorClient, ValidatorClientBuilder};
pub use self::service::ValidatorService;
use crate::validator::proto;

mod client;
mod service;

pub trait ExchangeSignatures: Send + Sync + 'static {
    type IntoIter: IntoIterator<Item = proto::PeerSignatureOwned>;

    type Err: std::fmt::Debug + Send;

    async fn exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: Arc<[u8; 64]>,
    ) -> Result<Self::IntoIter, Self::Err>;
}
