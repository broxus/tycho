use std::sync::Arc;
use crate::overlay_client::public_overlay_client::{OverlayClient, PublicOverlayClient};

pub struct BlockchainClient {
    client: PublicOverlayClient
}

impl BlockchainClient {

    pub fn new(overlay_client: PublicOverlayClient) -> Arc<BlockchainClient> {
        Arc::new(Self {
            client: overlay_client
        })
    }
}