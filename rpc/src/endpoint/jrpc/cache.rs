use std::sync::Arc;

use arc_swap::ArcSwapOption;
use everscale_types::boc::BocRepr;
use everscale_types::models::{Block, BlockchainConfig};
use serde::Serialize;
use serde_json::value::RawValue;

#[derive(Default)]
pub struct JrpcEndpointCache {
    latest_key_block: ArcSwapOption<Box<RawValue>>,
    blockchain_config: ArcSwapOption<Box<RawValue>>,
}

impl JrpcEndpointCache {
    pub fn load_latest_key_block(&self) -> arc_swap::Guard<Option<CachedJson>> {
        self.latest_key_block.load()
    }

    pub fn load_blockchain_config(&self) -> arc_swap::Guard<Option<CachedJson>> {
        self.blockchain_config.load()
    }

    pub fn handle_config(&self, global_id: i32, seqno: u32, config: &BlockchainConfig) {
        self.blockchain_config.store(
            match serde_json::value::to_raw_value(&LatestBlockchainConfigRef {
                global_id,
                seqno,
                config,
            }) {
                Ok(value) => Some(Arc::new(value)),
                Err(e) => {
                    tracing::error!("failed to serialize blockchain config json: {e}");
                    None
                }
            },
        );
    }

    pub fn handle_key_block(&self, block: &Block) {
        self.latest_key_block.store(
            match serde_json::value::to_raw_value(&LatestKeyBlockRef { block }) {
                Ok(value) => Some(Arc::new(value)),
                Err(e) => {
                    tracing::error!("failed to serialize key block json: {e}");
                    None
                }
            },
        );
    }
}

#[derive(Debug, Clone, Serialize)]
struct LatestKeyBlockRef<'a> {
    #[serde(with = "BocRepr")]
    block: &'a Block,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct LatestBlockchainConfigRef<'a> {
    global_id: i32,
    seqno: u32,
    #[serde(with = "BocRepr")]
    config: &'a BlockchainConfig,
}

type CachedJson = Arc<Box<RawValue>>;
