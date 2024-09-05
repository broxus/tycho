use std::sync::Arc;

use arc_swap::ArcSwapOption;
use everscale_types::boc::BocRepr;
use everscale_types::models::{Block, BlockchainConfig};

use super::extractor::{ProtoOkResponse, RawProtoOkResponse};
use super::protos::rpc::response;

#[derive(Default)]
pub struct ProtoEndpointCache {
    latest_key_block: ArcSwapOption<RawProtoOkResponse>,
    blockchain_config: ArcSwapOption<RawProtoOkResponse>,
}

impl ProtoEndpointCache {
    pub fn load_latest_key_block(&self) -> arc_swap::Guard<Option<Arc<RawProtoOkResponse>>> {
        self.latest_key_block.load()
    }

    pub fn load_blockchain_config(&self) -> arc_swap::Guard<Option<Arc<RawProtoOkResponse>>> {
        self.blockchain_config.load()
    }

    pub fn handle_config(&self, global_id: i32, seqno: u32, config: &BlockchainConfig) {
        self.blockchain_config.store(match BocRepr::encode(config) {
            Ok(config) => {
                let res = response::Result::GetBlockchainConfig(response::GetBlockchainConfig {
                    global_id,
                    seqno,
                    config: config.into(),
                });
                Some(Arc::new(ProtoOkResponse::new(res).into_raw()))
            }
            Err(e) => {
                tracing::error!("failed to serialize blockchain config proto: {e}");
                None
            }
        });
    }

    pub fn handle_key_block(&self, block: &Block) {
        self.latest_key_block.store(match BocRepr::encode(block) {
            Ok(block) => {
                let res = ProtoOkResponse::new(response::Result::GetLatestKeyBlock(
                    response::GetLatestKeyBlock {
                        block: block.into(),
                    },
                ));
                Some(Arc::new(res.into_raw()))
            }
            Err(e) => {
                tracing::error!("failed to serialize key block proto: {e}");
                None
            }
        });
    }
}
