use std::sync::Arc;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use everscale_types::boc::{Boc, BocRepr};
use everscale_types::cell::Cell;
use everscale_types::models::{Block, BlockchainConfig};
use everscale_types::prelude::HashBytes;
use tycho_util::FastHasherState;

use super::extractor::{ProtoOkResponse, RawProtoOkResponse};
use super::protos::rpc::response;
pub struct ProtoEndpointCache {
    libraries: moka::sync::Cache<HashBytes, Bytes, FastHasherState>,
    latest_key_block: ArcSwapOption<RawProtoOkResponse>,
    blockchain_config: ArcSwapOption<RawProtoOkResponse>,
}

impl Default for ProtoEndpointCache {
    fn default() -> Self {
        Self {
            libraries: moka::sync::Cache::builder()
                .max_capacity(100)
                .build_with_hasher(Default::default()),
            latest_key_block: Default::default(),
            blockchain_config: Default::default(),
        }
    }
}

impl ProtoEndpointCache {
    pub fn get_library_cell_proto(&self, hash: &HashBytes) -> Option<Bytes> {
        self.libraries.get(hash)
    }

    pub fn insert_library_cell(&self, hash_bytes: HashBytes, cell: Cell) -> Bytes {
        let boc = Boc::encode(cell);
        let boc_bytes: Bytes = boc.into();
        self.libraries.insert(hash_bytes, boc_bytes.clone());
        boc_bytes
    }
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
