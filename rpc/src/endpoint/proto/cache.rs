use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use tycho_types::boc::{Boc, BocRepr};
use tycho_types::cell::Cell;
use tycho_types::models::{Block, BlockId, BlockchainConfig};
use tycho_types::prelude::HashBytes;
use tycho_util::FastHasherState;

use super::extractor::{ProtoOkResponse, RawProtoOkResponse};
use super::make_response_block_id;
use super::protos::rpc::response;

pub struct ProtoEndpointCache {
    libraries: moka::sync::Cache<HashBytes, RawProtoOkResponse, FastHasherState>,
    key_block_proofs: moka::sync::Cache<u32, RawProtoOkResponse, FastHasherState>,
    latest_key_block: ArcSwapOption<RawProtoOkResponse>,
    blockchain_config: ArcSwapOption<RawProtoOkResponse>,
}

impl Default for ProtoEndpointCache {
    fn default() -> Self {
        Self {
            libraries: moka::sync::Cache::builder()
                .max_capacity(100)
                .build_with_hasher(Default::default()),
            key_block_proofs: moka::sync::Cache::builder()
                .max_capacity(10)
                .build_with_hasher(Default::default()),
            latest_key_block: Default::default(),
            blockchain_config: Default::default(),
        }
    }
}

impl ProtoEndpointCache {
    pub fn insert_library_cell_response(
        &self,
        hash_bytes: HashBytes,
        cell: Option<Cell>,
    ) -> RawProtoOkResponse {
        static EMPTY: OnceLock<RawProtoOkResponse> = OnceLock::new();

        match cell {
            None => EMPTY
                .get_or_init(|| {
                    make_cached(response::Result::GetLibraryCell(response::GetLibraryCell {
                        cell: None,
                    }))
                })
                .clone(),
            Some(cell) => {
                let res = make_cached(response::Result::GetLibraryCell(response::GetLibraryCell {
                    cell: Some(Bytes::from(Boc::encode(cell))),
                }));
                self.libraries.insert(hash_bytes, res.clone());
                res
            }
        }
    }

    pub fn get_library_cell_response(&self, hash: &HashBytes) -> Option<RawProtoOkResponse> {
        self.libraries.get(hash)
    }

    pub fn insert_key_block_proof_response(
        &self,
        seqno: u32,
        proof: Option<(BlockId, Bytes)>,
    ) -> RawProtoOkResponse {
        static EMPTY: OnceLock<RawProtoOkResponse> = OnceLock::new();

        match proof {
            None => EMPTY
                .get_or_init(|| {
                    make_cached(response::Result::GetKeyBlockProof(
                        response::GetKeyBlockProof { key_block: None },
                    ))
                })
                .clone(),
            Some((block_id, proof)) => {
                let res = make_cached(response::Result::GetKeyBlockProof(
                    response::GetKeyBlockProof {
                        key_block: Some(response::KeyBlockProof {
                            block_id: Some(make_response_block_id(block_id)),
                            proof,
                        }),
                    },
                ));
                self.key_block_proofs.insert(seqno, res.clone());
                res
            }
        }
    }

    pub fn get_key_block_proof_response(&self, seqno: u32) -> Option<RawProtoOkResponse> {
        self.key_block_proofs.get(&seqno)
    }

    pub fn load_latest_key_block(&self) -> arc_swap::Guard<Option<Arc<RawProtoOkResponse>>> {
        self.latest_key_block.load()
    }

    pub fn load_blockchain_config(&self) -> arc_swap::Guard<Option<Arc<RawProtoOkResponse>>> {
        self.blockchain_config.load()
    }

    pub fn handle_config(&self, global_id: i32, seqno: u32, config: &BlockchainConfig) {
        self.blockchain_config.store(match BocRepr::encode(config) {
            Ok(config) => Some(Arc::new(make_cached(
                response::Result::GetBlockchainConfig(response::GetBlockchainConfig {
                    global_id,
                    seqno,
                    config: config.into(),
                }),
            ))),
            Err(e) => {
                tracing::error!("failed to serialize blockchain config proto: {e:?}");
                None
            }
        });
    }

    pub fn handle_key_block(&self, block: &Block) {
        self.latest_key_block.store(match BocRepr::encode(block) {
            Ok(block) => Some(Arc::new(make_cached(response::Result::GetLatestKeyBlock(
                response::GetLatestKeyBlock {
                    block: block.into(),
                },
            )))),
            Err(e) => {
                tracing::error!("failed to serialize key block proto: {e:?}");
                None
            }
        });
    }
}

fn make_cached(res: response::Result) -> RawProtoOkResponse {
    ProtoOkResponse::new(res).into_raw()
}
