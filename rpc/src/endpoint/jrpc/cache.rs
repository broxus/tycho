use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use base64::prelude::{BASE64_STANDARD, Engine as _};
use moka::sync::Cache;
use serde::Serialize;
use serde_json::value::RawValue;
use tycho_types::boc::{Boc, BocRepr};
use tycho_types::cell::HashBytes;
use tycho_types::models::{Block, BlockId, BlockchainConfig};
use tycho_types::prelude::Cell;
use tycho_util::{FastHasherState, serde_helpers};

pub struct JrpcEndpointCache {
    libraries: Cache<HashBytes, CachedJson, FastHasherState>,
    key_block_proofs: Cache<u32, CachedJson, FastHasherState>,
    latest_key_block: ArcSwapOption<Box<RawValue>>,
    blockchain_config: ArcSwapOption<Box<RawValue>>,
}

impl Default for JrpcEndpointCache {
    fn default() -> Self {
        Self {
            libraries: Cache::builder()
                .max_capacity(100)
                .build_with_hasher(Default::default()),
            key_block_proofs: Cache::builder()
                .max_capacity(10)
                .build_with_hasher(Default::default()),
            latest_key_block: Default::default(),
            blockchain_config: Default::default(),
        }
    }
}

impl JrpcEndpointCache {
    pub fn insert_library_cell_response(
        &self,
        hash_bytes: HashBytes,
        cell: Option<Cell>,
    ) -> CachedJson {
        static EMPTY: OnceLock<CachedJson> = OnceLock::new();

        match cell {
            None => EMPTY
                .get_or_init(|| make_cached(GetLibraryCellResponse { cell: None }))
                .clone(),
            Some(cell) => {
                let res = make_cached(GetLibraryCellResponse {
                    cell: Some(Boc::encode_base64(cell)),
                });
                self.libraries.insert(hash_bytes, res.clone());
                res
            }
        }
    }

    pub fn get_library_cell_response(&self, hash: &HashBytes) -> Option<CachedJson> {
        self.libraries.get(hash)
    }

    pub fn insert_key_block_proof_response(
        &self,
        seqno: u32,
        proof: Option<(BlockId, impl AsRef<[u8]>)>,
    ) -> CachedJson {
        static EMPTY: OnceLock<CachedJson> = OnceLock::new();

        match proof {
            None => EMPTY
                .get_or_init(|| {
                    make_cached(BlockProofResponse {
                        block_id: None,
                        proof: None,
                    })
                })
                .clone(),
            Some((block_id, proof)) => {
                let res = make_cached(BlockProofResponse {
                    block_id: Some(block_id),
                    proof: Some(BASE64_STANDARD.encode(proof)),
                });
                self.key_block_proofs.insert(seqno, res.clone());
                res
            }
        }
    }

    pub fn get_key_block_proof_response(&self, seqno: u32) -> Option<CachedJson> {
        self.key_block_proofs.get(&seqno)
    }

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

#[derive(Serialize)]
struct GetLibraryCellResponse {
    cell: Option<String>,
}

// TODO: Add last_known_mc_seqno.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockProofResponse {
    #[serde(
        with = "serde_helpers::option_string",
        skip_serializing_if = "Option::is_none"
    )]
    block_id: Option<BlockId>,
    proof: Option<String>,
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

fn make_cached<T: Serialize>(value: T) -> CachedJson {
    Arc::new(serde_json::value::to_raw_value(&value).unwrap())
}
