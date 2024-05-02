use std::sync::Arc;

use everscale_types::models::*;
use parking_lot::Mutex;

use crate::db::*;
use crate::util::*;

pub struct NodeStateStorage {
    db: Arc<Db>,
    last_mc_block_id: BlockIdCache,
    init_mc_block_id: BlockIdCache,
}

impl NodeStateStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            last_mc_block_id: (Default::default(), LAST_MC_BLOCK_ID),
            init_mc_block_id: (Default::default(), INIT_MC_BLOCK_ID),
        }
    }

    pub fn store_last_mc_block_id(&self, id: &BlockId) {
        self.store_block_id(&self.last_mc_block_id, id);
    }

    pub fn load_last_mc_block_id(&self) -> Option<BlockId> {
        self.load_block_id(&self.last_mc_block_id)
    }

    pub fn store_init_mc_block_id(&self, id: &BlockId) {
        self.store_block_id(&self.init_mc_block_id, id);
    }

    pub fn load_init_mc_block_id(&self) -> Option<BlockId> {
        self.load_block_id(&self.init_mc_block_id)
    }

    #[inline(always)]
    fn store_block_id(&self, (cache, key): &BlockIdCache, block_id: &BlockId) {
        let node_states = &self.db.node_states;
        node_states
            .insert(key, write_block_id_le(block_id))
            .unwrap();
        *cache.lock() = Some(*block_id);
    }

    #[inline(always)]
    fn load_block_id(&self, (cache, key): &BlockIdCache) -> Option<BlockId> {
        if let Some(cached) = &*cache.lock() {
            return Some(*cached);
        }

        let value = read_block_id_le(&self.db.node_states.get(key).unwrap()?);
        *cache.lock() = Some(value);
        Some(value)
    }
}

type BlockIdCache = (Mutex<Option<BlockId>>, &'static [u8]);

const LAST_MC_BLOCK_ID: &[u8] = b"last_mc_block";
const INIT_MC_BLOCK_ID: &[u8] = b"init_mc_block";
