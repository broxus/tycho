use std::sync::Arc;

use everscale_types::models::*;
use parking_lot::Mutex;

use crate::db::*;
use crate::util::*;

pub struct NodeStateStorage {
    db: Arc<Db>,
    last_mc_block_id: BlockIdCache,
    init_mc_block_id: BlockIdCache,
    shards_client_mc_block_id: BlockIdCache,
}

impl NodeStateStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            last_mc_block_id: (Default::default(), LAST_MC_BLOCK_ID),
            init_mc_block_id: (Default::default(), INIT_MC_BLOCK_ID),
            shards_client_mc_block_id: (Default::default(), SHARDS_CLIENT_MC_BLOCK_ID),
        }
    }

    pub fn store_historical_sync_start(&self, id: &BlockId) {
        let node_states = &self.db.node_states;
        node_states
            .insert(HISTORICAL_SYNC_LOW, id.to_vec())
            .unwrap()
    }

    pub fn load_historical_sync_start(&self) -> Option<BlockId> {
        match self.db.node_states.get(HISTORICAL_SYNC_LOW).unwrap() {
            Some(data) => Some(BlockId::from_slice(data.as_ref())),
            None => None,
        }
    }

    pub fn store_historical_sync_end(&self, id: &BlockId) {
        let node_states = &self.db.node_states;
        node_states
            .insert(HISTORICAL_SYNC_HIGH, id.to_vec())
            .unwrap();
    }

    pub fn load_historical_sync_end(&self) -> Option<BlockId> {
        let node_states = &self.db.node_states;
        let data = node_states.get(HISTORICAL_SYNC_HIGH).unwrap()?;
        Some(BlockId::from_slice(data.as_ref()))
    }

    pub fn store_last_uploaded_archive(&self, archive_id: u32) {
        let node_states = &self.db.node_states;
        node_states
            .insert(LAST_UPLOADED_ARCHIVE, archive_id.to_le_bytes())
            .unwrap();
    }

    pub fn load_last_uploaded_archive(&self) -> Option<u32> {
        match self.db.node_states.get(LAST_UPLOADED_ARCHIVE).unwrap() {
            Some(data) if data.len() >= 4 => {
                Some(u32::from_le_bytes(data[..4].try_into().unwrap()))
            }
            _ => None,
        }
    }

    pub fn store_last_mc_block_id(&self, id: &BlockId) {
        self.store_block_id(&self.last_mc_block_id, id)
    }

    pub fn load_last_mc_block_id(&self) -> Option<BlockId> {
        self.load_block_id(&self.last_mc_block_id)
    }

    pub fn store_init_mc_block_id(&self, id: &BlockId) {
        self.store_block_id(&self.init_mc_block_id, id)
    }

    pub fn load_init_mc_block_id(&self) -> Option<BlockId> {
        self.load_block_id(&self.init_mc_block_id)
    }

    pub fn store_shards_client_mc_block_id(&self, id: &BlockId) {
        self.store_block_id(&self.shards_client_mc_block_id, id)
    }

    pub fn load_shards_client_mc_block_id(&self) -> Option<BlockId> {
        self.load_block_id(&self.shards_client_mc_block_id)
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

const HISTORICAL_SYNC_LOW: &[u8] = b"background_sync_low";
const HISTORICAL_SYNC_HIGH: &[u8] = b"background_sync_high";

const LAST_UPLOADED_ARCHIVE: &[u8] = b"last_uploaded_archive";

const LAST_MC_BLOCK_ID: &[u8] = b"LastMcBlockId";
const INIT_MC_BLOCK_ID: &[u8] = b"InitMcBlockId";
const SHARDS_CLIENT_MC_BLOCK_ID: &[u8] = b"ShardsClientMcBlockId";
