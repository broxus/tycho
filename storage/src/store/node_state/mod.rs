use everscale_types::models::*;
use parking_lot::Mutex;

use crate::db::*;
use crate::util::*;

pub struct NodeStateStorage {
    db: BaseDb,
    last_mc_block_id: BlockIdCache,
    init_mc_block_id: BlockIdCache,
}

impl NodeStateStorage {
    pub fn new(db: BaseDb) -> Self {
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
        let node_states = &self.db.state;
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

        let value = read_block_id_le(&self.db.state.get(key).unwrap()?);
        *cache.lock() = Some(value);
        Some(value)
    }

    pub fn store_instance_id(&self, id: InstanceId) {
        let node_states = &self.db.state;
        node_states.insert(INSTANCE_ID, id).unwrap();
    }

    pub fn load_instance_id(&self) -> Option<InstanceId> {
        let id = self.db.state.get(INSTANCE_ID).unwrap()?;
        Some(InstanceId::from_slice(id.as_ref()))
    }
}

type BlockIdCache = (Mutex<Option<BlockId>>, &'static [u8]);

const LAST_MC_BLOCK_ID: &[u8] = b"last_mc_block";
const INIT_MC_BLOCK_ID: &[u8] = b"init_mc_block";
