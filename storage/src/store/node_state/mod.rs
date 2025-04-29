use std::cmp::Ordering;
use std::sync::Arc;

use everscale_types::models::*;
use parking_lot::Mutex;

use crate::db::*;
use crate::util::*;

pub struct NodeStateStorage(Arc<NodeStateStorageInner>);

struct NodeStateStorageInner {
    db: BaseDb,
    last_mc_block_id: BlockIdCache,
    init_mc_block_id: BlockIdCache,
}

pub enum NodeSyncState {
    PersistentState,
    Blocks,
}

impl Clone for NodeStateStorage {
    fn clone(&self) -> Self {
        NodeStateStorage(self.0.clone())
    }
}

impl NodeStateStorage {
    pub fn new(db: BaseDb) -> Self {
        let inner = NodeStateStorageInner {
            db,
            last_mc_block_id: (Default::default(), LAST_MC_BLOCK_ID),
            init_mc_block_id: (Default::default(), INIT_MC_BLOCK_ID),
        };

        let state = &inner.db.state;
        if state.get(INSTANCE_ID).unwrap().is_none() {
            state
                .insert(INSTANCE_ID, rand::random::<InstanceId>())
                .unwrap();
        }

        Self(Arc::new(inner))
    }

    pub fn get_node_sync_state(&self) -> Option<NodeSyncState> {
        let init = self.load_init_mc_block_id()?;
        let last = self.load_last_mc_block_id()?;

        match last.seqno.cmp(&init.seqno) {
            Ordering::Equal => Some(NodeSyncState::PersistentState),
            Ordering::Greater => Some(NodeSyncState::Blocks),
            Ordering::Less => None,
        }
    }

    pub fn store_last_mc_block_id(&self, id: &BlockId) {
        self.store_block_id(&self.0.last_mc_block_id, id);
    }

    pub fn load_last_mc_block_id(&self) -> Option<BlockId> {
        self.load_block_id(&self.0.last_mc_block_id)
    }

    pub fn store_init_mc_block_id(&self, id: &BlockId) {
        self.store_block_id(&self.0.init_mc_block_id, id);
    }

    pub fn load_init_mc_block_id(&self) -> Option<BlockId> {
        self.load_block_id(&self.0.init_mc_block_id)
    }

    #[inline(always)]
    fn store_block_id(&self, (cache, key): &BlockIdCache, block_id: &BlockId) {
        let node_states = &self.0.db.state;
        let mut lock = cache.lock();
        // to sync with db write ops
        node_states
            .insert(key, write_block_id_le(block_id))
            .unwrap();
        *lock = Some(*block_id);
    }

    #[inline(always)]
    fn load_block_id(&self, (cache, key): &BlockIdCache) -> Option<BlockId> {
        if let Some(cached) = &*cache.lock() {
            return Some(*cached);
        }
        let value = read_block_id_le(&self.0.db.state.get(key).unwrap()?);
        *cache.lock() = Some(value);
        Some(value)
    }

    pub fn load_instance_id(&self) -> InstanceId {
        let id = self.0.db.state.get(INSTANCE_ID).unwrap().unwrap();
        InstanceId::from_slice(id.as_ref())
    }
}

type BlockIdCache = (Mutex<Option<BlockId>>, &'static [u8]);

const LAST_MC_BLOCK_ID: &[u8] = b"last_mc_block";
const INIT_MC_BLOCK_ID: &[u8] = b"init_mc_block";
const INSTANCE_ID: &[u8] = b"instance_id";
