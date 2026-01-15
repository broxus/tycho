use std::cmp::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use tycho_storage::kv::InstanceId;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::FastHasherState;
use weedb::rocksdb;

use super::CoreDb;
use super::util::{read_block_id_le, write_block_id_le};
use crate::global_config::ZerostateId;

pub struct NodeStateStorage {
    db: CoreDb,
    last_mc_block_id: BlockIdCache,
    init_mc_block_id: BlockIdCache,
    zerostate_id: ArcSwapOption<ZerostateId>,
    zerostate_proof: ArcSwapOption<Cell>,
    zerostate_proof_bytes: ArcSwapOption<Bytes>,
}

pub enum NodeSyncState {
    PersistentState,
    Blocks,
}

impl NodeStateStorage {
    pub fn new(db: CoreDb) -> Self {
        let this = Self {
            db,
            last_mc_block_id: (Default::default(), LAST_MC_BLOCK_ID),
            init_mc_block_id: (Default::default(), INIT_MC_BLOCK_ID),
            zerostate_id: Default::default(),
            zerostate_proof: Default::default(),
            zerostate_proof_bytes: Default::default(),
        };

        let state = &this.db.state;
        if state.get(INSTANCE_ID).unwrap().is_none() {
            state
                .insert(INSTANCE_ID, rand::random::<InstanceId>())
                .unwrap();
        }

        this
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

    pub fn store_zerostate_info(&self, zerostate_id: &ZerostateId, zerostate_proof: &Cell) {
        let node_states = &self.db.state;

        let mut value = Vec::with_capacity(256);

        let mut batch = rocksdb::WriteBatch::default();

        // Insert zerostate ID.
        value.extend_from_slice(&zerostate_id.seqno.to_le_bytes());
        value.extend_from_slice(zerostate_id.root_hash.as_slice());
        value.extend_from_slice(zerostate_id.file_hash.as_slice());
        batch.put_cf(&node_states.cf(), ZEROSTATE_ID, value.as_slice());

        // Insert zerostate proof.
        value.clear();
        tycho_types::boc::ser::BocHeader::<FastHasherState>::with_root(zerostate_proof.as_ref())
            .encode(&mut value);
        batch.put_cf(&node_states.cf(), ZEROSTATE_PROOF, value);

        // Apply
        node_states
            .db()
            .write_opt(batch, node_states.write_config())
            .unwrap();
    }

    pub fn load_zerostate_id(&self) -> Option<ZerostateId> {
        if let Some(cached) = &*self.zerostate_id.load() {
            return Some(*cached.as_ref());
        }

        let value = self.load_zerostate_id_no_cache()?;
        self.zerostate_id.store(Some(Arc::new(value)));
        Some(value)
    }

    pub fn load_zerostate_mc_seqno(&self) -> Option<u32> {
        if let Some(cached) = &*self.zerostate_id.load() {
            return Some(cached.seqno);
        }

        let value = self.load_zerostate_id_no_cache()?;
        self.zerostate_id.store(Some(Arc::new(value)));
        Some(value.seqno)
    }

    fn load_zerostate_id_no_cache(&self) -> Option<ZerostateId> {
        let value = self.db.state.get(ZEROSTATE_ID).unwrap()?;
        let value = value.as_ref();
        Some(ZerostateId {
            seqno: u32::from_le_bytes(value[0..4].try_into().unwrap()),
            root_hash: HashBytes::from_slice(&value[4..36]),
            file_hash: HashBytes::from_slice(&value[36..68]),
        })
    }

    pub fn load_zerostate_proof(&self) -> Option<Cell> {
        if let Some(cached) = &*self.zerostate_proof.load() {
            return Some(cached.as_ref().clone());
        }

        let bytes = self.load_zerostate_proof_bytes()?;
        let cell = Boc::decode(&bytes).unwrap();

        self.zerostate_proof.store(Some(Arc::new(cell.clone())));
        Some(cell)
    }

    pub fn load_zerostate_proof_bytes(&self) -> Option<Bytes> {
        if let Some(cached) = &*self.zerostate_proof_bytes.load() {
            return Some(cached.as_ref().clone());
        }

        let value = self.db.state.get(ZEROSTATE_PROOF).unwrap()?;
        let bytes = Bytes::copy_from_slice(value.as_ref());

        self.zerostate_proof_bytes
            .store(Some(Arc::new(bytes.clone())));

        Some(bytes)
    }

    #[inline(always)]
    fn store_block_id(&self, (cache, key): &BlockIdCache, block_id: &BlockId) {
        let node_states = &self.db.state;
        node_states
            .insert(key, write_block_id_le(block_id))
            .unwrap();
        cache.store(Some(Arc::new(*block_id)));
    }

    #[inline(always)]
    fn load_block_id(&self, (cache, key): &BlockIdCache) -> Option<BlockId> {
        if let Some(cached) = &*cache.load() {
            return Some(*cached.as_ref());
        }

        let value = read_block_id_le(&self.db.state.get(key).unwrap()?);
        cache.store(Some(Arc::new(value)));
        Some(value)
    }

    pub fn load_instance_id(&self) -> InstanceId {
        let id = self.db.state.get(INSTANCE_ID).unwrap().unwrap();
        InstanceId::from_slice(id.as_ref())
    }
}

type BlockIdCache = (ArcSwapOption<BlockId>, &'static [u8]);

const LAST_MC_BLOCK_ID: &[u8] = b"last_mc_block";
const INIT_MC_BLOCK_ID: &[u8] = b"init_mc_block";
const INSTANCE_ID: &[u8] = b"instance_id";
const ZEROSTATE_ID: &[u8] = b"zerostate_id";
const ZEROSTATE_PROOF: &[u8] = b"zerostate_proof";
