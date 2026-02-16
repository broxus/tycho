use std::cmp::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use parking_lot::RwLock;
use tycho_storage::kv::InstanceId;
use tycho_types::models::*;
use tycho_types::prelude::*;
use tycho_util::{FastHashMap, FastHasherState};
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

    pending_persistent_states: RwLock<FastHashMap<u32, Vec<BlockId>>>,
    pending_persistent_queues: RwLock<FastHashMap<u32, Vec<BlockId>>>,
}

pub enum NodeSyncState {
    PersistentState,
    Blocks,
}

impl NodeStateStorage {
    pub fn new(db: CoreDb) -> Self {
        let pending_persistent_states =
            Self::load_pending_from_db(&db, PENDING_PERSISTENT_STATE_PREFIX);
        let pending_perstistent_queues =
            Self::load_pending_from_db(&db, PENDING_PERSISTENT_QUEUE_PREFIX);

        let this = Self {
            db,
            last_mc_block_id: (Default::default(), LAST_MC_BLOCK_ID),
            init_mc_block_id: (Default::default(), INIT_MC_BLOCK_ID),
            zerostate_id: Default::default(),
            zerostate_proof: Default::default(),
            zerostate_proof_bytes: Default::default(),
            pending_persistent_states: RwLock::new(pending_persistent_states),
            pending_persistent_queues: RwLock::new(pending_perstistent_queues),
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

    fn pending_key(prefix: &[u8], mc_seqno: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + 4);
        key.extend_from_slice(prefix);
        key.extend_from_slice(&mc_seqno.to_le_bytes());

        key
    }

    fn load_pending_from_db(db: &CoreDb, prefix: &[u8]) -> FastHashMap<u32, Vec<BlockId>> {
        let mut out = FastHashMap::default();
        let mut iter = db.state.prefix_iterator(prefix);

        while let Some((key, value)) = iter.item() {
            if !key.starts_with(prefix) {
                break;
            }
            let offset = prefix.len();
            let mc_seqno = u32::from_le_bytes(key[offset..offset + 4].try_into().unwrap());

            let mut value_offset = 0usize;
            let mut blocks = Vec::new();
            while value.is_empty() {
                let block_id = read_block_id_le(&value[value_offset..value_offset + 80]);
                blocks.push(block_id);
                value_offset += 80;
            }
            out.insert(mc_seqno, blocks);

            iter.next();
        }
        out
    }

    pub fn get_pending_persistent_states(&self, mc_seqno: u32) -> Option<Vec<BlockId>> {
        self.pending_persistent_states
            .read()
            .get(&mc_seqno)
            .cloned()
    }

    pub fn get_pending_persistent_queues(&self, mc_seqno: u32) -> Option<Vec<BlockId>> {
        self.pending_persistent_queues
            .read()
            .get(&mc_seqno)
            .cloned()
    }

    pub fn add_pending_persistent_states(&self, mc_seqno: u32, block_ids: &[BlockId]) {
        let key = Self::pending_key(PENDING_PERSISTENT_STATE_PREFIX, mc_seqno);
        let mut value = Vec::with_capacity(80 * block_ids.len());
        for block_id in block_ids {
            value.extend_from_slice(&write_block_id_le(block_id));
        }

        self.db.state.insert(key, value).unwrap();
        self.pending_persistent_states
            .write()
            .insert(mc_seqno, block_ids.to_vec());
    }

    pub fn remove_pending_persistent_states(&self, mc_seqno: u32) {
        let key = Self::pending_key(PENDING_PERSISTENT_STATE_PREFIX, mc_seqno);
        self.db.state.remove(key).unwrap();
        self.pending_persistent_states.write().remove(&mc_seqno);
    }

    pub fn add_pending_persistent_queue_state(&self, mc_seqno: u32, block_ids: &[BlockId]) {
        let key = Self::pending_key(PENDING_PERSISTENT_QUEUE_PREFIX, mc_seqno);
        let mut value = Vec::with_capacity(80 * block_ids.len());
        for block_id in block_ids {
            value.extend_from_slice(&write_block_id_le(block_id));
        }
        self.db.state.insert(key, value).unwrap();
        self.pending_persistent_queues
            .write()
            .insert(mc_seqno, block_ids.to_vec());
    }

    pub fn remove_pending_persistent_queue_state(&self, mc_seqno: u32) {
        let key = Self::pending_key(PENDING_PERSISTENT_QUEUE_PREFIX, mc_seqno);
        self.db.state.remove(key).unwrap();
        self.pending_persistent_queues.write().remove(&mc_seqno);
    }

    pub fn load_pending_shard_states(&self) -> Vec<(u32, Vec<BlockId>)> {
        self.pending_persistent_states
            .read()
            .iter()
            .map(|(mc_seqno, block_ids)| (*mc_seqno, block_ids.clone()))
            .collect()
    }

    pub fn load_pending_queue_states(&self) -> Vec<(u32, Vec<BlockId>)> {
        self.pending_persistent_queues
            .read()
            .iter()
            .map(|(mc_seqno, block_ids)| (*mc_seqno, block_ids.clone()))
            .collect()
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
const PENDING_PERSISTENT_STATE_PREFIX: &[u8] = b"pending_persistent_state/";
const PENDING_PERSISTENT_QUEUE_PREFIX: &[u8] = b"pending_persistent_queue/";
