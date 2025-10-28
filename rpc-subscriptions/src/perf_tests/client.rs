use std::collections::HashSet;
use std::sync::atomic::AtomicU64;

use dashmap::DashMap;
use parking_lot::RwLock;
use tycho_types::models::StdAddr;
use uuid::Uuid;

#[derive(Debug)]
pub struct Client {
    pub uuid: Uuid,
    pub subscriptions: RwLock<HashSet<StdAddr>>,
    pub target_subs: usize,
    pub temp_subs: DashMap<StdAddr, u64>,
    pub last_full_resub_block: AtomicU64,
}

impl Client {
    pub fn new(uuid: Uuid, target_subs: usize) -> Self {
        Self {
            uuid,
            subscriptions: RwLock::new(HashSet::new()),
            target_subs,
            temp_subs: DashMap::new(),
            last_full_resub_block: AtomicU64::new(0),
        }
    }
}
