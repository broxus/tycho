use std::sync::Arc;

use everscale_types::models::OwnedMessage;

// TYPES

pub(crate) type MempoolAnchorId = u32;

pub(crate) struct MempoolAnchor {
    id: MempoolAnchorId,
    chain_time: u64,
    externals: Vec<Arc<OwnedMessage>>,
}
impl MempoolAnchor {
    pub fn new(id: MempoolAnchorId, chain_time: u64, externals: Vec<Arc<OwnedMessage>>) -> Self {
        Self {
            id,
            chain_time,
            externals,
        }
    }
    pub fn id(&self) -> MempoolAnchorId {
        self.id
    }
    pub fn chain_time(&self) -> u64 {
        self.chain_time
    }
    pub fn externals_count(&self) -> usize {
        self.externals.len()
    }
    pub fn has_externals(&self) -> bool {
        !self.externals.is_empty()
    }
    pub fn externals_iterator(&self, from_idx: usize) -> impl Iterator<Item = Arc<OwnedMessage>> + '_ {
        self.externals.iter().skip(from_idx).cloned()
    }
}
