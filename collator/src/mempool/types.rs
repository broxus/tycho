use std::sync::Arc;

use everscale_types::models::{ExtInMsgInfo, ShardIdent};
use everscale_types::prelude::{Cell, HashBytes};

use crate::types::ShardIdentExt;

// TYPES

pub type MempoolAnchorId = u32;

#[derive(Debug)]
pub struct ExternalMessage {
    message_cell: Cell,
    message_info: ExtInMsgInfo,
}

impl ExternalMessage {
    pub fn new(message_cell: Cell, message_info: ExtInMsgInfo) -> ExternalMessage {
        Self {
            message_cell,
            message_info,
        }
    }

    pub fn info(&self) -> &ExtInMsgInfo {
        &self.message_info
    }

    pub fn hash(&self) -> &HashBytes {
        self.message_cell.repr_hash()
    }

    pub fn cell(&self) -> &Cell {
        &self.message_cell
    }
}

#[derive(Debug)]
pub struct MempoolAnchor {
    id: MempoolAnchorId,
    chain_time: u64,
    externals: Vec<Arc<ExternalMessage>>,
}

impl MempoolAnchor {
    pub fn new(id: MempoolAnchorId, chain_time: u64, externals: Vec<Arc<ExternalMessage>>) -> Self {
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

    pub fn check_has_externals_for(&self, shard_id: &ShardIdent) -> bool {
        self.externals
            .iter()
            .any(|ext| shard_id.contains_address(&ext.info().dst))
    }

    pub fn externals_iterator(
        &self,
        from_idx: usize,
    ) -> impl Iterator<Item = Arc<ExternalMessage>> + '_ {
        self.externals.iter().skip(from_idx).cloned()
    }
}
