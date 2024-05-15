use std::sync::Arc;

use everscale_types::models::{ExtInMsgInfo, MsgInfo};
use everscale_types::prelude::Cell;

// TYPES

pub type MempoolAnchorId = u32;

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
}

impl From<&ExternalMessage> for (MsgInfo, Cell) {
    fn from(value: &ExternalMessage) -> Self {
        (
            MsgInfo::ExtIn(value.message_info.clone()),
            value.message_cell.clone(),
        )
    }
}

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

    pub fn has_externals(&self) -> bool {
        !self.externals.is_empty()
    }

    pub fn externals_iterator(
        &self,
        from_idx: usize,
    ) -> impl Iterator<Item = Arc<ExternalMessage>> + '_ {
        self.externals.iter().skip(from_idx).cloned()
    }
}
