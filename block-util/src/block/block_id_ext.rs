use std::fmt::{Display, Formatter};

use everscale_types::models::BlockId;

pub trait BlockIdExt {
    fn relative_to(self, mc_block_id: BlockId) -> BlockIdRelation;

    fn relative_to_self(self) -> BlockIdRelation;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BlockIdRelation {
    mc_block_id: BlockId,
    block_id: BlockId,
}

impl BlockIdRelation {
    pub fn mc_block_id(&self) -> &BlockId {
        &self.mc_block_id
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }
}

impl Display for BlockIdRelation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "McBlockId {}:{}:{}:{} - BlockId {}:{}:{}:{}",
            self.mc_block_id.shard,
            self.mc_block_id.seqno,
            self.mc_block_id.root_hash,
            self.mc_block_id.file_hash,
            self.block_id.shard,
            self.block_id.seqno,
            self.block_id.root_hash,
            self.block_id.file_hash,
        ))
    }
}

impl BlockIdExt for BlockId {
    fn relative_to(self, mc_block_id: BlockId) -> BlockIdRelation {
        BlockIdRelation {
            block_id: self,
            mc_block_id,
        }
    }

    fn relative_to_self(self) -> BlockIdRelation {
        BlockIdRelation {
            mc_block_id: self,
            block_id: self,
        }
    }
}

impl BlockIdExt for &BlockId {
    fn relative_to(self, mc_block_id: BlockId) -> BlockIdRelation {
        BlockIdRelation {
            block_id: *self,
            mc_block_id,
        }
    }

    fn relative_to_self(self) -> BlockIdRelation {
        BlockIdRelation {
            mc_block_id: *self,
            block_id: *self,
        }
    }
}
