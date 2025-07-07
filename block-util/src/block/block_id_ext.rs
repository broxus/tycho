use tycho_types::models::{BlockId, BlockIdShort};

pub trait BlockIdExt {
    fn relative_to(self, mc_block_id: BlockId) -> BlockIdRelation;

    fn relative_to_self(self) -> BlockIdRelation;
}

#[derive(Default, Clone, Copy)]
pub struct BlockIdRelation {
    pub mc_block_id: BlockId,
    pub block_id: BlockId,
}

impl std::fmt::Debug for BlockIdRelation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct DebugBlockId<'a>(&'a BlockId);

        impl std::fmt::Debug for DebugBlockId<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(self.0, f)
            }
        }

        f.debug_struct("BlockIdRelation")
            .field("mc_block_id", &DebugBlockId(&self.mc_block_id))
            .field("block_id", &DebugBlockId(&self.block_id))
            .finish()
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

pub fn calc_next_block_id_short(prev_blocks_ids: &[BlockId]) -> BlockIdShort {
    debug_assert!(!prev_blocks_ids.is_empty());

    let shard = prev_blocks_ids[0].shard;
    let max_prev_seqno = prev_blocks_ids.iter().map(|id| id.seqno).max().unwrap();
    BlockIdShort {
        shard,
        seqno: max_prev_seqno + 1,
    }
}
