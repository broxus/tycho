use everscale_types::models::BlockId;

pub trait BlockStriderState: Send + Sync + 'static {
    fn load_last_traversed_master_block_id(&self) -> BlockId;
    fn is_traversed(&self, block_id: &BlockId) -> bool;
    fn commit_traversed(&self, block_id: BlockId);
}

impl<T: BlockStriderState> BlockStriderState for Box<T> {
    fn load_last_traversed_master_block_id(&self) -> BlockId {
        <T as BlockStriderState>::load_last_traversed_master_block_id(self)
    }

    fn is_traversed(&self, block_id: &BlockId) -> bool {
        <T as BlockStriderState>::is_traversed(self, block_id)
    }

    fn commit_traversed(&self, block_id: BlockId) {
        <T as BlockStriderState>::commit_traversed(self, block_id);
    }
}
