use everscale_types::models::{Block, BlockId};
use explorer_models::ProcessingContext;

mod block;
mod message;
mod transaction;

pub fn fill_processing_context(block: Block, block_id: BlockId) -> ProcessingContext {
    let mut context = ProcessingContext::default();

    context
}
