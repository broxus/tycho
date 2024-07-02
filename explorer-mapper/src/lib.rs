use anyhow::Result;
use everscale_types::models::{Block, BlockId};
use explorer_models::ProcessingContext;

use crate::block::fill_block;

mod block;
mod message;
mod transaction;

pub fn fill_processing_context(block: &Block, block_id: BlockId) -> Result<ProcessingContext> {
    let mut context = ProcessingContext::default();
    fill_block(&mut context, block, block_id)?;

    Ok(context)
}
