use everscale_types::models::Block;
use explorer_models::ProcessingContext;

mod block;

pub fn fill_processing_context(block: Block) -> ProcessingContext {
    let mut context = ProcessingContext::default();
    context.blocks.push(block);
    context
}
