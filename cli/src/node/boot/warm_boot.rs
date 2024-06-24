use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::{BlockId, ShardIdent};

use crate::node::Node;

/// Boot type when already synced or started syncing (there are states for each workchain).
///
/// Returns last masterchain key block id
pub async fn run(node: &Arc<Node>, mut last_mc_block_id: BlockId) -> Result<BlockId> {
    // TODO: why we need to return last key block instead of just mc block?

    tracing::info!("starting warm boot");

    let block_handle_storage = node.storage.block_handle_storage();
    let handle = block_handle_storage
        .load_handle(&last_mc_block_id)
        .ok_or(WarmBootError::FailedToLoadInitialBlock)?;

    let shard_state_storage = node.storage.shard_state_storage();
    let state = shard_state_storage.load_state(&last_mc_block_id).await?;

    if !handle.meta().is_key_block() {
        tracing::info!("started from non-key block");

        last_mc_block_id = state
            .state_extra()?
            .last_key_block
            .clone()
            .ok_or(WarmBootError::MasterchainStateNotFound)?
            .as_block_id(ShardIdent::MASTERCHAIN);

        tracing::info!(%last_mc_block_id);
    }

    tracing::info!("warm boot finished");

    Ok(last_mc_block_id)
}

#[derive(Debug, thiserror::Error)]
enum WarmBootError {
    #[error("Failed to load initial block handle")]
    FailedToLoadInitialBlock,
    #[error("Masterchain state not found")]
    MasterchainStateNotFound,
}
