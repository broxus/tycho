use anyhow::{Context, Result};
use everscale_types::models::{BlockId, ShardIdent};

use super::StarterInner;

impl StarterInner {
    #[tracing::instrument(skip_all)]
    pub async fn warm_boot(&self, mut last_mc_block_id: BlockId) -> Result<BlockId> {
        // TODO: why we need to return last key block instead of just mc block?

        tracing::info!("started");

        let block_handle_storage = self.storage.block_handle_storage();
        let handle = block_handle_storage
            .load_handle(&last_mc_block_id)
            .context("failed to load an initial block handle")?;

        let shard_state_storage = self.storage.shard_state_storage();
        let state = shard_state_storage.load_state(&last_mc_block_id).await?;

        if !handle.meta().is_key_block() {
            tracing::info!("started from a non-key block");

            last_mc_block_id = state
                .state_extra()?
                .last_key_block
                .as_ref()
                .context("latest known key block not found")?
                .as_block_id(ShardIdent::MASTERCHAIN);
        }

        tracing::info!(%last_mc_block_id, "finished");
        Ok(last_mc_block_id)
    }
}
