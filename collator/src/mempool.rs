use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use tycho_block_util::state::ShardStateStuff;

// ADAPTER

#[async_trait]
pub trait MempoolAdapter: Send + Sync + 'static {
    /// Schedule task to process new master block state (may perform gc or nodes rotation)
    async fn enqueue_process_new_mc_block_state(
        &self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()>;
}

pub(crate) struct MempoolAdapterStdImpl {}

#[async_trait]
impl MempoolAdapter for MempoolAdapterStdImpl {
    async fn enqueue_process_new_mc_block_state(
        &self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        todo!()
    }
}
