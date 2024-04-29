use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::models::BlockId;

use crate::overlay_client::{PublicOverlayClient, QueryResponse};
use crate::proto::blockchain::*;

pub struct BlockchainRpcClientConfig {
    pub get_next_block_polling_interval: Duration,
    pub get_block_polling_interval: Duration,
}

impl Default for BlockchainRpcClientConfig {
    fn default() -> Self {
        Self {
            get_block_polling_interval: Duration::from_millis(50),
            get_next_block_polling_interval: Duration::from_millis(50),
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct BlockchainRpcClient {
    inner: Arc<Inner>,
}

struct Inner {
    overlay_client: PublicOverlayClient,
    config: BlockchainRpcClientConfig,
}

impl BlockchainRpcClient {
    pub fn new(overlay_client: PublicOverlayClient, config: BlockchainRpcClientConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                overlay_client,
                config,
            }),
        }
    }

    pub fn overlay_client(&self) -> &PublicOverlayClient {
        &self.inner.overlay_client
    }

    pub fn config(&self) -> &BlockchainRpcClientConfig {
        &self.inner.config
    }

    pub async fn get_next_key_block_ids(
        &self,
        block: &BlockId,
        max_size: u32,
    ) -> Result<QueryResponse<KeyBlockIds>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, KeyBlockIds>(&rpc::GetNextKeyBlockIds {
                block_id: *block,
                max_size,
            })
            .await?;
        Ok(data)
    }

    pub async fn get_block_full(&self, block: &BlockId) -> Result<QueryResponse<BlockFull>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, BlockFull>(&rpc::GetBlockFull { block_id: *block })
            .await?;
        Ok(data)
    }

    pub async fn get_next_block_full(
        &self,
        prev_block: &BlockId,
    ) -> Result<QueryResponse<BlockFull>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, BlockFull>(&rpc::GetNextBlockFull {
                prev_block_id: *prev_block,
            })
            .await?;
        Ok(data)
    }

    pub async fn get_archive_info(&self, mc_seqno: u32) -> Result<QueryResponse<ArchiveInfo>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, ArchiveInfo>(&rpc::GetArchiveInfo { mc_seqno })
            .await?;
        Ok(data)
    }

    pub async fn get_archive_slice(
        &self,
        archive_id: u64,
        offset: u64,
        max_size: u32,
    ) -> Result<QueryResponse<Data>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, Data>(&rpc::GetArchiveSlice {
                archive_id,
                offset,
                max_size,
            })
            .await?;
        Ok(data)
    }

    pub async fn get_persistent_state_part(
        &self,
        mc_block: &BlockId,
        block: &BlockId,
        offset: u64,
        max_size: u64,
    ) -> Result<QueryResponse<PersistentStatePart>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, PersistentStatePart>(&rpc::GetPersistentStatePart {
                block_id: *block,
                mc_block_id: *mc_block,
                offset,
                max_size,
            })
            .await?;
        Ok(data)
    }
}
