use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::models::BlockId;

use crate::overlay_client::{PublicOverlayClient, QueryResponse};
use crate::proto::overlay::rpc::*;
use crate::proto::overlay::*;

pub struct BlockchainClientConfig {
    pub get_next_block_polling_interval: Duration,
    pub get_block_polling_interval: Duration,
}

impl Default for BlockchainClientConfig {
    fn default() -> Self {
        Self {
            get_block_polling_interval: Duration::from_millis(50),
            get_next_block_polling_interval: Duration::from_millis(50),
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct BlockchainClient {
    inner: Arc<Inner>,
}

struct Inner {
    overlay_client: PublicOverlayClient,
    config: BlockchainClientConfig,
}

impl BlockchainClient {
    pub fn new(overlay_client: PublicOverlayClient, config: BlockchainClientConfig) -> Self {
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

    pub fn config(&self) -> &BlockchainClientConfig {
        &self.inner.config
    }

    pub async fn get_next_key_block_ids(
        &self,
        block: &BlockId,
        max_size: u32,
    ) -> Result<QueryResponse<KeyBlockIds>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, KeyBlockIds>(&GetNextKeyBlockIds {
                block: *block,
                max_size,
            })
            .await?;
        Ok(data)
    }

    pub async fn get_block_full(&self, block: &BlockId) -> Result<QueryResponse<BlockFull>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, BlockFull>(GetBlockFull { block: *block })
            .await?;
        Ok(data)
    }

    pub async fn get_next_block_full(
        &self,
        prev_block: &BlockId,
    ) -> Result<QueryResponse<BlockFull>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, BlockFull>(GetNextBlockFull {
                prev_block: *prev_block,
            })
            .await?;
        Ok(data)
    }

    pub async fn get_archive_info(&self, mc_seqno: u32) -> Result<QueryResponse<ArchiveInfo>> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, ArchiveInfo>(GetArchiveInfo { mc_seqno })
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
            .query::<_, Data>(GetArchiveSlice {
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
            .query::<_, PersistentStatePart>(GetPersistentStatePart {
                block: *block,
                mc_block: *mc_block,
                offset,
                max_size,
            })
            .await?;
        Ok(data)
    }
}
