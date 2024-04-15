use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::BlockId;

use crate::overlay_client::public_overlay_client::{
    OverlayClient, PublicOverlayClient, QueryResponse,
};
use crate::proto::overlay::rpc::*;
use crate::proto::overlay::{ArchiveInfo, BlockFull, Data, KeyBlockIds};

pub struct BlockchainClient {
    client: PublicOverlayClient,
}

impl BlockchainClient {
    pub fn new(overlay_client: PublicOverlayClient) -> Arc<BlockchainClient> {
        Arc::new(Self {
            client: overlay_client,
        })
    }

    pub async fn get_next_block_ids(
        &self,
        block: BlockId,
        max_size: u32,
    ) -> Result<QueryResponse<'_, KeyBlockIds>> {
        let data = self
            .client
            .query::<GetNextKeyBlockIds, KeyBlockIds>(GetNextKeyBlockIds { block, max_size })
            .await?;
        Ok(data)
    }

    pub async fn get_block_full(&self, block: BlockId) -> Result<QueryResponse<'_, BlockFull>> {
        let data = self
            .client
            .query::<GetBlockFull, BlockFull>(GetBlockFull { block })
            .await?;
        Ok(data)
    }

    pub async fn get_next_block_full(
        &self,
        prev_block: BlockId,
    ) -> Result<QueryResponse<'_, BlockFull>> {
        let data = self
            .client
            .query::<GetNextBlockFull, BlockFull>(GetNextBlockFull { prev_block })
            .await?;
        Ok(data)
    }

    pub async fn get_archive_info(&self, mc_seqno: u32) -> Result<QueryResponse<'_, ArchiveInfo>> {
        let data = self
            .client
            .query::<GetArchiveInfo, ArchiveInfo>(GetArchiveInfo { mc_seqno })
            .await?;
        Ok(data)
    }

    pub async fn get_archive_slice(
        &self,
        archive_id: u64,
        offset: u64,
        max_size: u32,
    ) -> Result<QueryResponse<'_, Data>> {
        let data = self
            .client
            .query::<GetArchiveSlice, Data>(GetArchiveSlice {
                archive_id,
                offset,
                max_size,
            })
            .await?;
        Ok(data)
    }
}
