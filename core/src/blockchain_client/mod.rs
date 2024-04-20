use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::BlockId;
use futures_util::future::BoxFuture;
use tycho_block_util::block::{BlockStuff, BlockStuffAug};

use crate::block_strider::provider::*;
use crate::overlay_client::public_overlay_client::*;
use crate::proto::overlay::rpc::*;
use crate::proto::overlay::*;

pub struct BlockchainClient {
    client: PublicOverlayClient,
}

impl BlockchainClient {
    pub fn new(overlay_client: PublicOverlayClient) -> Arc<BlockchainClient> {
        Arc::new(Self {
            client: overlay_client,
        })
    }

    pub async fn get_next_key_block_ids(
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

    pub async fn get_persistent_state_part(
        &self,
        mc_block: BlockId,
        block: BlockId,
        offset: u64,
        max_size: u64,
    ) -> Result<QueryResponse<'_, PersistentStatePart>> {
        let data = self
            .client
            .query::<GetPersistentStatePart, PersistentStatePart>(GetPersistentStatePart {
                block,
                mc_block,
                offset,
                max_size,
            })
            .await?;
        Ok(data)
    }
}

impl BlockProvider for BlockchainClient {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        Box::pin(async {
            let get_block = || async {
                let res = self.get_next_block_full(*prev_block_id).await?;
                let block = match res.data() {
                    BlockFull::Found {
                        block_id,
                        block: data,
                        ..
                    } => {
                        let block = BlockStuff::deserialize_checked(*block_id, data)?;
                        Some(BlockStuffAug::new(block, data.to_vec()))
                    }
                    BlockFull::Empty => None,
                };

                Ok::<_, anyhow::Error>(block)
            };

            match get_block().await {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        })
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        Box::pin(async {
            let get_block = || async {
                let res = self.get_block_full(*block_id).await?;
                let block = match res.data() {
                    BlockFull::Found {
                        block_id,
                        block: data,
                        ..
                    } => {
                        let block = BlockStuff::deserialize_checked(*block_id, data)?;
                        Some(BlockStuffAug::new(block, data.to_vec()))
                    }
                    BlockFull::Empty => None,
                };

                Ok::<_, anyhow::Error>(block)
            };

            match get_block().await {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        })
    }
}
