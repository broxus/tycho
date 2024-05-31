use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::BlockId;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tycho_network::{PublicOverlay, Request};

use crate::overlay_client::{Error, PublicOverlayClient, QueryResponse};
use crate::proto::blockchain::*;
use crate::proto::overlay::BroadcastPrefix;

#[derive(Clone)]
#[repr(transparent)]
pub struct BlockchainRpcClient {
    inner: Arc<Inner>,
}

struct Inner {
    overlay_client: PublicOverlayClient,
}

impl BlockchainRpcClient {
    pub fn new(overlay_client: PublicOverlayClient) -> Self {
        Self {
            inner: Arc::new(Inner { overlay_client }),
        }
    }

    pub fn overlay(&self) -> &PublicOverlay {
        self.inner.overlay_client.overlay()
    }

    pub fn overlay_client(&self) -> &PublicOverlayClient {
        &self.inner.overlay_client
    }

    pub async fn broadcast_external_message(&self, message: &[u8]) {
        struct ExternalMessage<'a> {
            data: &'a [u8],
        }

        impl<'a> tl_proto::TlWrite for ExternalMessage<'a> {
            type Repr = tl_proto::Boxed;

            fn max_size_hint(&self) -> usize {
                4 + MessageBroadcastRef { data: self.data }.max_size_hint()
            }

            fn write_to<P>(&self, packet: &mut P)
            where
                P: tl_proto::TlPacket,
            {
                packet.write_u32(BroadcastPrefix::TL_ID);
                MessageBroadcastRef { data: self.data }.write_to(packet);
            }
        }

        // TODO: Add a proper target selector
        // TODO: Add rate limiting
        const TARGET_COUNT: usize = 5;

        let req = Request::from_tl(ExternalMessage { data: message });

        let client = &self.inner.overlay_client;

        // TODO: Reuse vector
        let neighbours = client.neighbours().choose_multiple(TARGET_COUNT).await;
        let mut futures = FuturesUnordered::new();
        for neighbour in neighbours {
            futures.push(client.send_raw(neighbour, req.clone()));
        }

        while let Some(res) = futures.next().await {
            if let Err(e) = res {
                tracing::warn!("failed to broadcast external message: {e}");
            }
        }
    }

    pub async fn get_next_key_block_ids(
        &self,
        block: &BlockId,
        max_size: u32,
    ) -> Result<QueryResponse<KeyBlockIds>, Error> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, KeyBlockIds>(&rpc::GetNextKeyBlockIds {
                block_id: *block,
                max_size,
            })
            .await?;
        Ok(data)
    }

    pub async fn get_block_full(&self, block: &BlockId) -> Result<QueryResponse<BlockFull>, Error> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, BlockFull>(&rpc::GetBlockFull { block_id: *block })
            .await?;
        Ok(data)
    }

    pub async fn get_next_block_full(
        &self,
        prev_block: &BlockId,
    ) -> Result<QueryResponse<BlockFull>, Error> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, BlockFull>(&rpc::GetNextBlockFull {
                prev_block_id: *prev_block,
            })
            .await?;
        Ok(data)
    }

    pub async fn get_archive_info(
        &self,
        mc_seqno: u32,
    ) -> Result<QueryResponse<ArchiveInfo>, Error> {
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
    ) -> Result<QueryResponse<Data>, Error> {
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
    ) -> Result<QueryResponse<PersistentStatePart>, Error> {
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
