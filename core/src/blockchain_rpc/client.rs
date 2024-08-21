use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use everscale_types::models::BlockId;
use futures_util::stream::{FuturesUnordered, StreamExt};
use scopeguard::ScopeGuard;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tycho_block_util::archive::ArchiveVerifier;
use tycho_block_util::state::ShardStateStuff;
use tycho_network::{PublicOverlay, Request};
use tycho_storage::Storage;
use tycho_util::futures::JoinTask;
use tycho_util::sync::rayon_run;

use crate::overlay_client::{
    Error, Neighbour, PublicOverlayClient, QueryResponse, QueryResponseHandle,
};
use crate::proto::blockchain::*;
use crate::proto::overlay::BroadcastPrefix;

/// A listener for self-broadcasted messages.
///
/// NOTE: `async_trait` is used to add object safety to the trait.
#[async_trait::async_trait]
pub trait SelfBroadcastListener: Send + Sync + 'static {
    async fn handle_message(&self, message: Bytes);
}

pub struct BlockchainRpcClientBuilder<MandatoryFields = PublicOverlayClient> {
    mandatory_fields: MandatoryFields,
    broadcast_listener: Option<Box<dyn SelfBroadcastListener>>,
}

impl BlockchainRpcClientBuilder<PublicOverlayClient> {
    pub fn build(self) -> BlockchainRpcClient {
        BlockchainRpcClient {
            inner: Arc::new(Inner {
                overlay_client: self.mandatory_fields,
                broadcast_listener: self.broadcast_listener,
            }),
        }
    }
}

impl BlockchainRpcClientBuilder<()> {
    pub fn with_public_overlay_client(
        self,
        client: PublicOverlayClient,
    ) -> BlockchainRpcClientBuilder<PublicOverlayClient> {
        BlockchainRpcClientBuilder {
            mandatory_fields: client,
            broadcast_listener: self.broadcast_listener,
        }
    }
}

impl<T> BlockchainRpcClientBuilder<T> {
    pub fn with_self_broadcast_listener(mut self, listener: impl SelfBroadcastListener) -> Self {
        self.broadcast_listener = Some(Box::new(listener));
        self
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct BlockchainRpcClient {
    inner: Arc<Inner>,
}

impl BlockchainRpcClient {
    pub fn builder() -> BlockchainRpcClientBuilder<()> {
        BlockchainRpcClientBuilder {
            mandatory_fields: (),
            broadcast_listener: None,
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

        // Broadcast to yourself
        if let Some(l) = &self.inner.broadcast_listener {
            l.handle_message(Bytes::copy_from_slice(message)).await;
        }

        // TODO: Add a proper target selector
        // TODO: Add rate limiting
        const TARGET_COUNT: usize = 5;

        let req = Request::from_tl(ExternalMessage { data: message });

        let client = &self.inner.overlay_client;

        // TODO: Reuse vector
        let neighbours = client.current_vset().choose_multiple(TARGET_COUNT).await;
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

    pub async fn get_key_block_proof(
        &self,
        block_id: &BlockId,
    ) -> Result<QueryResponse<KeyBlockProof>, Error> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, KeyBlockProof>(&rpc::GetKeyBlockProof {
                block_id: *block_id,
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

    pub async fn get_archive_chunk(
        &self,
        archive_id: u64,
        offset: u64,
    ) -> Result<QueryResponse<Data>, Error> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, Data>(&rpc::GetArchiveChunk { archive_id, offset })
            .await?;
        Ok(data)
    }

    pub async fn get_persistent_state_info(
        &self,
        block_id: &BlockId,
    ) -> Result<QueryResponse<PersistentStateInfo>, Error> {
        let client = &self.inner.overlay_client;
        let data = client
            .query::<_, PersistentStateInfo>(&rpc::GetPersistentStateInfo {
                block_id: *block_id,
            })
            .await?;
        Ok(data)
    }

    pub async fn get_persistent_state_part(
        &self,
        neighbour: &Neighbour,
        block_id: &BlockId,
        limit: u32,
        offset: u64,
    ) -> Result<QueryResponse<Data>, Error> {
        let client = &self.inner.overlay_client;
        let data = client
            .query_raw::<Data>(
                neighbour.clone(),
                Request::from_tl(rpc::GetPersistentStatePart {
                    block_id: *block_id,
                    limit,
                    offset,
                }),
            )
            .await?;
        Ok(data)
    }

    pub async fn download_and_store_state(
        &self,
        block_id: &BlockId,
        storage: Storage,
    ) -> Result<ShardStateStuff, Error> {
        const PARALLEL_REQUESTS: usize = 10;
        const CHUNK_SIZE: u32 = 2 << 20; // 2 MB
        const MAX_STATE_SIZE: u64 = 10 << 30; // 10 GB

        // TODO: Iterate through all known (or unknown) neighbours
        const NEIGHBOUR_COUNT: usize = 10;
        let neighbours = self
            .overlay_client()
            .neighbours()
            .choose_multiple(NEIGHBOUR_COUNT)
            .await;

        // Find a neighbour which has the requested state
        let (neighbour, max_size) = 'info: {
            let req = Request::from_tl(rpc::GetPersistentStateInfo {
                block_id: *block_id,
            });

            let mut futures = FuturesUnordered::new();
            for neighbour in neighbours {
                futures.push(self.overlay_client().query_raw(neighbour, req.clone()));
            }

            let mut err = None;
            while let Some(info) = futures.next().await {
                let (handle, info) = match info {
                    Ok(res) => res.split(),
                    Err(e) => {
                        err = Some(e);
                        continue;
                    }
                };

                match info {
                    PersistentStateInfo::Found { size } if size <= MAX_STATE_SIZE => {
                        break 'info (handle.accept(), size)
                    }
                    PersistentStateInfo::Found { size } => {
                        let neighbour = handle.reject();
                        tracing::warn!(
                            peer_id = %neighbour.peer_id(),
                            size,
                            "malicious neighbour has a too large state",
                        );
                        continue;
                    }
                    PersistentStateInfo::NotFound => continue,
                }
            }

            return match err {
                None => Err(Error::Internal(anyhow::anyhow!(
                    "no neighbour has the requested state"
                ))),
                Some(err) => Err(err),
            };
        };

        // Download the state
        let chunk_count = (max_size + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64;
        let mut stream =
            futures_util::stream::iter((0..chunk_count).map(|i| i * CHUNK_SIZE as u64))
                .map(|offset| {
                    let neighbour = neighbour.clone();
                    let req = Request::from_tl(rpc::GetPersistentStatePart {
                        block_id: *block_id,
                        limit: CHUNK_SIZE,
                        offset,
                    });

                    let client = self.overlay_client().clone();
                    JoinTask::new(async move {
                        // TODO: Retry on error
                        client.query_raw::<Data>(neighbour, req).await
                    })
                })
                .buffered(PARALLEL_REQUESTS);

        let mut store_state_op = storage
            .shard_state_storage()
            .begin_store_state_raw(block_id)
            .map(Box::new)
            .map_err(Error::Internal)?;

        // NOTE: Buffered items in stream will be polled because they are spawned as tasks
        while let Some(response) = stream.next().await.transpose()? {
            let (op, finished) = rayon_run(move || {
                let (handle, part) = response.split();
                match store_state_op.process_part(part.data) {
                    Ok(finished) => Ok((store_state_op, finished)),
                    Err(e) => {
                        handle.reject();
                        Err(e)
                    }
                }
            })
            .await
            .map_err(Error::Internal)?;

            if !finished {
                store_state_op = op;
                continue;
            }

            return rayon_run(move || op.finalize())
                .await
                .map_err(Error::Internal);
        }

        Err(Error::Internal(anyhow::anyhow!(
            "downloaded incomplete state"
        )))
    }

    pub async fn find_archive(&self, mc_seqno: u32) -> std::result::Result<PendingArchive, Error> {
        const NEIGHBOUR_COUNT: usize = 10;
        let neighbours = self
            .overlay_client()
            .neighbours()
            .choose_multiple(NEIGHBOUR_COUNT)
            .await;

        // Find a neighbour which has the requested archive
        let pending_archive = 'info: {
            let req = Request::from_tl(rpc::GetArchiveInfo { mc_seqno });

            let mut futures = FuturesUnordered::new();
            for neighbour in neighbours {
                futures.push(self.overlay_client().query_raw(neighbour, req.clone()));
            }

            let mut err = None;
            while let Some(info) = futures.next().await {
                let (handle, info) = match info {
                    Ok(res) => res.split(),
                    Err(e) => {
                        err = Some(e);
                        continue;
                    }
                };

                match info {
                    ArchiveInfo::Found {
                        id,
                        size,
                        chunk_size,
                    } => {
                        break 'info PendingArchive {
                            id,
                            size,
                            chunk_size,
                            neighbour: handle.accept(),
                        }
                    }
                    ArchiveInfo::NotFound => {
                        handle.accept();
                        continue;
                    }
                }
            }

            return match err {
                None => Err(Error::Internal(anyhow::anyhow!(
                    "no neighbour has the requested archive",
                ))),
                Some(err) => Err(err),
            };
        };

        tracing::debug!(
            peer_id = %pending_archive.neighbour.peer_id(),
            archive_id = pending_archive.id,
            archive_size = pending_archive.size.get(),
            archuve_chunk_size = pending_archive.chunk_size.get(),
            "found archive",
        );

        Ok(pending_archive)
    }

    #[tracing::instrument(skip_all, fields(
        peer_id = %archive.neighbour.peer_id(),
        archive_id = archive.id,
        archive_size = %bytesize::ByteSize::b(archive.size.get()),
        archive_chunk_size = %bytesize::ByteSize::b(archive.chunk_size.get() as _),
    ))]
    pub async fn download_archive(
        &self,
        archive: PendingArchive,
        output: &mut DynArchiveWriter,
    ) -> Result<(), Error> {
        const PARALLEL_REQUESTS: usize = 10;

        tracing::debug!("started");

        let mut stream = futures_util::stream::iter(
            (0..archive.size.get()).step_by(archive.chunk_size.get() as usize),
        )
        .map(|offset| {
            let archive_id = archive.id;
            let neighbour = archive.neighbour.clone();
            let overlay_client = self.overlay_client().clone();

            tracing::debug!(archive_id, offset, "downloading archive chunk");
            JoinTask::new(download_archive_inner(
                Request::from_tl(rpc::GetArchiveChunk { archive_id, offset }),
                overlay_client,
                neighbour,
            ))
        })
        .buffered(PARALLEL_REQUESTS);

        let mut downloaded = 0;
        let mut verifier = ArchiveVerifier::default();
        let mut stream = std::pin::pin!(stream);
        while let Some((h, chunk)) = stream.next().await.transpose()? {
            let guard = scopeguard::guard(h, |handle| {
                handle.reject();
            });

            downloaded += chunk.len() as u64;

            tracing::debug!(
                downloaded = %bytesize::ByteSize::b(downloaded),
                "got archive chunk"
            );
            verifier.write_verify(&chunk).map_err(|e| {
                Error::Internal(anyhow::anyhow!("Received invalid archive chunk: {e}"))
            })?;

            output.write_all(&chunk).await.map_err(|e| {
                Error::Internal(anyhow::anyhow!("Failed to write archive chunk: {e}"))
            })?;

            ScopeGuard::into_inner(guard).accept(); // defuse the guard
        }

        if archive.size.get() != downloaded {
            return Err(Error::Internal(anyhow::anyhow!("archive size mismatch")));
        }

        verifier
            .final_check()
            .map_err(|e| Error::Internal(anyhow::anyhow!("Received invalid archive: {e}")))?;
        output
            .flush()
            .await
            .map_err(|e| Error::Internal(anyhow::anyhow!("Failed to flush archive: {e}")))?;

        tracing::debug!("finished");
        Ok(())
    }
}

pub type DynArchiveWriter = dyn AsyncWrite + Unpin + Send;

struct Inner {
    overlay_client: PublicOverlayClient,
    broadcast_listener: Option<Box<dyn SelfBroadcastListener>>,
}

#[derive(Clone)]
pub struct PendingArchive {
    pub id: u64,
    pub size: NonZeroU64,
    pub chunk_size: NonZeroU32,
    pub neighbour: Neighbour,
}

async fn download_archive_inner(
    req: Request,
    overlay_client: PublicOverlayClient,
    neighbour: Neighbour,
) -> Result<(QueryResponseHandle, Bytes), Error> {
    // TODO: move to config?
    const MAX_RETRIES: usize = 10;

    let mut retries = 0;
    loop {
        match overlay_client
            .query_raw::<Data>(neighbour.clone(), req.clone())
            .await
        {
            Ok(r) => {
                let (h, res) = r.split();
                return Ok((h, res.data));
            }
            Err(e) => {
                tracing::error!("Failed to download archive slice: {e}");
                retries += 1;
                if retries >= MAX_RETRIES {
                    return Err(e);
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
