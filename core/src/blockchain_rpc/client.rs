use std::io::Write;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use everscale_types::models::BlockId;
use futures_util::stream::{FuturesUnordered, StreamExt};
use scopeguard::ScopeGuard;
use tokio::sync::mpsc;
use tycho_block_util::archive::ArchiveVerifier;
use tycho_block_util::state::ShardStateStuff;
use tycho_network::{PublicOverlay, Request};
use tycho_storage::Storage;
use tycho_util::compression::ZstdDecompressStream;
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
    broadcast_timeout: Duration,
}

impl BlockchainRpcClientBuilder<PublicOverlayClient> {
    pub fn build(self) -> BlockchainRpcClient {
        BlockchainRpcClient {
            inner: Arc::new(Inner {
                overlay_client: self.mandatory_fields,
                broadcast_listener: self.broadcast_listener,
                broadcast_timeout: self.broadcast_timeout,
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
            broadcast_timeout: self.broadcast_timeout,
        }
    }
}

impl<T> BlockchainRpcClientBuilder<T> {
    pub fn with_self_broadcast_listener(mut self, listener: impl SelfBroadcastListener) -> Self {
        self.broadcast_listener = Some(Box::new(listener));
        self
    }

    pub fn with_broadcast_timeout(mut self, timeout: Duration) -> Self {
        self.broadcast_timeout = timeout;
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
            broadcast_timeout: Duration::from_secs(1),
        }
    }

    pub fn overlay(&self) -> &PublicOverlay {
        self.inner.overlay_client.overlay()
    }

    pub fn overlay_client(&self) -> &PublicOverlayClient {
        &self.inner.overlay_client
    }

    // TODO: Add rate limiting
    /// Broadcasts a message to the current targets list and
    /// returns the number of peers the message was delivered to.
    pub async fn broadcast_external_message(&self, message: &[u8]) -> usize {
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

        let client = &self.inner.overlay_client;

        let mut delivered_to = 0;

        let targets = client.get_broadcast_targets();
        let request = Request::from_tl(ExternalMessage { data: message });

        let mut futures = FuturesUnordered::new();
        for validator in targets.as_ref() {
            let client = client.clone();
            let validator = validator.clone();
            let request = request.clone();
            futures.push(JoinTask::new(async move {
                client.send_to_validator(validator, request).await
            }));
        }

        tokio::time::timeout(self.inner.broadcast_timeout, async {
            while let Some(res) = futures.next().await {
                if let Err(e) = res {
                    tracing::warn!("failed to broadcast external message: {e}");
                } else {
                    delivered_to += 1;
                }
            }
        })
        .await
        .ok();

        if delivered_to == 0 {
            tracing::debug!("message was not delivered to any peer");
        }

        delivered_to
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

    pub async fn get_block_full(
        &self,
        block: &BlockId,
        requirement: DataRequirement,
    ) -> Result<BlockDataFullWithNeighbour, Error> {
        let overlay_client = self.inner.overlay_client.clone();

        let Some(neighbour) = overlay_client.neighbours().choose().await else {
            return Err(Error::NoNeighbours);
        };

        download_block_inner(
            Request::from_tl(rpc::GetBlockFull { block_id: *block }),
            overlay_client,
            neighbour,
            requirement,
        )
        .await
    }

    pub async fn get_next_block_full(
        &self,
        prev_block: &BlockId,
        requirement: DataRequirement,
    ) -> Result<BlockDataFullWithNeighbour, Error> {
        let overlay_client = self.inner.overlay_client.clone();

        let Some(neighbour) = overlay_client.neighbours().choose().await else {
            return Err(Error::NoNeighbours);
        };

        download_block_inner(
            Request::from_tl(rpc::GetNextBlockFull {
                prev_block_id: *prev_block,
            }),
            overlay_client,
            neighbour,
            requirement,
        )
        .await
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

    pub async fn find_archive(&self, mc_seqno: u32) -> Result<Option<PendingArchive>, Error> {
        const NEIGHBOUR_COUNT: usize = 10;
        let neighbours = self
            .overlay_client()
            .neighbours()
            .choose_multiple(NEIGHBOUR_COUNT)
            .await;

        // Find a neighbour which has the requested archive
        let pending_archive = 'info: {
            let req = Request::from_tl(rpc::GetArchiveInfo { mc_seqno });

            // Real neighbours count
            let neighbour_count = neighbours.len();

            // Number of ArchiveInfo::TooNew responses
            let mut new_archive_count = 0usize;

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
                    ArchiveInfo::TooNew => {
                        new_archive_count += 1;

                        handle.accept();
                        continue;
                    }
                    ArchiveInfo::NotFound => {
                        handle.accept();
                        continue;
                    }
                }
            }

            // Stop using archives when enough neighbors
            // have responded ArchiveInfo::TooNew
            if new_archive_count >= neighbour_count {
                return Ok(None);
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

        Ok(Some(pending_archive))
    }

    #[tracing::instrument(skip_all, fields(
        peer_id = %archive.neighbour.peer_id(),
        archive_id = archive.id,
        archive_size = %bytesize::ByteSize::b(archive.size.get()),
        archive_chunk_size = %bytesize::ByteSize::b(archive.chunk_size.get() as _),
    ))]
    pub async fn download_archive<W>(
        &self,
        archive: PendingArchive,
        mut output: W,
    ) -> Result<W, Error>
    where
        W: Write + Send + 'static,
    {
        const PARALLEL_REQUESTS: usize = 10;

        tracing::debug!("started");

        let target_size = archive.size.get();
        let chunk_size = archive.chunk_size.get() as usize;

        let (chunks_tx, mut chunks_rx) =
            mpsc::channel::<(QueryResponseHandle, Bytes)>(PARALLEL_REQUESTS);

        let processing_task = tokio::task::spawn_blocking(move || {
            let mut verifier = ArchiveVerifier::default();
            let mut zstd_decoder = ZstdDecompressStream::new(chunk_size)?;

            // Reuse buffer for decompressed data
            let mut decompressed_chunk = Vec::new();

            // Receive and process chunks
            let mut downloaded = 0;
            while let Some((h, chunk)) = chunks_rx.blocking_recv() {
                let guard = scopeguard::guard(h, |handle| {
                    handle.reject();
                });

                anyhow::ensure!(chunk.len() <= chunk_size as _, "received invalid chunk");

                downloaded += chunk.len() as u64;
                tracing::debug!(
                    downloaded = %bytesize::ByteSize::b(downloaded),
                    "got archive chunk"
                );

                anyhow::ensure!(downloaded <= target_size, "received too many chunks");

                decompressed_chunk.clear();
                zstd_decoder.write(chunk.as_ref(), &mut decompressed_chunk)?;
                verifier.write_verify(&decompressed_chunk)?;
                output.write_all(&decompressed_chunk)?;

                ScopeGuard::into_inner(guard).accept(); // defuse the guard
            }

            anyhow::ensure!(
                target_size == downloaded,
                "archive size mismatch (target size: {target_size}; downloaded: {downloaded})",
            );

            verifier.final_check()?;
            output.flush()?;

            Ok(output)
        });

        let mut stream = futures_util::stream::iter((0..archive.size.get()).step_by(chunk_size))
            .map(|offset| {
                let archive_id = archive.id;
                let neighbour = archive.neighbour.clone();
                let overlay_client = self.overlay_client().clone();

                tracing::debug!(archive_id, offset, "downloading archive chunk");
                JoinTask::new(download_with_retries(
                    Request::from_tl(rpc::GetArchiveChunk { archive_id, offset }),
                    overlay_client,
                    neighbour,
                ))
            })
            .buffered(PARALLEL_REQUESTS);

        let mut stream = std::pin::pin!(stream);
        while let Some(chunk) = stream.next().await.transpose()? {
            if chunks_tx.send(chunk).await.is_err() {
                break;
            }
        }

        drop(chunks_tx);

        let output = processing_task
            .await
            .map_err(|e| Error::Internal(anyhow::anyhow!("Failed to join blocking task: {e}")))?
            .map_err(Error::Internal)?;

        tracing::debug!("finished");
        Ok(output)
    }
}

struct Inner {
    overlay_client: PublicOverlayClient,
    broadcast_listener: Option<Box<dyn SelfBroadcastListener>>,
    broadcast_timeout: Duration,
}

#[derive(Clone)]
pub struct PendingArchive {
    pub id: u64,
    pub size: NonZeroU64,
    pub chunk_size: NonZeroU32,
    pub neighbour: Neighbour,
}

async fn download_with_retries(
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

pub struct BlockDataFull {
    pub block_id: BlockId,
    pub block_data: Bytes,
    pub proof_data: Bytes,
    pub queue_diff_data: Bytes,
}

pub struct BlockDataFullWithNeighbour {
    pub data: Option<BlockDataFull>,
    pub neighbour: Neighbour,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataRequirement {
    /// Data is not required to be present on the neighbour (mostly for polling).
    ///
    /// NOTE: Node will not be punished if the data is not present.
    Optional,
    /// We assume that the node has the data, but it's not required.
    ///
    /// NOTE: Node will be punished as [`PunishReason::Dumb`] if the data is not present.
    Expected,
    /// Data must be present on the requested neighbour.
    ///
    /// NOTE: Node will be punished as [`PunishReason::Malicious`] if the data is not present.
    Required,
}

async fn download_block_inner(
    req: Request,
    overlay_client: PublicOverlayClient,
    neighbour: Neighbour,
    requirement: DataRequirement,
) -> Result<BlockDataFullWithNeighbour, Error> {
    let response = overlay_client
        .query_raw::<BlockFull>(neighbour.clone(), req)
        .await?;

    let (handle, block_full) = response.split();

    let BlockFull::Found {
        block_id,
        block: block_data,
        proof: proof_data,
        queue_diff: queue_diff_data,
    } = block_full
    else {
        match requirement {
            DataRequirement::Optional => {
                handle.accept();
            }
            DataRequirement::Expected => {
                handle.reject();
            }
            DataRequirement::Required => {
                neighbour.punish(crate::overlay_client::PunishReason::Malicious);
            }
        }

        return Ok(BlockDataFullWithNeighbour {
            data: None,
            neighbour,
        });
    };

    const PARALLEL_REQUESTS: usize = 10;

    let target_size = block_data.size.get();
    let chunk_size = block_data.chunk_size.get();
    let block_data_size = block_data.data.len() as u32;

    if block_data_size > target_size || block_data_size > chunk_size {
        return Err(Error::Internal(anyhow::anyhow!("invalid first chunk")));
    }

    let (chunks_tx, mut chunks_rx) =
        mpsc::channel::<(QueryResponseHandle, Bytes)>(PARALLEL_REQUESTS);

    let processing_task = tokio::task::spawn_blocking(move || {
        let mut zstd_decoder = ZstdDecompressStream::new(chunk_size as usize)?;

        // Buffer for decompressed data
        let mut decompressed = Vec::new();

        // Decompress chunk
        zstd_decoder.write(block_data.data.as_ref(), &mut decompressed)?;

        // Receive and process chunks
        let mut downloaded = block_data.data.len() as u32;
        while let Some((h, chunk)) = chunks_rx.blocking_recv() {
            let guard = scopeguard::guard(h, |handle| {
                handle.reject();
            });

            anyhow::ensure!(chunk.len() <= chunk_size as _, "received invalid chunk");

            downloaded += chunk.len() as u32;
            tracing::debug!(
                downloaded = %bytesize::ByteSize::b(downloaded as _),
                "got block data chunk"
            );

            anyhow::ensure!(downloaded <= target_size, "received too many chunks");

            // Decompress chunk
            zstd_decoder.write(chunk.as_ref(), &mut decompressed)?;

            ScopeGuard::into_inner(guard).accept(); // defuse the guard
        }

        anyhow::ensure!(
            target_size == downloaded,
            "block size mismatch (target size: {target_size}; downloaded: {downloaded})",
        );

        Ok(decompressed)
    });

    let mut stream =
        futures_util::stream::iter((chunk_size..target_size).step_by(chunk_size as usize))
            .map(|offset| {
                let neighbour = neighbour.clone();
                let overlay_client = overlay_client.clone();

                tracing::debug!(%block_id, offset, "downloading block data chunk");
                JoinTask::new(download_with_retries(
                    Request::from_tl(rpc::GetBlockDataChunk { block_id, offset }),
                    overlay_client,
                    neighbour,
                ))
            })
            .buffered(PARALLEL_REQUESTS);

    let mut stream = std::pin::pin!(stream);
    while let Some(chunk) = stream.next().await.transpose()? {
        if chunks_tx.send(chunk).await.is_err() {
            break;
        }
    }

    drop(chunks_tx);

    let block_data = processing_task
        .await
        .map_err(|e| Error::Internal(anyhow::anyhow!("Failed to join blocking task: {e}")))?
        .map(Bytes::from)
        .map_err(Error::Internal)?;

    Ok(BlockDataFullWithNeighbour {
        data: Some(BlockDataFull {
            block_id,
            block_data,
            proof_data,
            queue_diff_data,
        }),
        neighbour: neighbour.clone(),
    })
}
