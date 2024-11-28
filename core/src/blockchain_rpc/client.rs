use std::future::Future;
use std::io::Write;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use bytesize::ByteSize;
use everscale_types::models::BlockId;
use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use scopeguard::ScopeGuard;
use tokio::sync::mpsc;
use tycho_block_util::archive::ArchiveVerifier;
use tycho_network::{PublicOverlay, Request};
use tycho_storage::PersistentStateKind;
use tycho_util::compression::ZstdDecompressStream;
use tycho_util::futures::JoinTask;

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
                // todo: do we need to move this to config?
                min_broadcast_timeout: Duration::from_millis(100),
                response_tracker: Mutex::new(
                    // 5 windows, 60 seconds each, 0.75 quantile
                    tycho_util::time::RollingP2Estimator::new_with_config(
                        0.75, // should be enough to filter most of the outliers
                        Duration::from_secs(60),
                        5,
                        tycho_util::time::RealClock,
                    )
                    .expect("correct quantile"),
                ),
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

    // TODO: Add rate limiting
    /// Broadcasts a message to the current targets list and
    /// returns the number of peers the message was delivered to.
    pub async fn broadcast_external_message(&self, message: &[u8]) -> usize {
        struct ExternalMessage<'a> {
            data: &'a [u8],
        }

        impl tl_proto::TlWrite for ExternalMessage<'_> {
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

        // we wait for all the responses to come back but cap them at `broadcast_timeout_upper_bound`
        // all peers timeouts are calculated based on p90 of the previous responses time weighted average
        // This will make broadcast timeout to be adaptive based on the network conditions
        for validator in targets.as_ref() {
            let client = client.clone();
            let validator = validator.clone();
            let request = request.clone();
            let this = self.inner.clone();
            // we are not using `JoinTask` here because we want to measure the time taken by the broadcast
            futures.push(tokio::spawn(async move {
                let start = Instant::now();
                let res = client.send_to_validator(validator, request).await;
                this.response_tracker
                    .lock()
                    .append(start.elapsed().as_millis() as i64);
                res
            }));
        }

        let timeout = self.compute_broadcast_timeout();
        tokio::time::timeout(timeout, async {
            // inner task timeout won't happen because outer task timeout is always <= inner task timeout
            while let Some(Ok(res)) = futures.next().await {
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

    fn compute_broadcast_timeout(&self) -> Duration {
        let max_broadcast_timeout = std::cmp::max(
            self.inner.overlay_client.config().validators.send_timeout,
            self.inner.min_broadcast_timeout,
        );

        if let Some(prev_time) = self
            .inner
            .response_tracker
            .lock()
            .exponentially_weighted_average()
            .map(|x| Duration::from_millis(x as _))
        {
            metrics::gauge!("tycho_broadcast_timeout", "kind" => "calculated").set(prev_time);
            let value = prev_time.clamp(self.inner.min_broadcast_timeout, max_broadcast_timeout);
            metrics::gauge!("tycho_broadcast_timeout", "kind" => "clamped").set(value);
            value
        } else {
            max_broadcast_timeout
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
            .query::<_, PersistentStateInfo>(&rpc::GetPersistentShardStateInfo {
                block_id: *block_id,
            })
            .await?;
        Ok(data)
    }

    pub async fn get_persistent_state_part(
        &self,
        neighbour: &Neighbour,
        block_id: &BlockId,
        offset: u64,
    ) -> Result<QueryResponse<Data>, Error> {
        let client = &self.inner.overlay_client;
        let data = client
            .query_raw::<Data>(
                neighbour.clone(),
                Request::from_tl(rpc::GetPersistentShardStateChunk {
                    block_id: *block_id,
                    offset,
                }),
            )
            .await?;
        Ok(data)
    }

    pub async fn find_persistent_state(
        &self,
        block_id: &BlockId,
        kind: PersistentStateKind,
    ) -> Result<PendingPersistentState, Error> {
        const NEIGHBOUR_COUNT: usize = 10;

        let neighbours = self
            .overlay_client()
            .neighbours()
            .choose_multiple(NEIGHBOUR_COUNT)
            .await;

        let req = match kind {
            PersistentStateKind::Shard => Request::from_tl(rpc::GetPersistentShardStateInfo {
                block_id: *block_id,
            }),
            PersistentStateKind::Queue => Request::from_tl(rpc::GetPersistentQueueStateInfo {
                block_id: *block_id,
            }),
        };

        let mut futures = FuturesUnordered::new();
        for neighbour in neighbours {
            futures.push(
                self.overlay_client()
                    .query_raw::<PersistentStateInfo>(neighbour.clone(), req.clone()),
            );
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
                PersistentStateInfo::Found { size, chunk_size } => {
                    let neighbour = handle.accept();
                    tracing::debug!(
                        peer_id = %neighbour.peer_id(),
                        state_size = size.get(),
                        state_chunk_size = chunk_size.get(),
                        ?kind,
                        "found persistent state",
                    );

                    return Ok(PendingPersistentState {
                        block_id: *block_id,
                        kind,
                        size,
                        chunk_size,
                        neighbour,
                    });
                }
                PersistentStateInfo::NotFound => continue,
            }
        }

        match err {
            None => Err(Error::NotFound),
            Some(err) => Err(err),
        }
    }

    #[tracing::instrument(skip_all, fields(
        peer_id = %state.neighbour.peer_id(),
        block_id = %state.block_id,
        kind = ?state.kind,
    ))]
    pub async fn download_persistent_state<W>(
        &self,
        state: PendingPersistentState,
        output: W,
    ) -> Result<W, Error>
    where
        W: Write + Send + 'static,
    {
        tracing::debug!("started");
        scopeguard::defer! {
            tracing::debug!("finished");
        }

        let block_id = state.block_id;

        download_compressed(
            state.size,
            state.chunk_size,
            output,
            |offset| {
                tracing::debug!("downloading persistent state chunk");

                let req = match state.kind {
                    PersistentStateKind::Shard => {
                        Request::from_tl(rpc::GetPersistentShardStateChunk { block_id, offset })
                    }
                    PersistentStateKind::Queue => {
                        Request::from_tl(rpc::GetPersistentQueueStateChunk { block_id, offset })
                    }
                };
                download_with_retries(req, self.overlay_client().clone(), state.neighbour.clone())
            },
            |output, chunk| {
                output.write_all(chunk)?;
                Ok(())
            },
            |mut output| {
                output.flush()?;
                Ok(output)
            },
        )
        .await
    }

    pub async fn find_archive(&self, mc_seqno: u32) -> Result<PendingArchiveResponse, Error> {
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
                return Ok(PendingArchiveResponse::TooNew);
            }

            return match err {
                None => Err(Error::NotFound),
                Some(err) => Err(err),
            };
        };

        tracing::debug!(
            peer_id = %pending_archive.neighbour.peer_id(),
            archive_id = pending_archive.id,
            archive_size = %ByteSize(pending_archive.size.get()),
            archuve_chunk_size = %ByteSize(pending_archive.chunk_size.get() as _),
            "found archive",
        );
        Ok(PendingArchiveResponse::Found(pending_archive))
    }

    #[tracing::instrument(skip_all, fields(
        peer_id = %archive.neighbour.peer_id(),
        archive_id = archive.id,
    ))]
    pub async fn download_archive<W>(&self, archive: PendingArchive, output: W) -> Result<W, Error>
    where
        W: Write + Send + 'static,
    {
        use futures_util::FutureExt;

        tracing::debug!("started");
        scopeguard::defer! {
            tracing::debug!("finished");
        }

        download_compressed(
            archive.size,
            archive.chunk_size,
            (output, ArchiveVerifier::default()),
            |offset| {
                let archive_id = archive.id;
                let neighbour = archive.neighbour.clone();
                let overlay_client = self.overlay_client().clone();

                let started_at = Instant::now();

                tracing::debug!(offset, "downloading archive chunk");
                download_with_retries(
                    Request::from_tl(rpc::GetArchiveChunk { archive_id, offset }),
                    overlay_client,
                    neighbour,
                )
                .map(move |res| {
                    tracing::info!(
                        offset,
                        elapsed = %humantime::format_duration(started_at.elapsed()),
                        "downloaded archive chunk",
                    );
                    res
                })
            },
            |(output, verifier), chunk| {
                verifier.write_verify(chunk)?;
                output.write_all(chunk)?;
                Ok(())
            },
            |(mut output, verifier)| {
                verifier.final_check()?;
                output.flush()?;
                Ok(output)
            },
        )
        .await
    }
}

struct Inner {
    overlay_client: PublicOverlayClient,
    broadcast_listener: Option<Box<dyn SelfBroadcastListener>>,
    min_broadcast_timeout: Duration,
    response_tracker: Mutex<tycho_util::time::RollingP2Estimator>,
}

pub enum PendingArchiveResponse {
    Found(PendingArchive),
    TooNew,
}

#[derive(Clone)]
pub struct PendingArchive {
    pub id: u64,
    pub size: NonZeroU64,
    pub chunk_size: NonZeroU32,
    pub neighbour: Neighbour,
}

#[derive(Clone)]
pub struct PendingPersistentState {
    pub block_id: BlockId,
    pub kind: PersistentStateKind,
    pub size: NonZeroU64,
    pub chunk_size: NonZeroU32,
    pub neighbour: Neighbour,
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

    let span = tracing::Span::current();
    let processing_task = tokio::task::spawn_blocking(move || {
        let _span = span.enter();

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

async fn download_compressed<S, T, DF, DFut, PF, FF>(
    target_size: NonZeroU64,
    chunk_size: NonZeroU32,
    mut state: S,
    mut download_fn: DF,
    mut process_fn: PF,
    finalize_fn: FF,
) -> Result<T, Error>
where
    S: Send + 'static,
    T: Send + 'static,
    DF: FnMut(u64) -> DFut,
    DFut: Future<Output = DownloadedChunkResult> + Send + 'static,
    PF: FnMut(&mut S, &[u8]) -> Result<()> + Send + 'static,
    FF: FnOnce(S) -> Result<T> + Send + 'static,
{
    const PARALLEL_REQUESTS: usize = 10;

    let target_size = target_size.get();
    let chunk_size = chunk_size.get() as usize;

    let (chunks_tx, mut chunks_rx) =
        mpsc::channel::<(QueryResponseHandle, Bytes)>(PARALLEL_REQUESTS);

    let span = tracing::Span::current();
    let processing_task = tokio::task::spawn_blocking(move || {
        let _span = span.enter();

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
                "got chunk"
            );

            anyhow::ensure!(downloaded <= target_size, "received too many chunks");

            decompressed_chunk.clear();
            zstd_decoder.write(chunk.as_ref(), &mut decompressed_chunk)?;

            process_fn(&mut state, &decompressed_chunk)?;

            ScopeGuard::into_inner(guard).accept(); // defuse the guard
        }

        anyhow::ensure!(
            target_size == downloaded,
            "size mismatch (target size: {target_size}; downloaded: {downloaded})",
        );

        finalize_fn(state)
    });

    let mut stream = futures_util::stream::iter((0..target_size).step_by(chunk_size))
        .map(|offset| JoinTask::new(download_fn(offset)))
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

    Ok(output)
}

async fn download_with_retries(
    req: Request,
    overlay_client: PublicOverlayClient,
    neighbour: Neighbour,
) -> DownloadedChunkResult {
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

type DownloadedChunkResult = Result<(QueryResponseHandle, Bytes), Error>;

#[cfg(test)]
mod tests {
    use rand::RngCore;
    use tycho_network::PeerId;
    use tycho_util::compression::zstd_compress;

    use super::*;

    #[tokio::test]
    async fn download_compressed_works() -> Result<()> {
        let neighbour = Neighbour::new(PeerId([0; 32]), u32::MAX, &Duration::from_millis(100));

        let mut original_data = vec![0u8; 1 << 20]; // 1 MB of garbage
        rand::thread_rng().fill_bytes(&mut original_data);

        let mut compressed_data = Vec::new();
        zstd_compress(&original_data, &mut compressed_data, 9);
        let compressed_data = Bytes::from(compressed_data);

        assert_ne!(compressed_data, original_data);

        const CHUNK_SIZE: usize = 128;

        let received = download_compressed(
            NonZeroU64::new(compressed_data.len() as _).unwrap(),
            NonZeroU32::new(CHUNK_SIZE as _).unwrap(),
            Vec::new(),
            |offset| {
                assert_eq!(offset % CHUNK_SIZE as u64, 0);
                assert!(offset < compressed_data.len() as u64);
                let from = offset as usize;
                let to = std::cmp::min(from + CHUNK_SIZE, compressed_data.len());
                let chunk = compressed_data.slice(from..to);
                let handle = QueryResponseHandle::with_roundtrip_ms(neighbour.clone(), 100);
                futures_util::future::ready(Ok((handle, chunk)))
            },
            |result, chunk| {
                result.extend_from_slice(chunk);
                Ok(())
            },
            Ok,
        )
        .await?;
        assert_eq!(received, original_data);

        Ok(())
    }
}
