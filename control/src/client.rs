use std::io::Write;
use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use tarpc::tokio_serde::formats::Bincode;
use tarpc::{client, context};
use tokio::sync::mpsc;
use tracing::Instrument;
use tycho_types::boc::{Boc, BocRepr};
use tycho_types::cell::{DynCell, HashBytes};
use tycho_types::models::{BlockId, BlockIdShort, OwnedMessage, StdAddr};
use tycho_util::compression::ZstdDecompressStream;
use tycho_util::futures::JoinTask;

use crate::error::{ClientError, ClientResult};
use crate::mempool;
use crate::proto::*;

pub struct ControlClient {
    inner: ControlServerClient,
}

impl ControlClient {
    pub async fn connect<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let mut connect = tarpc::serde_transport::unix::connect(path, Bincode::default);
        connect.config_mut().max_frame_length(usize::MAX);
        let transport = connect.await?;

        let inner = ControlServerClient::new(client::Config::default(), transport).spawn();

        Ok(Self { inner })
    }

    pub async fn ping(&self) -> ClientResult<u64> {
        self.inner.ping(current_context()).await.map_err(Into::into)
    }

    pub async fn get_status(&self) -> ClientResult<NodeStatusResponse> {
        self.inner
            .get_status(current_context())
            .await?
            .map_err(Into::into)
    }

    pub async fn trigger_archives_gc(&self, req: TriggerGcRequest) -> ClientResult<()> {
        self.inner
            .trigger_archives_gc(current_context(), req)
            .await
            .map_err(Into::into)
    }

    pub async fn trigger_blocks_gc(&self, req: TriggerGcRequest) -> ClientResult<()> {
        self.inner
            .trigger_blocks_gc(current_context(), req)
            .await
            .map_err(Into::into)
    }

    pub async fn trigger_states_gc(&self, req: TriggerGcRequest) -> ClientResult<()> {
        self.inner
            .trigger_states_gc(current_context(), req)
            .await
            .map_err(Into::into)
    }

    pub async fn trigger_compaction(&self, req: TriggerCompactionRequest) -> ClientResult<()> {
        self.inner
            .trigger_compaction(current_context(), req)
            .await
            .map_err(Into::into)
    }

    pub async fn set_memory_profiler_enabled(&self, enabled: bool) -> ClientResult<bool> {
        self.inner
            .set_memory_profiler_enabled(current_context(), enabled)
            .await
            .map_err(Into::into)
    }

    pub async fn dump_memory_profiler(&self) -> ClientResult<Vec<u8>> {
        self.inner
            .dump_memory_profiler(current_context())
            .await?
            .map_err(Into::into)
    }

    pub async fn get_neighbours_info(&self) -> ClientResult<NeighboursInfoResponse> {
        self.inner
            .get_neighbours_info(current_context())
            .await?
            .map_err(Into::into)
    }

    pub async fn broadcast_external_message(&self, message: OwnedMessage) -> ClientResult<()> {
        if !message.info.is_external_in() {
            return Err(ClientError::ClientFailed(anyhow::anyhow!(
                "expected an ExtIn message"
            )));
        }

        let message = BocRepr::encode(message)
            .map_err(|e| ClientError::ClientFailed(e.into()))?
            .into();

        self.inner
            .broadcast_external_message(current_context(), BroadcastExtMsgRequest { message })
            .await?
            .map_err(Into::into)
    }

    pub async fn broadcast_external_message_raw(&self, message: &DynCell) -> ClientResult<()> {
        let message = Boc::encode(message).into();
        self.inner
            .broadcast_external_message(current_context(), BroadcastExtMsgRequest { message })
            .await?
            .map_err(Into::into)
    }

    pub async fn get_account_state(&self, addr: &StdAddr) -> ClientResult<AccountStateResponse> {
        self.inner
            .get_account_state(current_context(), AccountStateRequest {
                address: addr.clone(),
            })
            .await?
            .map_err(Into::into)
    }

    pub async fn get_blockchain_config(&self) -> ClientResult<BlockchainConfigResponse> {
        self.inner
            .get_blockchain_config(current_context())
            .await?
            .map_err(Into::into)
    }

    pub async fn get_block(&self, block_id: &BlockId) -> ClientResult<Option<Bytes>> {
        let req = BlockRequest {
            block_id: *block_id,
        };
        match self.inner.get_block(current_context(), req).await?? {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }

    pub async fn get_block_proof(&self, block_id: &BlockId) -> ClientResult<Option<Bytes>> {
        let req = BlockRequest {
            block_id: *block_id,
        };

        match self.inner.get_block_proof(current_context(), req).await?? {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }

    pub async fn get_queue_diff(&self, block_id: &BlockId) -> ClientResult<Option<Bytes>> {
        let req = BlockRequest {
            block_id: *block_id,
        };

        match self.inner.get_queue_diff(current_context(), req).await?? {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }

    pub async fn find_archive(&self, mc_seqno: u32) -> ClientResult<Option<ArchiveInfo>> {
        match self
            .inner
            .get_archive_info(current_context(), ArchiveInfoRequest { mc_seqno })
            .await??
        {
            ArchiveInfoResponse::Found(info) => Ok(Some(info)),
            ArchiveInfoResponse::TooNew | ArchiveInfoResponse::NotFound => Ok(None),
        }
    }

    pub async fn download_archive<W>(
        &self,
        info: ArchiveInfo,
        decompress: bool,
        mut output: W,
    ) -> ClientResult<W>
    where
        W: Write + Send + 'static,
    {
        const PARALLEL_CHUNKS: usize = 10;

        let target_size = info.size.get();
        let chunk_size = info.chunk_size.get() as u64;
        let chunk_num = info.size.get().div_ceil(chunk_size);

        let (chunks_tx, mut chunks_rx) = mpsc::channel::<ArchiveSliceResponse>(PARALLEL_CHUNKS);

        let processing_task = tokio::task::spawn_blocking(move || {
            let mut zstd_decoder = ZstdDecompressStream::new(chunk_size as _)?;

            let mut decompressed_chunk = Vec::new();
            let mut downloaded = 0;
            while let Some(res) = chunks_rx.blocking_recv() {
                downloaded += res.data.len() as u64;

                output.write_all(if decompress {
                    decompressed_chunk.clear();
                    zstd_decoder.write(&res.data, &mut decompressed_chunk)?;
                    &decompressed_chunk
                } else {
                    &res.data
                })?;
            }

            anyhow::ensure!(downloaded == target_size, "downloaded size mismatch");

            output.flush()?;
            Ok(output)
        });

        let mut chunks = futures_util::stream::iter(0..chunk_num)
            .map(|chunk| {
                let req = ArchiveSliceRequest {
                    archive_id: info.id,
                    offset: chunk * chunk_size,
                };

                let span = tracing::debug_span!("get_archive_chunk", ?req);

                let client = self.inner.clone();
                JoinTask::new(
                    async move {
                        tracing::debug!("started");
                        let res = client.get_archive_chunk(current_context(), req).await;
                        tracing::debug!("finished");
                        res
                    }
                    .instrument(span),
                )
            })
            .buffered(PARALLEL_CHUNKS);

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk??;
            if chunks_tx.send(chunk).await.is_err() {
                break;
            }
        }

        drop(chunks_tx);

        processing_task
            .await
            .map_err(|e| {
                ClientError::ClientFailed(anyhow::anyhow!("Failed to join blocking task: {e}"))
            })?
            .map_err(ClientError::ClientFailed)
    }

    pub async fn list_archives(&self) -> ClientResult<Vec<ArchiveInfo>> {
        self.inner
            .get_archive_ids(current_context())
            .await?
            .map_err(Into::into)
    }

    pub async fn list_blocks(
        &self,
        continuation: Option<BlockIdShort>,
    ) -> ClientResult<BlockListResponse> {
        self.inner
            .get_block_ids(current_context(), BlockListRequest { continuation })
            .await?
            .map_err(Into::into)
    }

    pub async fn get_overlay_ids(&self) -> ClientResult<OverlayIdsResponse> {
        self.inner
            .get_overlay_ids(current_context())
            .await?
            .map_err(Into::into)
    }

    pub async fn get_overlay_peers(
        &self,
        overaly_id: &HashBytes,
    ) -> ClientResult<OverlayPeersResponse> {
        self.inner
            .get_overlay_peers(current_context(), OverlayPeersRequest {
                overlay_id: *overaly_id,
            })
            .await?
            .map_err(Into::into)
    }

    pub async fn dht_find_node(
        &self,
        key: &HashBytes,
        k: u32,
        peer_id: Option<&HashBytes>,
    ) -> ClientResult<Vec<DhtFindNodeResponseItem>> {
        self.inner
            .dht_find_node(current_context(), DhtFindNodeRequest {
                peer_id: peer_id.copied(),
                key: *key,
                k,
            })
            .await?
            .map_err(Into::into)
            .map(|res| res.nodes)
    }

    pub async fn mempool_dump_bans(&self) -> ClientResult<Vec<mempool::DumpBansItem>> {
        self.inner
            .mempool_dump_bans(current_context())
            .await?
            .map_err(Into::into)
    }

    pub async fn mempool_dump_events(
        &self,
        peer_id: Option<&HashBytes>,
        pretty: bool,
    ) -> ClientResult<String> {
        self.inner
            .mempool_dump_events(current_context(), mempool::DumpEventsRequest {
                peer_id: peer_id.copied(),
                pretty,
            })
            .await?
            .map_err(Into::into)
    }

    pub async fn mempool_ban(
        &self,
        peer_id: &HashBytes,
        duration: Duration,
        pretty: bool,
    ) -> ClientResult<String> {
        self.inner
            .mempool_ban(current_context(), mempool::BanRequest {
                peer_id: *peer_id,
                duration,
                pretty,
            })
            .await?
            .map_err(Into::into)
    }

    pub async fn mempool_unban(&self, peer_id: &HashBytes) -> ClientResult<()> {
        self.inner
            .mempool_unban(current_context(), *peer_id)
            .await?
            .map_err(Into::into)
    }

    pub async fn mempool_list_events(
        &self,
        count: u16,
        page: u32,
        asc: bool,
        with_ids: bool,
    ) -> ClientResult<Vec<mempool::MempoolEventDisplay>> {
        self.inner
            .mempool_list_events(current_context(), mempool::ListEventsRequest {
                count,
                page,
                asc,
                with_ids,
            })
            .await?
            .map_err(Into::into)
    }

    pub async fn mempool_delete_events(&self, since: u64, until: u64) -> ClientResult<()> {
        self.inner
            .mempool_delete_events(current_context(), since..until)
            .await?
            .map_err(Into::into)
    }

    pub async fn mempool_get_event_point(
        &self,
        round: u32,
        digest: HashBytes,
    ) -> ClientResult<Bytes> {
        self.inner
            .mempool_get_event_point(current_context(), mempool::PointKey(round, digest))
            .await?
            .map_err(Into::into)
    }
}

// sets a 10-minute deadline on the context instead of default 10 seconds
fn current_context() -> context::Context {
    use std::time::{Duration, Instant};

    let mut context = context::current();
    context.deadline = Instant::now() + Duration::from_secs(600);
    context
}
