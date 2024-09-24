use std::io::Write;
use std::path::Path;

use everscale_types::models::BlockId;
use futures_util::StreamExt;
use tarpc::tokio_serde::formats::Bincode;
use tarpc::{client, context};
use tokio::sync::mpsc;
use tracing::Instrument;
use tycho_core::block_strider::ManualGcTrigger;
use tycho_util::compression::ZstdDecompressStream;
use tycho_util::futures::JoinTask;

use crate::error::{ClientError, ClientResult};
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
        self.inner
            .ping(context::current())
            .await
            .map_err(Into::into)
    }

    pub async fn trigger_archives_gc(&self, trigger: ManualGcTrigger) -> ClientResult<()> {
        self.inner
            .trigger_archives_gc(context::current(), trigger)
            .await
            .map_err(Into::into)
    }

    pub async fn trigger_blocks_gc(&self, trigger: ManualGcTrigger) -> ClientResult<()> {
        self.inner
            .trigger_blocks_gc(context::current(), trigger)
            .await
            .map_err(Into::into)
    }

    pub async fn trigger_states_gc(&self, trigger: ManualGcTrigger) -> ClientResult<()> {
        self.inner
            .trigger_states_gc(context::current(), trigger)
            .await
            .map_err(Into::into)
    }

    pub async fn set_memory_profiler_enabled(&self, enabled: bool) -> ClientResult<bool> {
        self.inner
            .set_memory_profiler_enabled(context::current(), enabled)
            .await
            .map_err(Into::into)
    }

    pub async fn dump_memory_profiler(&self) -> ClientResult<Vec<u8>> {
        self.inner
            .dump_memory_profiler(context::current())
            .await?
            .map_err(Into::into)
    }

    pub async fn get_block(&self, block_id: &BlockId) -> ClientResult<Option<Vec<u8>>> {
        let req = BlockRequest {
            block_id: *block_id,
        };
        match self.inner.get_block(context::current(), req).await?? {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }

    pub async fn get_block_proof(&self, block_id: &BlockId) -> ClientResult<Option<Vec<u8>>> {
        let req = BlockRequest {
            block_id: *block_id,
        };

        match self
            .inner
            .get_block_proof(context::current(), req)
            .await??
        {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }

    pub async fn get_queue_diff(&self, block_id: &BlockId) -> ClientResult<Option<Vec<u8>>> {
        let req = BlockRequest {
            block_id: *block_id,
        };

        match self.inner.get_queue_diff(context::current(), req).await?? {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }

    pub async fn find_archive(&self, mc_seqno: u32) -> ClientResult<Option<ArchiveInfo>> {
        match self
            .inner
            .get_archive_info(context::current(), ArchiveInfoRequest { mc_seqno })
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
        let chunk_num = (info.size.get() + chunk_size - 1) / chunk_size;

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
                        let res = client.get_archive_chunk(context::current(), req).await;
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
            .get_archive_ids(context::current())
            .await?
            .map_err(Into::into)
    }

    pub async fn list_blocks(&self, limit: u32, offset: u32) -> ClientResult<Vec<BlockId>> {
        self.inner
            .get_block_ids(context::current(), BlockListRequest { limit, offset })
            .await?
            .map_err(Into::into)
    }
}
