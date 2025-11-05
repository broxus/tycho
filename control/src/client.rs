use std::io::Write;
use std::path::Path;
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
use crate::proto::*;
pub struct ControlClient {
    inner: ControlServerClient,
}
impl ControlClient {
    pub async fn connect<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(connect)),
            file!(),
            24u32,
        );
        let path = path;
        let mut connect = tarpc::serde_transport::unix::connect(path, Bincode::default);
        connect.config_mut().max_frame_length(usize::MAX);
        let transport = {
            __guard.end_section(27u32);
            let __result = connect.await;
            __guard.start_section(27u32);
            __result
        }?;
        let inner = ControlServerClient::new(client::Config::default(), transport)
            .spawn();
        Ok(Self { inner })
    }
    pub async fn ping(&self) -> ClientResult<u64> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(ping)),
            file!(),
            34u32,
        );
        {
            __guard.end_section(35u32);
            let __result = self.inner.ping(current_context()).await;
            __guard.start_section(35u32);
            __result
        }
            .map_err(Into::into)
    }
    pub async fn get_status(&self) -> ClientResult<NodeStatusResponse> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_status)),
            file!(),
            38u32,
        );
        {
            __guard.end_section(41u32);
            let __result = self.inner.get_status(current_context()).await;
            __guard.start_section(41u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn trigger_archives_gc(&self, req: TriggerGcRequest) -> ClientResult<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(trigger_archives_gc)),
            file!(),
            45u32,
        );
        let req = req;
        {
            __guard.end_section(48u32);
            let __result = self.inner.trigger_archives_gc(current_context(), req).await;
            __guard.start_section(48u32);
            __result
        }
            .map_err(Into::into)
    }
    pub async fn trigger_blocks_gc(&self, req: TriggerGcRequest) -> ClientResult<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(trigger_blocks_gc)),
            file!(),
            52u32,
        );
        let req = req;
        {
            __guard.end_section(55u32);
            let __result = self.inner.trigger_blocks_gc(current_context(), req).await;
            __guard.start_section(55u32);
            __result
        }
            .map_err(Into::into)
    }
    pub async fn trigger_states_gc(&self, req: TriggerGcRequest) -> ClientResult<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(trigger_states_gc)),
            file!(),
            59u32,
        );
        let req = req;
        {
            __guard.end_section(62u32);
            let __result = self.inner.trigger_states_gc(current_context(), req).await;
            __guard.start_section(62u32);
            __result
        }
            .map_err(Into::into)
    }
    pub async fn trigger_compaction(
        &self,
        req: TriggerCompactionRequest,
    ) -> ClientResult<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(trigger_compaction)),
            file!(),
            66u32,
        );
        let req = req;
        {
            __guard.end_section(69u32);
            let __result = self.inner.trigger_compaction(current_context(), req).await;
            __guard.start_section(69u32);
            __result
        }
            .map_err(Into::into)
    }
    pub async fn set_memory_profiler_enabled(
        &self,
        enabled: bool,
    ) -> ClientResult<bool> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(set_memory_profiler_enabled)),
            file!(),
            73u32,
        );
        let enabled = enabled;
        {
            __guard.end_section(76u32);
            let __result = self
                .inner
                .set_memory_profiler_enabled(current_context(), enabled)
                .await;
            __guard.start_section(76u32);
            __result
        }
            .map_err(Into::into)
    }
    pub async fn dump_memory_profiler(&self) -> ClientResult<Vec<u8>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(dump_memory_profiler)),
            file!(),
            80u32,
        );
        {
            __guard.end_section(83u32);
            let __result = self.inner.dump_memory_profiler(current_context()).await;
            __guard.start_section(83u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn get_neighbours_info(&self) -> ClientResult<NeighboursInfoResponse> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_neighbours_info)),
            file!(),
            87u32,
        );
        {
            __guard.end_section(90u32);
            let __result = self.inner.get_neighbours_info(current_context()).await;
            __guard.start_section(90u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn broadcast_external_message(
        &self,
        message: OwnedMessage,
    ) -> ClientResult<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(broadcast_external_message)),
            file!(),
            94u32,
        );
        let message = message;
        if !message.info.is_external_in() {
            {
                __guard.end_section(96u32);
                return Err(
                    ClientError::ClientFailed(
                        anyhow::anyhow!("expected an ExtIn message"),
                    ),
                );
            };
        }
        let message = BocRepr::encode(message)
            .map_err(|e| ClientError::ClientFailed(e.into()))?
            .into();
        {
            __guard.end_section(107u32);
            let __result = self
                .inner
                .broadcast_external_message(
                    current_context(),
                    BroadcastExtMsgRequest { message },
                )
                .await;
            __guard.start_section(107u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn broadcast_external_message_raw(
        &self,
        message: &DynCell,
    ) -> ClientResult<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(broadcast_external_message_raw)),
            file!(),
            111u32,
        );
        let message = message;
        let message = Boc::encode(message).into();
        {
            __guard.end_section(115u32);
            let __result = self
                .inner
                .broadcast_external_message(
                    current_context(),
                    BroadcastExtMsgRequest { message },
                )
                .await;
            __guard.start_section(115u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn get_account_state(
        &self,
        addr: &StdAddr,
    ) -> ClientResult<AccountStateResponse> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_account_state)),
            file!(),
            119u32,
        );
        let addr = addr;
        {
            __guard.end_section(124u32);
            let __result = self
                .inner
                .get_account_state(
                    current_context(),
                    AccountStateRequest {
                        address: addr.clone(),
                    },
                )
                .await;
            __guard.start_section(124u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn get_blockchain_config(&self) -> ClientResult<BlockchainConfigResponse> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_blockchain_config)),
            file!(),
            128u32,
        );
        {
            __guard.end_section(131u32);
            let __result = self.inner.get_blockchain_config(current_context()).await;
            __guard.start_section(131u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn get_block(&self, block_id: &BlockId) -> ClientResult<Option<Bytes>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_block)),
            file!(),
            135u32,
        );
        let block_id = block_id;
        let req = BlockRequest {
            block_id: *block_id,
        };
        match {
            __guard.end_section(139u32);
            let __result = self.inner.get_block(current_context(), req).await;
            __guard.start_section(139u32);
            __result
        }?? {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }
    pub async fn get_block_proof(
        &self,
        block_id: &BlockId,
    ) -> ClientResult<Option<Bytes>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_block_proof)),
            file!(),
            145u32,
        );
        let block_id = block_id;
        let req = BlockRequest {
            block_id: *block_id,
        };
        match {
            __guard.end_section(150u32);
            let __result = self.inner.get_block_proof(current_context(), req).await;
            __guard.start_section(150u32);
            __result
        }?? {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }
    pub async fn get_queue_diff(
        &self,
        block_id: &BlockId,
    ) -> ClientResult<Option<Bytes>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_queue_diff)),
            file!(),
            156u32,
        );
        let block_id = block_id;
        let req = BlockRequest {
            block_id: *block_id,
        };
        match {
            __guard.end_section(161u32);
            let __result = self.inner.get_queue_diff(current_context(), req).await;
            __guard.start_section(161u32);
            __result
        }?? {
            BlockResponse::Found { data } => Ok(Some(data)),
            BlockResponse::NotFound => Ok(None),
        }
    }
    pub async fn find_archive(
        &self,
        mc_seqno: u32,
    ) -> ClientResult<Option<ArchiveInfo>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(find_archive)),
            file!(),
            167u32,
        );
        let mc_seqno = mc_seqno;
        match {
            __guard.end_section(171u32);
            let __result = self
                .inner
                .get_archive_info(current_context(), ArchiveInfoRequest { mc_seqno })
                .await;
            __guard.start_section(171u32);
            __result
        }?? {
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(download_archive)),
            file!(),
            186u32,
        );
        let info = info;
        let decompress = decompress;
        let mut output = output;
        const PARALLEL_CHUNKS: usize = 10;
        let target_size = info.size.get();
        let chunk_size = info.chunk_size.get() as u64;
        let chunk_num = info.size.get().div_ceil(chunk_size);
        let (chunks_tx, mut chunks_rx) = mpsc::channel::<
            ArchiveSliceResponse,
        >(PARALLEL_CHUNKS);
        let processing_task = tokio::task::spawn_blocking(move || {
            let mut zstd_decoder = ZstdDecompressStream::new(chunk_size as _)?;
            let mut decompressed_chunk = Vec::new();
            let mut downloaded = 0;
            while let Some(res) = chunks_rx.blocking_recv() {
                downloaded += res.data.len() as u64;
                output
                    .write_all(
                        if decompress {
                            decompressed_chunk.clear();
                            zstd_decoder.write(&res.data, &mut decompressed_chunk)?;
                            &decompressed_chunk
                        } else {
                            &res.data
                        },
                    )?;
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
                let span = tracing::debug_span!("get_archive_chunk", ? req);
                let client = self.inner.clone();
                JoinTask::new(
                    async move {
                        let mut __guard = crate::__async_profile_guard__::Guard::new(
                            concat!(module_path!(), "::async_block"),
                            file!(),
                            229u32,
                        );
                        tracing::debug!("started");
                        let res = {
                            __guard.end_section(231u32);
                            let __result = client
                                .get_archive_chunk(current_context(), req)
                                .await;
                            __guard.start_section(231u32);
                            __result
                        };
                        tracing::debug!("finished");
                        res
                    }
                        .instrument(span),
                )
            })
            .buffered(PARALLEL_CHUNKS);
        while let Some(chunk) = {
            __guard.end_section(240u32);
            let __result = chunks.next().await;
            __guard.start_section(240u32);
            __result
        } {
            __guard.checkpoint(240u32);
            let chunk = chunk??;
            if {
                __guard.end_section(242u32);
                let __result = chunks_tx.send(chunk).await;
                __guard.start_section(242u32);
                __result
            }
                .is_err()
            {
                {
                    __guard.end_section(243u32);
                    __guard.start_section(243u32);
                    break;
                };
            }
        }
        drop(chunks_tx);
        {
            __guard.end_section(250u32);
            let __result = processing_task.await;
            __guard.start_section(250u32);
            __result
        }
            .map_err(|e| {
                ClientError::ClientFailed(
                    anyhow::anyhow!("Failed to join blocking task: {e}"),
                )
            })?
            .map_err(ClientError::ClientFailed)
    }
    pub async fn list_archives(&self) -> ClientResult<Vec<ArchiveInfo>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(list_archives)),
            file!(),
            257u32,
        );
        {
            __guard.end_section(260u32);
            let __result = self.inner.get_archive_ids(current_context()).await;
            __guard.start_section(260u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn list_blocks(
        &self,
        continuation: Option<BlockIdShort>,
    ) -> ClientResult<BlockListResponse> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(list_blocks)),
            file!(),
            267u32,
        );
        let continuation = continuation;
        {
            __guard.end_section(270u32);
            let __result = self
                .inner
                .get_block_ids(current_context(), BlockListRequest { continuation })
                .await;
            __guard.start_section(270u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn get_overlay_ids(&self) -> ClientResult<OverlayIdsResponse> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_overlay_ids)),
            file!(),
            274u32,
        );
        {
            __guard.end_section(277u32);
            let __result = self.inner.get_overlay_ids(current_context()).await;
            __guard.start_section(277u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn get_overlay_peers(
        &self,
        overaly_id: &HashBytes,
    ) -> ClientResult<OverlayPeersResponse> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_overlay_peers)),
            file!(),
            284u32,
        );
        let overaly_id = overaly_id;
        {
            __guard.end_section(289u32);
            let __result = self
                .inner
                .get_overlay_peers(
                    current_context(),
                    OverlayPeersRequest {
                        overlay_id: *overaly_id,
                    },
                )
                .await;
            __guard.start_section(289u32);
            __result
        }?
            .map_err(Into::into)
    }
    pub async fn dht_find_node(
        &self,
        key: &HashBytes,
        k: u32,
        peer_id: Option<&HashBytes>,
    ) -> ClientResult<Vec<DhtFindNodeResponseItem>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(dht_find_node)),
            file!(),
            298u32,
        );
        let key = key;
        let k = k;
        let peer_id = peer_id;
        {
            __guard.end_section(305u32);
            let __result = self
                .inner
                .dht_find_node(
                    current_context(),
                    DhtFindNodeRequest {
                        peer_id: peer_id.copied(),
                        key: *key,
                        k,
                    },
                )
                .await;
            __guard.start_section(305u32);
            __result
        }?
            .map_err(Into::into)
            .map(|res| res.nodes)
    }
}
fn current_context() -> context::Context {
    use std::time::{Duration, Instant};
    let mut context = context::current();
    context.deadline = Instant::now() + Duration::from_secs(600);
    context
}
