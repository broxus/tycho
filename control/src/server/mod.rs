use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::BlockId;
use futures_util::StreamExt;
use tarpc::context::Context;
use tarpc::server::incoming::Incoming;
use tarpc::server::{self, Channel};
use tarpc::tokio_serde::formats::Json;
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{watch, Notify};
use tycho_core::block_strider::ManualGcTrigger;
use tycho_core::blockchain_rpc::INTERNAL_ERROR_CODE;
use tycho_core::proto::blockchain::{ArchiveInfo, Data};
use tycho_core::proto::overlay;
use tycho_storage::{KeyBlocksDirection, Storage};

use crate::models::BlockFull;
use crate::ControlServer;

#[derive(Clone)]
pub struct ControlServerListener {
    connection_address: SocketAddr,
    server: ControlServerImpl,
}

impl ControlServerListener {
    pub async fn serve<A: ToSocketAddrs>(addr: A, control_server: ControlServerImpl) -> Result<()> {
        let mut listener = tarpc::serde_transport::tcp::listen(&addr, Json::default).await?;
        tracing::info!(target:"control", "Control server is listening on port {}", listener.local_addr().port());
        listener.config_mut().max_frame_length(usize::MAX);
        listener
            // Ignore accept errors.
            .filter_map(|r| futures_util::future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            // Limit channels to 1 per IP.
            .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
            .map(|channel| {
                let server = ControlServerListener {
                    connection_address: channel.transport().peer_addr().unwrap(),
                    server: control_server.clone(),
                };

                channel.execute(server.serve()).for_each(|x| async move {
                    tokio::spawn(x);
                })
            })
            // Max 1 channel.
            .buffer_unordered(1)
            .for_each(|_| async {})
            .await;

        Ok(())
    }
}

#[derive(Clone)]
pub struct ControlServerImpl {
    inner: Arc<Inner>,
}

impl ControlServerImpl {
    pub fn new(storage: Storage) -> Self {
        let (manual_gc_trigger, manual_gc_receiver) = watch::channel(None::<ManualGcTrigger>);
        let (memory_profiler_trigger, memory_profiler_receiver) = watch::channel(false);

        let inner = Inner {
            storage,
            manual_gc_trigger,
            manual_gc_receiver,

            memory_profiler_trigger,
            memory_profiler_receiver,
            memory_profiler_state: Arc::new(AtomicBool::new(false)),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn shared_memory_profiler_state(&self) -> Arc<AtomicBool> {
        self.inner.memory_profiler_state.clone()
    }

    pub fn manual_gc_trigger(&self) -> watch::Sender<Option<ManualGcTrigger>> {
        self.inner.manual_gc_trigger.clone()
    }

    pub fn manual_gc_receiver(&self) -> watch::Receiver<Option<ManualGcTrigger>> {
        self.inner.manual_gc_receiver.clone()
    }

    pub fn profiler_switch(&self) -> watch::Sender<bool> {
        self.inner.memory_profiler_trigger.clone()
    }

    pub fn profiler_receiver(&self) -> watch::Receiver<bool> {
        self.inner.memory_profiler_receiver.clone()
    }
}

impl ControlServer for ControlServerListener {
    async fn ping(self, _: Context, i: u32) -> u32 {
        i.saturating_add(1)
    }

    async fn trigger_gc(self, _: Context, manual_gc_trigger: ManualGcTrigger) -> () {
        self.server
            .inner
            .manual_gc_trigger
            .send_replace(Some(manual_gc_trigger));
    }

    async fn trigger_memory_profiler(self, _: Context, set: bool) -> bool {
        let is_active = self
            .server
            .inner
            .memory_profiler_state
            .load(Ordering::Acquire);
        if is_active == set {
            tracing::info!("Profiler state has not changed");
            return false;
        }

        if let Err(e) = self.server.inner.memory_profiler_trigger.send(set) {
            tracing::error!("Failed to change memory profiler state. {e:?}");
            return false;
        }

        return true;
    }

    async fn get_next_key_blocks_ids(
        self,
        _: Context,
        block_id: BlockId,
        max_size: usize,
    ) -> Option<Vec<BlockId>> {
        let block_handle_storage = self.server.inner.storage.block_handle_storage();

        if !block_id.shard.is_masterchain() {
            tracing::error!("first block id is not from masterchain");
            return None;
        }

        let mut iterator = block_handle_storage
            .key_blocks_iterator(KeyBlocksDirection::ForwardFrom(block_id.seqno))
            .take(max_size);

        if let Some(id) = iterator.next() {
            if id.root_hash != block_id.root_hash {
                tracing::error!("first block root hash mismatch");
                return None;
            }

            if id.file_hash != block_id.file_hash {
                tracing::error!("first block file hash mismatch");
                return None;
            }
        };

        Some(iterator.take(max_size).collect::<Vec<_>>())
    }

    async fn get_block_full(self, _: Context, block_id: BlockId) -> Option<BlockFull> {
        let block_handle_storage = self.server.inner.storage.block_handle_storage();
        let block_storage = self.server.inner.storage.block_storage();

        let mut is_link = false;
        match block_handle_storage.load_handle(&block_id) {
            Some(handle) if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) => {
                let block = block_storage.load_block_data_raw(&handle).await;
                let proof = block_storage.load_block_proof_raw(&handle, is_link).await;
                match (block, proof) {
                    (Ok(block), Ok(proof)) => Some(BlockFull {
                        id: block_id,
                        block,
                        proof,
                        is_link,
                    }),
                    _ => None,
                }
            }
            _ => {
                tracing::error!("Found block empty {}\n", &block_id);
                None
            }
        }
    }

    async fn get_archive_info(self, _: Context, mc_seqno: u32) -> Option<u32> {
        let node_state = self.server.inner.storage.node_state();

        match node_state.load_last_mc_block_id() {
            Some(last_applied_mc_block) => {
                if mc_seqno > last_applied_mc_block.seqno {
                    return None;
                }

                let block_storage =  self.server.inner.storage.block_storage();

                match block_storage.get_archive_id(mc_seqno) {
                    Some(id) => Some(id),
                    None => None,
                }
            }
            None => {
                tracing::warn!("get_archive_id failed: no blocks applied");
                None
            }
        }
    }

    async fn get_archive_slice(self, _: Context, id: u32, limit: u32, offset: u64) -> Option<Vec<u8>> {
        let block_storage = self.server.inner.storage.block_storage();

        let archive_res = block_storage.get_archive_slice(
            id,
            limit as usize,
            offset as usize,
        );

        match archive_res {
            Ok(Some(data)) => Some(data),
            _ => {
                tracing::warn!("get_archive_slice failed. Archive not found.");
                None
            }
        }
    }
}

struct Inner {
    storage: Storage,
    manual_gc_trigger: watch::Sender<Option<ManualGcTrigger>>,
    manual_gc_receiver: watch::Receiver<Option<ManualGcTrigger>>,

    memory_profiler_trigger: watch::Sender<bool>,
    memory_profiler_receiver: watch::Receiver<bool>,
    memory_profiler_state: Arc<AtomicBool>,
}
