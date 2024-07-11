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
use tokio::sync::mpsc::UnboundedSender;
use tycho_core::block_strider::{GcTrigger, TriggerTx};
use tycho_storage::{KeyBlocksDirection, Storage};

use crate::models::BlockFull;
use crate::ControlServer;

#[derive(Clone)]
pub struct ControlServerImpl {
    connection_address: SocketAddr,
    inner: Arc<Inner>,
}

impl ControlServerImpl {
    fn create_inner(
        storage: Storage,
        trigger_tx: TriggerTx,
        memory_profiler_trigger: UnboundedSender<bool>,
        memory_profiler_state: Arc<AtomicBool>,
    ) -> Arc<Inner> {
        let inner = Inner {
            storage,
            gc_trigger: trigger_tx,
            memory_profiler_trigger,
            memory_profiler_state,
        };

        Arc::new(inner)
    }
    pub async fn serve<A: ToSocketAddrs>(
        addr: A,
        storage: Storage,
        trigger_tx: TriggerTx,
        memory_profiler_trigger: UnboundedSender<bool>,
        memory_profiler_state: Arc<AtomicBool>,
    ) -> Result<()> {
        let inner = Self::create_inner(
            storage,
            trigger_tx,
            memory_profiler_trigger,
            memory_profiler_state,
        );
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
                let server = ControlServerImpl {
                    connection_address: channel.transport().peer_addr().unwrap(),
                    inner: inner.clone(),
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

impl ControlServer for ControlServerImpl {
    async fn ping(self, _: Context, i: u32) -> u32 {
        i.saturating_add(1)
    }

    async fn trigger_gc(self, _: Context, mc_block_id: BlockId, last_key_block_seqno: u32) -> () {
        self.inner.gc_trigger.send_replace(Some(GcTrigger {
            mc_block_id,
            last_key_block_seqno,
        }));
    }

    async fn trigger_memory_profiler(self, _: Context, set: bool) -> bool {
        let is_active = self.inner.memory_profiler_state.load(Ordering::Acquire);
        if is_active == set {
            tracing::info!("Profiler state has not changed");
            return false;
        }

        if let Err(e) = self.inner.memory_profiler_trigger.send(set) {
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
        let block_handle_storage = self.inner.storage.block_handle_storage();

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
        let block_handle_storage = self.inner.storage.block_handle_storage();
        let block_storage = self.inner.storage.block_storage();

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
}

struct Inner {
    storage: Storage,
    gc_trigger: TriggerTx,

    memory_profiler_trigger: UnboundedSender<bool>,
    memory_profiler_state: Arc<AtomicBool>,
}
