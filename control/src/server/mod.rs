use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::BlockId;
use futures_util::StreamExt;
use tarpc::context::Context;
use tarpc::server::incoming::Incoming;
use tarpc::server::{self, Channel};
use tarpc::tokio_serde::formats::Json;
use tarpc::{client, context};
use tokio::net::ToSocketAddrs;
use tycho_storage::{KeyBlocksDirection, Storage};

use crate::ControlServer;

#[derive(Clone)]
pub struct ControlServerImpl {
    inner: Arc<Inner>,
}

impl ControlServerImpl {
    fn create(address: SocketAddr, storage: Storage) -> Self {
        let inner = Inner {
            socket_address: address,
            storage,
        };

        Self {
            inner: Arc::new(inner),
        }
    }
    pub async fn serve<A: ToSocketAddrs>(addr: A, storage: Storage) -> Result<()> {
        let mut listener = tarpc::serde_transport::tcp::listen(&addr, Json::default).await?;
        tracing::info!(target:"control", "Cli Tarpc listening on port {}", listener.local_addr().port());
        listener.config_mut().max_frame_length(usize::MAX);
        listener
            // Ignore accept errors.
            .filter_map(|r| futures_util::future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            // Limit channels to 1 per IP.
            .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
            .map(|channel| {
                let server =
                    Self::create(channel.transport().peer_addr().unwrap(), storage.clone());
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
}

struct Inner {
    socket_address: SocketAddr,

    storage: Storage,
}
