use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::*;
use everscale_types::prelude::*;
use indexmap::IndexMap;
use parking_lot::RwLock;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, Notify};
use tycho_consensus::outer_round::{Collator, OuterRound};
use tycho_consensus::{InputBuffer, InputBufferImpl, MempoolConfig, Point};
use tycho_network::{DhtClient, OverlayService, PeerId};
use tycho_storage::MempoolStorage;

use crate::mempool::{
    ExternalMessage, ExternalMessageCache, MempoolAdapter, MempoolAdapterFactory, MempoolAnchor,
    MempoolAnchorId, MempoolEventListener,
};
use crate::tracing_targets;

pub struct MempoolAdapterStdImpl {
    // TODO: replace with rocksdb
    anchors: Arc<RwLock<IndexMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,

    externals_rx: InputBuffer,
    externals_tx: mpsc::UnboundedSender<Bytes>,
    top_known_block_round: OuterRound<Collator>,

    anchor_added: Arc<Notify>,
}

impl MempoolAdapterStdImpl {
    pub fn new() -> Self {
        let anchors = Arc::new(RwLock::new(IndexMap::new()));

        let (externals_tx, externals_rx) = mpsc::unbounded_channel();

        Self {
            anchors,
            externals_tx,
            externals_rx: InputBufferImpl::new(externals_rx),
            top_known_block_round: OuterRound::default(),
            anchor_added: Arc::new(Notify::new()),
        }
    }

    pub fn run(
        self: &Arc<Self>,
        key_pair: Arc<KeyPair>,
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        mempool_storage: &MempoolStorage,
        peers: Vec<PeerId>,
        use_genesis: bool,
    ) {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");

        let (sender, receiver) = mpsc::unbounded_channel();

        let mut engine = tycho_consensus::Engine::new(
            key_pair,
            dht_client,
            overlay_service,
            mempool_storage,
            sender,
            &self.top_known_block_round,
            self.externals_rx.clone(),
        );

        if use_genesis {
            engine.init_with_genesis(&peers);
        }
        tokio::spawn(async move {
            engine.run().await;
        });

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool adapter created");

        tokio::spawn(self.clone().handle_anchors_task(receiver));
    }

    pub fn send_external(&self, message: Bytes) {
        self.externals_tx.send(message).ok();
    }

    async fn handle_anchors_task(self: Arc<Self>, mut rx: UnboundedReceiver<(Point, Vec<Point>)>) {
        let mut cache = ExternalMessageCache::new(MempoolConfig::DEDUPLICATE_ROUNDS);
        while let Some((anchor, points)) = rx.recv().await {
            let anchor_id: MempoolAnchorId = anchor.body().round.0;
            let mut messages = Vec::new();
            let mut total_messages = 0;
            let mut total_bytes = 0;
            let mut messages_bytes = 0;

            for point in points.iter() {
                total_messages += point.body().payload.len();
                'message: for message in &point.body().payload {
                    total_bytes += message.len();
                    let cell = match Boc::decode(message) {
                        Ok(cell) => cell,
                        Err(e) => {
                            tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Failed to deserialize bytes into cell. Error: {e:?}"); // TODO: should handle errors properly?
                            continue 'message;
                        }
                    };

                    let mut slice = match cell.as_slice() {
                        Ok(slice) => slice,
                        Err(e) => {
                            tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Failed to make slice from cell. Error: {e:?}");
                            continue 'message;
                        }
                    };

                    let info = match MsgInfo::load_from(&mut slice) {
                        Ok(MsgInfo::ExtIn(message)) => message,
                        Ok(info) => {
                            tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, ?info, "Bad message. Unexpected message variant");
                            continue 'message;
                        }
                        Err(e) => {
                            tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Bad cell. Failed to deserialize to ExtInMsgInfo. Err: {e:?}");
                            continue 'message;
                        }
                    };

                    if cache.check_unique(anchor_id, cell.repr_hash()) {
                        messages.push(Arc::new(ExternalMessage { cell, info }));
                        messages_bytes += message.len();
                    }
                }
            }

            metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor.body().round.0);
            metrics::counter!("tycho_mempool_externals_count_total").increment(messages.len() as _);
            metrics::counter!("tycho_mempool_externals_bytes_total").increment(messages_bytes as _);
            metrics::counter!("tycho_mempool_duplicates_count_total")
                .increment((total_messages - messages.len()) as _);
            metrics::counter!("tycho_mempool_duplicates_bytes_total")
                .increment((total_bytes - messages_bytes) as _);

            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                round = anchor_id,
                time = anchor.body().time.as_u64(),
                externals_unique = messages.len(),
                externals_skipped = total_messages - messages.len(),
                "new anchor"
            );

            self.add_anchor(Arc::new(MempoolAnchor {
                id: anchor_id,
                chain_time: anchor.body().time.as_u64(),
                author: anchor.body().author,
                externals: messages,
            }));

            cache.clean(anchor_id);
        }
    }

    fn add_anchor(&self, anchor: Arc<MempoolAnchor>) {
        self.anchors.write().insert(anchor.id, anchor);
        self.anchor_added.notify_waiters();
    }
}

impl MempoolAdapterFactory for Arc<MempoolAdapterStdImpl> {
    type Adapter = MempoolAdapterStdImpl;

    fn create(&self, _listener: Arc<dyn MempoolEventListener>) -> Arc<Self::Adapter> {
        self.clone()
    }
}

#[async_trait]
impl MempoolAdapter for MempoolAdapterStdImpl {
    async fn on_new_mc_state(&self, mc_block_id: &BlockId) -> Result<()> {
        // TODO: make real implementation, currently does nothing
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "STUB: New masterchain state (block_id: {}) processing enqueued to mempool",
            mc_block_id.as_short_id(),
        );
        Ok(())
    }

    async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>> {
        // TODO: make real implementation, currently only return anchor from local cache
        Ok(self.anchors.read().get(&anchor_id).cloned())
    }

    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<Arc<MempoolAnchor>> {
        // TODO: make real implementation, currently only return anchor from local cache
        loop {
            // NOTE: Subscribe to notification before checking
            let anchor_added = self.anchor_added.notified();

            {
                let anchors = self.anchors.read();

                match anchors.first() {
                    // Continue to wait for the first anchor on node start
                    None if prev_anchor_id == 0 => {}
                    // Return the first anchor on node start
                    Some((_, first)) if prev_anchor_id == 0 => return Ok(first.clone()),
                    // Trying to get anchor that is too old
                    Some((first_id, _)) if prev_anchor_id < *first_id => {
                        anyhow::bail!("Requested anchor {prev_anchor_id} is too old");
                    }
                    _ => {
                        // Find the index of the previous anchor
                        let Some(index) = anchors.get_index_of(&prev_anchor_id) else {
                            anyhow::bail!("Presented anchor {prev_anchor_id} is unknown");
                        };

                        // Try to get the next anchor
                        if let Some((_, value)) = anchors.get_index(index + 1) {
                            return Ok(value.clone());
                        }
                    }
                }
            }

            anchor_added.await;
        }
    }

    async fn handle_top_processed_to_anchor(&self, anchor_id: u32) -> Result<()> {
        self.top_known_block_round.set_max_raw(anchor_id);
        Ok(())
    }

    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        let mut anchors_cache_rw = self.anchors.write();

        anchors_cache_rw.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }
}
