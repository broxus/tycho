use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::*;
use everscale_types::prelude::*;
use indexmap::IndexMap;
use parking_lot::RwLock;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, Notify};
use tycho_consensus::prelude::*;
use tycho_network::{Network, OverlayService, PeerId, PeerResolver};
use tycho_storage::MempoolStorage;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;
use tycho_util::time::now_millis;

use crate::mempool::deduplicator::Deduplicator;
use crate::mempool::{
    ExternalMessage, MempoolAdapter, MempoolAdapterFactory, MempoolAnchor, MempoolAnchorId,
    MempoolEventListener,
};
use crate::tracing_targets;

pub struct MempoolAdapterStdImpl {
    // TODO: replace with rocksdb
    anchors: Arc<RwLock<IndexMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
    store: MempoolAdapterStore,

    externals_rx: InputBuffer,
    externals_tx: mpsc::UnboundedSender<Bytes>,
    top_known_block_round: OuterRound<Collator>,

    anchor_added: Arc<Notify>,
}

impl MempoolAdapterStdImpl {
    pub fn new(mempool_storage: &MempoolStorage) -> Self {
        let anchors = Arc::new(RwLock::new(IndexMap::new()));

        let (externals_tx, externals_rx) = mpsc::unbounded_channel();

        Self {
            anchors,
            store: MempoolAdapterStore::new(mempool_storage.clone(), OuterRound::default()),
            externals_tx,
            externals_rx: InputBuffer::new(externals_rx),
            top_known_block_round: OuterRound::default(),
            anchor_added: Arc::new(Notify::new()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        self: &Arc<Self>,
        key_pair: Arc<KeyPair>,
        network: &Network,
        peer_resolver: &PeerResolver,
        overlay_service: &OverlayService,
        peers: Vec<PeerId>,
        _use_genesis: bool,
        mempool_start_round: Option<u32>,
    ) {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");

        let (sender, receiver) = mpsc::unbounded_channel();

        let mut engine = Engine::new(
            key_pair,
            network,
            peer_resolver,
            overlay_service,
            &self.store,
            self.externals_rx.clone(),
            sender,
            &self.top_known_block_round,
            mempool_start_round,
        );

        // if use_genesis {
        engine.init_with_genesis(&peers);
        // }
        tokio::spawn(async move {
            engine.run().await;
        });

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool adapter created");

        tokio::spawn(self.clone().handle_anchors_task(receiver));
    }

    pub fn send_external(&self, message: Bytes) {
        self.externals_tx.send(message).ok();
    }

    async fn handle_anchors_task(
        self: Arc<Self>,
        mut rx: UnboundedReceiver<(PointInfo, Vec<PointInfo>)>,
    ) {
        let mut deduplicator = Deduplicator::new(MempoolConfig::DEDUPLICATE_ROUNDS);
        while let Some((anchor, history)) = rx.recv().await {
            let author = anchor.data().author;
            let chain_time = anchor.data().time.as_u64();
            let anchor_id: MempoolAnchorId = anchor.round().0;
            metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor_id);

            metrics::histogram!("tycho_mempool_commit_anchor_latency_time").record(
                Duration::from_millis(now_millis().max(chain_time) - chain_time).as_secs_f64(),
            );

            let task = tokio::task::spawn_blocking({
                let store = self.store.clone();
                let anchor = anchor.clone();
                move || {
                    // may skip expand part, but never skip set committed part;
                    let points = store.expand_anchor_history(&history);
                    // set committed only after point data is read or skipped
                    store.set_committed(&anchor, &history);
                    points
                }
            });
            let points = task.await.expect("expand anchor history task failed");

            let (unique_messages, mut moved_dedup) =
                rayon_run(move || Self::handle_anchor(anchor, points, deduplicator)).await;

            self.add_anchor(Arc::new(MempoolAnchor {
                id: anchor_id,
                chain_time,
                author,
                externals: unique_messages,
            }));

            deduplicator = rayon_run(move || {
                moved_dedup.clean(anchor_id);
                moved_dedup
            })
            .await;
        }
    }

    fn handle_anchor(
        anchor: PointInfo,
        payloads: Vec<Bytes>,
        mut deduplicator: Deduplicator,
    ) -> (Vec<Arc<ExternalMessage>>, Deduplicator) {
        let _guard = HistogramGuard::begin("tycho_mempool_adapter_parse_anchor_history_time");

        let anchor_id: MempoolAnchorId = anchor.round().0;

        let total_messages = payloads.len();
        let total_bytes: usize = payloads.iter().fold(0, |acc, bytes| acc + bytes.len());
        metrics::counter!("tycho_mempool_externals_count_total").increment(total_messages as _);
        metrics::counter!("tycho_mempool_externals_bytes_total").increment(total_bytes as _);

        let all_messages = payloads
            .into_par_iter()
            .filter_map(|bytes| Self::parse_message_bytes(&bytes).map(|cell| (cell, bytes.len())))
            .collect::<Vec<_>>();

        let mut unique_messages_bytes = 0;
        let unique_messages = all_messages
            .into_iter()
            .filter(|(message, _)| deduplicator.check_unique(anchor_id, message.cell.repr_hash()))
            .map(|(message, byte_len)| {
                unique_messages_bytes += byte_len;
                message
            })
            .collect::<Vec<_>>();

        metrics::counter!("tycho_mempool_duplicates_count_total")
            .increment((total_messages - unique_messages.len()) as _);
        metrics::counter!("tycho_mempool_duplicates_bytes_total")
            .increment((total_bytes - unique_messages_bytes) as _);

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            round = anchor_id,
            time = anchor.data().time.as_u64(),
            externals_unique = unique_messages.len(),
            externals_skipped = total_messages - unique_messages.len(),
            "new anchor"
        );
        (unique_messages, deduplicator)
    }

    fn parse_message_bytes(message: &Bytes) -> Option<Arc<ExternalMessage>> {
        let cell = match Boc::decode(message) {
            Ok(cell) => cell,
            Err(e) => {
                // TODO: should handle errors properly?
                tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Failed to deserialize bytes into cell. Error: {e:?}");
                return None;
            }
        };

        let mut slice = match cell.as_slice() {
            Ok(slice) => slice,
            Err(e) => {
                tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Failed to make slice from cell. Error: {e:?}");
                return None;
            }
        };

        let info = match MsgInfo::load_from(&mut slice) {
            Ok(MsgInfo::ExtIn(message)) => message,
            Ok(info) => {
                tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, ?info, "Bad message. Unexpected message variant");
                return None;
            }
            Err(e) => {
                tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Bad cell. Failed to deserialize to ExtInMsgInfo. Err: {e:?}");
                return None;
            }
        };
        Some(Arc::new(ExternalMessage { cell, info }))
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
        Ok(self.anchors.read().get(&anchor_id).cloned())
    }

    async fn get_next_anchor(
        &self,
        prev_anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>> {
        loop {
            // NOTE: Subscribe to notification before checking
            let anchor_added = self.anchor_added.notified();

            {
                let anchors = self.anchors.read();

                match anchors.first() {
                    // Continue to wait for the first anchor on node start
                    None if prev_anchor_id == 0 => {}
                    // Return the first anchor on node start
                    Some((_, first)) if prev_anchor_id == 0 => return Ok(Some(first.clone())),
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
                            return Ok(Some(value.clone()));
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

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER,
            "anchors cache was cleared before anchor {}",
            before_anchor_id,
        );

        Ok(())
    }
}
