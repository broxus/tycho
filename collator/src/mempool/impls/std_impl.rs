mod cache;
mod deduplicator;
mod parser;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::ConsensusConfig;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use tokio::sync::mpsc;
use tycho_consensus::prelude::*;
use tycho_network::{Network, OverlayService, PeerId, PeerResolver};
use tycho_storage::MempoolStorage;
use tycho_util::time::now_millis;

use crate::mempool::impls::std_impl::cache::Cache;
use crate::mempool::impls::std_impl::parser::Parser;
use crate::mempool::{
    DebugStateUpdateContext, MempoolAdapter, MempoolAdapterFactory, MempoolAnchor, MempoolAnchorId,
    MempoolEventListener, StateUpdateContext,
};
use crate::tracing_targets;

pub struct MempoolAdapterStdImpl {
    config_builder: Mutex<MempoolConfigBuilder>,

    cache: Arc<Cache>,

    store: MempoolAdapterStore,

    externals_rx: InputBuffer,
    externals_tx: mpsc::UnboundedSender<Bytes>,
    top_known_anchor: RoundWatch<TopKnownAnchor>,
}

impl MempoolAdapterStdImpl {
    pub fn new(mempool_storage: &MempoolStorage, mempool_node_config: &MempoolNodeConfig) -> Self {
        let mut config_builder = MempoolConfigBuilder::default();
        config_builder.set_node_config(mempool_node_config);

        let (externals_tx, externals_rx) = mpsc::unbounded_channel();

        Self {
            config_builder: Mutex::new(config_builder),
            cache: Default::default(),
            store: MempoolAdapterStore::new(mempool_storage.clone(), RoundWatch::default()),
            externals_tx,
            externals_rx: InputBuffer::new(externals_rx),
            top_known_anchor: RoundWatch::default(),
        }
    }

    pub fn config_builder(&self) -> MutexGuard<'_, RawMutex, MempoolConfigBuilder> {
        self.config_builder.lock()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        self: &Arc<Self>,
        key_pair: Arc<KeyPair>,
        network: &Network,
        peer_resolver: &PeerResolver,
        overlay_service: &OverlayService,
        peers: Vec<PeerId>,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");

        let (anchor_tx, anchor_rx) = mpsc::unbounded_channel();

        let (consensus_config, mempool_config) = {
            let builder = self.config_builder.lock();
            let consensus_config = builder
                .get_consensus_config()
                .ok_or(anyhow!("consensus config is not set"))?;
            (consensus_config.clone(), builder.build()?)
        };

        let mut engine = Engine::new(
            key_pair,
            network,
            peer_resolver,
            overlay_service,
            &self.store,
            self.externals_rx.clone(),
            anchor_tx,
            &self.top_known_anchor,
            &mempool_config,
        );

        tokio::spawn(async move {
            engine.set_peers(&peers);
            engine.run().await;
        });

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool adapter created");

        tokio::spawn(Self::handle_anchors_task(
            self.cache.clone(),
            self.store.clone(),
            consensus_config,
            anchor_rx,
        ));

        Ok(())
    }

    pub fn send_external(&self, message: Bytes) {
        self.externals_tx.send(message).ok();
    }

    async fn handle_anchors_task(
        cache: Arc<Cache>,
        store: MempoolAdapterStore,
        config: ConsensusConfig,
        mut anchor_rx: mpsc::UnboundedReceiver<CommitResult>,
    ) {
        let mut parser = Parser::new(config.deduplicate_rounds);
        let mut first_after_gap = None;
        while let Some(commit) = anchor_rx.recv().await {
            let (anchor, history) = match commit {
                CommitResult::NewStartAfterGap(anchors_full_bottom) => {
                    cache.reset();
                    parser = Parser::new(config.deduplicate_rounds);
                    store.report_new_start(anchors_full_bottom);
                    first_after_gap = Some(
                        (anchors_full_bottom.0).saturating_add(config.deduplicate_rounds as u32),
                    );
                    tracing::info!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        "externals cache dropped"
                    );
                    continue;
                }
                CommitResult::Next(data) => (data.anchor, data.history),
            };

            let task = tokio::task::spawn_blocking({
                let anchors = cache.clone();
                let store = store.clone();

                move || {
                    let author = anchor.data().author;
                    let chain_time = anchor.data().time.as_u64();
                    let anchor_id: MempoolAnchorId = anchor.round().0;
                    metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor_id);

                    // may skip expand part, but never skip set committed part;
                    let points = store.expand_anchor_history(&history);
                    // set committed only after point data is read or skipped
                    store.set_committed(&anchor, &history);

                    let is_executable = first_after_gap
                        .as_ref()
                        .map_or(true, |first_id| anchor_id >= *first_id);

                    let unique_messages =
                        parser.parse_unique(anchor_id, chain_time, is_executable, points);

                    if is_executable {
                        anchors.push(Arc::new(MempoolAnchor {
                            id: anchor_id,
                            chain_time,
                            author,
                            externals: unique_messages,
                        }));
                    }

                    metrics::histogram!("tycho_mempool_commit_anchor_latency_time").record(
                        Duration::from_millis(now_millis().max(chain_time) - chain_time)
                            .as_secs_f64(),
                    );

                    parser.clean(anchor_id)
                }
            });
            parser = task.await.expect("expand anchor history task failed");
        }
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
    async fn handle_mc_state_update(&self, cx: StateUpdateContext) -> Result<()> {
        // TODO: make real implementation, currently does nothing
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "STUB: Processing state update from mc block {}: {:?}",
            cx.mc_block_id.as_short_id(), DebugStateUpdateContext(&cx),
        );
        Ok(())
    }

    async fn handle_top_processed_to_anchor(&self, anchor_id: u32) -> Result<()> {
        self.top_known_anchor.set_max_raw(anchor_id);
        Ok(())
    }

    async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>> {
        Ok(self.cache.get_anchor_by_id(anchor_id).await)
    }

    async fn get_next_anchor(
        &self,
        prev_anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>> {
        Ok(self.cache.get_next_anchor(prev_anchor_id).await)
    }

    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        self.cache.clear(before_anchor_id);
        Ok(())
    }
}
