mod cache;
mod deduplicator;
mod parser;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Result};
use async_trait::async_trait;
use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::{ConsensusConfig, ValidatorSet};
use parking_lot::Mutex;
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

struct UnappliedConfig {
    builder: MempoolConfigBuilder,
    state_update_ctx: Option<StateUpdateContext>,
    engine_handle: Option<EngineHandle>,
}

pub struct MempoolAdapterStdImpl {
    unapplied_config: Mutex<UnappliedConfig>,

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
            unapplied_config: Mutex::new(UnappliedConfig {
                builder: config_builder,
                state_update_ctx: None,
                engine_handle: None,
            }),
            cache: Default::default(),
            store: MempoolAdapterStore::new(mempool_storage.clone(), RoundWatch::default()),
            externals_tx,
            externals_rx: InputBuffer::new(externals_rx),
            top_known_anchor: RoundWatch::default(),
        }
    }

    pub fn set_update_ctx(&self, state_update_ctx: StateUpdateContext) {
        let mut config_guard = self.unapplied_config.lock();
        config_guard
            .builder
            .set_consensus_config(&state_update_ctx.consensus_config);
        // TODO set genesis from state update
        config_guard.builder.set_genesis(0, 0);
        config_guard.state_update_ctx = Some(state_update_ctx);
    }

    /// **Warning:** only to apply changes from `GlobalConfig` json after mempool crash
    pub fn override_config<F>(&self, fun: F)
    where
        F: FnOnce(&mut MempoolConfigBuilder),
    {
        let mut guard = self.unapplied_config.lock();
        fun(&mut guard.builder);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        self: &Arc<Self>,
        key_pair: Arc<KeyPair>,
        network: &Network,
        peer_resolver: &PeerResolver,
        overlay_service: &OverlayService,
    ) -> Result<()> {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");

        let (anchor_tx, anchor_rx) = mpsc::unbounded_channel();

        let mut config_guard = self.unapplied_config.lock();

        let last_state_update = config_guard
            .state_update_ctx
            .as_ref()
            .ok_or(anyhow!("consensus config is not set"))?;
        let mempool_config = config_guard.builder.build()?;
        let consensus_config = last_state_update.consensus_config.clone();

        let prev_peers = last_state_update
            .prev_validator_set
            .as_ref()
            .map(|(_, prev_set)| compute_subset(last_state_update, prev_set))
            .transpose()?;

        let current_peers = compute_subset(
            last_state_update,
            &last_state_update.current_validator_set.1,
        )?;

        let engine = Engine::new(
            key_pair,
            network,
            peer_resolver,
            overlay_service,
            &self.store,
            self.externals_rx.clone(),
            anchor_tx,
            &self.top_known_anchor,
            // This will be used as next set after genesis, skipping some existed set
            prev_peers.as_ref().unwrap_or(&current_peers),
            &mempool_config,
        );

        let handle = engine.get_handle();

        if prev_peers.is_some() {
            handle.set_next_peers(&current_peers, Some(last_state_update.mempool_switch_round));
        }

        if let Some((_, next_set)) = last_state_update.next_validator_set.as_ref() {
            let next_peers = compute_subset(last_state_update, next_set)?;
            handle.set_next_peers(&next_peers, None);
        }

        ensure!(
            config_guard.engine_handle.replace(handle).is_none(),
            "engine already started"
        );

        drop(config_guard);

        tokio::spawn(async move {
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

fn compute_subset(cx: &StateUpdateContext, validator_set: &ValidatorSet) -> Result<Vec<PeerId>> {
    let Some((list, _)) = validator_set.compute_subset(
        cx.mc_block_id.shard,
        &cx.catchain_config,
        // FIXME round at which current set is applied from next - so cannot shuffle prev epoch,
        //   also cannot determine which peers to broadcast to before round is determined
        //   => have to use previous switch round to shuffle current subset
        //      or use this to shuffle current validator set only
        0, // cx.mempool_switch_round,
    ) else {
        bail!(
            "Mempool peer set is empty after shuffle, mc_block_id: {}",
            cx.mc_block_id
        )
    };
    let result = list
        .into_iter()
        .map(|x| PeerId(x.public_key.0))
        .collect::<Vec<_>>();
    tracing::info!("New mempool validator subset len {}", result.len());
    Ok(result)
}

impl MempoolAdapterFactory for Arc<MempoolAdapterStdImpl> {
    type Adapter = MempoolAdapterStdImpl;

    fn create(&self, _listener: Arc<dyn MempoolEventListener>) -> Arc<Self::Adapter> {
        self.clone()
    }
}

#[async_trait]
impl MempoolAdapter for MempoolAdapterStdImpl {
    async fn handle_mc_state_update(&self, new_cx: StateUpdateContext) -> Result<()> {
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "Processing state update from mc block {}: {:?}",
            new_cx.mc_block_id.as_short_id(), DebugStateUpdateContext(&new_cx),
        );

        let mut config_guard = self.unapplied_config.lock();

        let (skip, set_current) = match config_guard.state_update_ctx.as_ref() {
            Some(old_cx) => {
                let skip = old_cx.mempool_switch_round >= new_cx.mempool_switch_round;
                let set_current =
                    !skip && old_cx.current_validator_set.0 != new_cx.current_validator_set.0;
                (skip, set_current)
            }
            None => (false, true),
        };

        if skip {
            tracing::info!(
                "Skipped old state update from mc block {}: {:?}",
                new_cx.mc_block_id.as_short_id(),
                DebugStateUpdateContext(&new_cx),
            );
            return Ok(());
        };

        let Some(engine) = config_guard.engine_handle.as_ref() else {
            tracing::info!(
                "Queued state update from mc block {}: {:?} to apply on mempool start",
                new_cx.mc_block_id.as_short_id(),
                DebugStateUpdateContext(&new_cx),
            );
            config_guard.state_update_ctx = Some(new_cx);
            return Ok(());
        };

        if set_current {
            let subset = compute_subset(&new_cx, &new_cx.current_validator_set.1)?;
            engine.set_next_peers(&subset, Some(new_cx.mempool_switch_round));
        }

        if let Some(next_set) = &new_cx.next_validator_set {
            let subset = compute_subset(&new_cx, &next_set.1)?;
            engine.set_next_peers(&subset, None);
        }

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
