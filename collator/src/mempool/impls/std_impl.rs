mod cache;
mod config;
mod deduplicator;
mod parser;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::ConsensusConfig;
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tycho_consensus::prelude::*;
use tycho_network::{Network, OverlayService, PeerResolver};
use tycho_storage::MempoolStorage;
use tycho_util::time::now_millis;

use crate::mempool::impls::std_impl::cache::Cache;
use crate::mempool::impls::std_impl::config::ConfigAdapter;
use crate::mempool::impls::std_impl::parser::Parser;
use crate::mempool::{
    DebugStateUpdateContext, GetAnchorResult, MempoolAdapter, MempoolAdapterFactory, MempoolAnchor,
    MempoolAnchorId, MempoolEventListener, StateUpdateContext,
};
use crate::tracing_targets;

pub struct MempoolAdapterStdImpl {
    config: Mutex<ConfigAdapter>,

    cache: Arc<Cache>,

    key_pair: Arc<KeyPair>,
    network: Network,
    peer_resolver: PeerResolver,
    overlay_service: OverlayService,
    store: MempoolAdapterStore,

    input_buffer: InputBuffer,
    top_known_anchor: RoundWatch<TopKnownAnchor>,
}

impl MempoolAdapterStdImpl {
    pub fn new(
        key_pair: Arc<KeyPair>,
        network: &Network,
        peer_resolver: &PeerResolver,
        overlay_service: &OverlayService,
        mempool_storage: &MempoolStorage,
        mempool_node_config: &MempoolNodeConfig,
    ) -> Self {
        let mut config_builder = MempoolConfigBuilder::default();
        config_builder.set_node_config(mempool_node_config);

        Self {
            config: Mutex::new(ConfigAdapter {
                builder: config_builder,
                state_update_ctx: None,
                engine_handle: None,
            }),
            cache: Default::default(),
            key_pair,
            network: network.clone(),
            peer_resolver: peer_resolver.clone(),
            overlay_service: overlay_service.clone(),
            store: MempoolAdapterStore::new(mempool_storage.clone(), RoundWatch::default()),
            input_buffer: InputBuffer::default(),
            top_known_anchor: RoundWatch::default(),
        }
    }

    /// **Warning:** changes from `GlobalConfig` may be rewritten by applied mc state
    /// only if applied mc state has greater time and round
    pub async fn set_config<F, R>(&self, fun: F) -> R
    where
        F: FnOnce(&mut MempoolConfigBuilder) -> R,
    {
        let mut config_guard = self.config.lock().await;
        fun(&mut config_guard.builder)
    }

    /// Runs mempool engine
    fn run(&self, config_guard: &MutexGuard<'_, ConfigAdapter>) -> Result<EngineHandle> {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Starting mempool engine...");

        let (anchor_tx, anchor_rx) = mpsc::unbounded_channel();

        let last_state_update = config_guard
            .state_update_ctx
            .as_ref()
            .ok_or(anyhow!("last state update context is not set"))?;
        let (genesis_round, _) = config_guard
            .builder
            .get_genesis()
            .context("genesis must be set")?;
        let mempool_config = config_guard.builder.build()?;
        let consensus_config = (config_guard.builder.get_consensus_config().cloned())
            .ok_or(anyhow!("consensus config is not set"))?;

        // TODO support config change; payload size is bound to mempool rounds
        self.input_buffer.apply_config(&consensus_config);

        // Note: mempool is always run from applied mc block
        self.top_known_anchor
            .set_max_raw(last_state_update.mc_processed_to_anchor_id);

        let engine = Engine::new(
            self.key_pair.clone(),
            &self.network,
            &self.peer_resolver,
            &self.overlay_service,
            &self.store,
            self.input_buffer.clone(),
            anchor_tx,
            &self.top_known_anchor,
            // This will be used as next set after genesis, skipping some existed set
            &mempool_config,
        );

        let handle = engine.get_handle();

        // actual oldest sync round will be not less than this
        let estimated_sync_bottom = last_state_update
            .mc_processed_to_anchor_id
            .saturating_sub(consensus_config.max_consensus_lag_rounds as u32)
            .saturating_sub(consensus_config.deduplicate_rounds as u32)
            .saturating_sub(consensus_config.commit_history_rounds as u32)
            .max(genesis_round);
        if estimated_sync_bottom >= last_state_update.consensus_info.vset_switch_round {
            if last_state_update.prev_validator_set.is_some() {
                tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "will not use prev vset");
            }
            ConfigAdapter::apply_vset(&handle, last_state_update)?;
            ConfigAdapter::apply_next_vset(&handle, last_state_update);
        } else if estimated_sync_bottom >= last_state_update.consensus_info.prev_vset_switch_round {
            ConfigAdapter::apply_prev_vset(&handle, last_state_update)?;
            ConfigAdapter::apply_vset(&handle, last_state_update)?;
            if last_state_update.next_validator_set.is_some() {
                tracing::warn!(target: tracing_targets::MEMPOOL_ADAPTER, "cannot use next vset");
            }
        } else {
            bail!(
                "cannot start from outdated peer sets (too short mempool epoch(s)): \
                 estimated sync bottom {estimated_sync_bottom} \
                 is older than prev vset switch round {}; \
                 genesis round {genesis_round}, masterchain processed to anchor {} in block {}",
                last_state_update.consensus_info.prev_vset_switch_round,
                last_state_update.mc_processed_to_anchor_id,
                last_state_update.mc_block_id,
            )
        };

        tokio::spawn(async move {
            scopeguard::defer!(tracing::warn!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                "mempool engine stopped"
            ));
            engine.run().await;
        });

        tokio::spawn(Self::handle_anchors_task(
            self.cache.clone(),
            self.store.clone(),
            consensus_config,
            anchor_rx,
        ));

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool started");

        Ok(handle)
    }

    pub fn send_external(&self, message: Bytes) {
        self.input_buffer.push(message);
    }

    async fn handle_anchors_task(
        cache: Arc<Cache>,
        store: MempoolAdapterStore,
        config: ConsensusConfig,
        mut anchor_rx: mpsc::UnboundedReceiver<MempoolOutput>,
    ) {
        scopeguard::defer!(tracing::warn!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "handle anchors task stopped"
        ));
        let mut parser = Parser::new(config.deduplicate_rounds);
        let mut first_after_gap = None;
        while let Some(commit) = anchor_rx.recv().await {
            let committed = match commit {
                MempoolOutput::NextAnchor(committed) => committed,
                MempoolOutput::NewStartAfterGap(anchors_full_bottom) => {
                    cache.reset();
                    parser = Parser::new(config.deduplicate_rounds);
                    store.report_new_start(anchors_full_bottom);
                    let first_to_execute =
                        (anchors_full_bottom.0).saturating_add(config.deduplicate_rounds as u32);
                    first_after_gap = Some(first_to_execute);
                    tracing::info!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        new_bottom = anchors_full_bottom.0,
                        first_after_gap = first_to_execute,
                        "externals cache dropped",
                    );
                    continue;
                }
                MempoolOutput::Running => {
                    cache.set_paused(false);
                    continue;
                }
                MempoolOutput::Paused => {
                    cache.set_paused(true);
                    continue;
                }
            };

            let task = tokio::task::spawn_blocking({
                let anchors = cache.clone();
                let store = store.clone();

                move || {
                    let author = committed.anchor.data().author;
                    let chain_time = committed.anchor.data().time.as_u64();
                    let anchor_id: MempoolAnchorId = committed.anchor.round().0;
                    metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor_id);

                    let payloads =
                        store.expand_anchor_history(&committed.anchor, &committed.history);

                    let is_executable = first_after_gap
                        .as_ref()
                        .map_or(true, |first_id| anchor_id >= *first_id);

                    let unique_messages =
                        parser.parse_unique(anchor_id, chain_time, is_executable, payloads);

                    if is_executable {
                        anchors.push(Arc::new(MempoolAnchor {
                            id: anchor_id,
                            prev_id: committed.prev_anchor.map(|round| round.0),
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
    async fn handle_mc_state_update(&self, new_cx: StateUpdateContext) -> Result<()> {
        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            id = %new_cx.mc_block_id.as_short_id(),
            new_cx = ?DebugStateUpdateContext(&new_cx),
            "Processing state update from mc block",
        );

        // NOTE: on the first call mempool engine will not be running
        //      and `state_update_ctx` will be `None`

        let mut config_guard = self.config.lock().await;

        let Some(engine) = config_guard.engine_handle.as_ref() else {
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                id = %new_cx.mc_block_id.as_short_id(),
                new_cx = ?DebugStateUpdateContext(&new_cx),
                "Will start mempool with state update from mc block"
            );

            if let Some((round, time)) = (config_guard.builder.get_genesis())
                .filter(|(_, time)| *time > new_cx.consensus_info.genesis_millis)
            {
                // Note: assume that global config is applied to mempool adapter
                //   before collator is run in synchronous code, so this method is called later

                anyhow::ensure!(
                    round >= new_cx.mc_processed_to_anchor_id && time >= new_cx.mc_block_chain_time,
                    "new genesis round {} and time {} should be >= \
                    master block processed_to_anchor_id {} and gen chain_time {}",
                    round,
                    time,
                    new_cx.mc_processed_to_anchor_id,
                    new_cx.mc_block_chain_time,
                );

                tracing::warn!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    %round,
                    %time,
                    "Using genesis override from global config"
                );
                match config_guard.builder.get_consensus_config() {
                    Some(cc) if cc == &new_cx.consensus_config => {
                        tracing::warn!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            "consensus config from global config is the same as in mc block"
                        );
                    }
                    Some(_) => {
                        tracing::warn!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            "consensus config from global config overrides one from mc block"
                        );
                    }
                    None => {
                        (config_guard.builder).set_consensus_config(&new_cx.consensus_config);
                        tracing::warn!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            "no consensus config in global config, using one from mc block"
                        );
                    }
                }
            } else {
                config_guard.builder.set_genesis(
                    new_cx.consensus_info.genesis_round,
                    new_cx.consensus_info.genesis_millis,
                );
                (config_guard.builder).set_consensus_config(&new_cx.consensus_config);
            }

            config_guard.state_update_ctx = Some(new_cx);
            config_guard.engine_handle = Some(self.run(&config_guard)?);
            return Ok(());
        };

        if (config_guard.state_update_ctx.as_ref()).map_or(false, |old_cx| {
            old_cx.consensus_info.vset_switch_round >= new_cx.consensus_info.vset_switch_round
        }) {
            tracing::debug!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                id = %new_cx.mc_block_id.as_short_id(),
                new_cx = ?DebugStateUpdateContext(&new_cx),
                "Skipped old state update from mc block",
            );
            return Ok(());
        };

        ConfigAdapter::apply_vset(engine, &new_cx)?;
        ConfigAdapter::apply_next_vset(engine, &new_cx);
        config_guard.state_update_ctx = Some(new_cx);
        Ok(())
    }

    async fn handle_top_processed_to_anchor(&self, anchor_id: u32) -> Result<()> {
        self.top_known_anchor.set_max_raw(anchor_id);
        Ok(())
    }

    async fn get_anchor_by_id(
        &self,
        top_processed_to_anchor: MempoolAnchorId,
        anchor_id: MempoolAnchorId,
    ) -> Result<GetAnchorResult> {
        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            %top_processed_to_anchor,
            %anchor_id,
            "get_anchor_by_id"
        );

        let result = match self.cache.get_anchor_by_id(anchor_id).await {
            Some(anchor) => GetAnchorResult::Exist(anchor),
            None => GetAnchorResult::NotExist,
        };

        Ok(result)
    }

    async fn get_next_anchor(
        &self,
        top_processed_to_anchor: MempoolAnchorId,
        prev_anchor_id: MempoolAnchorId,
    ) -> Result<GetAnchorResult> {
        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            %top_processed_to_anchor,
            %prev_anchor_id,
            "get_next_anchor"
        );

        let result = match self.cache.get_next_anchor(prev_anchor_id).await? {
            Some(anchor) => GetAnchorResult::Exist(anchor),
            None => GetAnchorResult::NotExist,
        };

        Ok(result)
    }

    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        self.cache.clear(before_anchor_id);
        Ok(())
    }
}
