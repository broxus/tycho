mod anchor_handler;
mod cache;
mod config;
mod deduplicator;
mod parser;
mod state_update_queue;

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::Instrument;
use tycho_consensus::prelude::*;
use tycho_network::{Network, OverlayService, PeerResolver};
use tycho_storage::MempoolStorage;

use crate::mempool::impls::std_impl::anchor_handler::AnchorHandler;
use crate::mempool::impls::std_impl::cache::Cache;
use crate::mempool::impls::std_impl::config::ConfigAdapter;
use crate::mempool::{
    DebugStateUpdateContext, GetAnchorResult, MempoolAdapter, MempoolAdapterFactory,
    MempoolAnchorId, MempoolEventListener, StateUpdateContext,
};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

pub struct MempoolAdapterStdImpl {
    cache: Arc<Cache>,
    net_args: EngineNetworkArgs,

    config: Mutex<ConfigAdapter>,

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
        let config_builder = MempoolConfigBuilder::new(mempool_node_config);

        Self {
            cache: Default::default(),
            net_args: EngineNetworkArgs {
                key_pair,
                network: network.clone(),
                peer_resolver: peer_resolver.clone(),
                overlay_service: overlay_service.clone(),
            },
            config: Mutex::new(ConfigAdapter {
                builder: config_builder,
                state_update_queue: Default::default(),
                engine_running: None,
            }),
            store: MempoolAdapterStore::new(mempool_storage.clone(), RoundWatch::default()),
            input_buffer: InputBuffer::default(),
            top_known_anchor: RoundWatch::default(),
        }
    }

    /// **Warning:** changes from `GlobalConfig` may be rewritten by applied mc state
    /// only if applied mc state has greater time and GEQ round
    pub async fn set_config<F, R>(&self, fun: F) -> R
    where
        F: FnOnce(&mut MempoolConfigBuilder) -> R,
    {
        let mut config_guard = self.config.lock().await;
        fun(&mut config_guard.builder)
    }

    async fn handle_state_update(
        &self,
        config_guard: &mut ConfigAdapter,
        new_cx: &StateUpdateContext,
    ) -> Result<()> {
        // method is called in a for-cycle, so `seq_no` may differ
        let span = tracing::error_span!("mc_state_update", seq_no = new_cx.mc_block_id.seqno);
        let _guard = span.enter();

        if let Some(engine) = config_guard.engine_running.as_ref() {
            tracing::debug!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                id = %new_cx.mc_block_id.as_short_id(),
                new_cx = ?DebugStateUpdateContext(new_cx),
                "Processing state update from mc block",
            );

            // when genesis doesn't change - just (re-)schedule v_set change as defined by collator
            if engine.handle().merged_conf().genesis_info() == new_cx.consensus_info.genesis_info {
                ConfigAdapter::apply_prev_vset(engine.handle(), new_cx)?;
                ConfigAdapter::apply_curr_vset(engine.handle(), new_cx)?;
                ConfigAdapter::apply_next_vset(engine.handle(), new_cx);
                return Ok(());
            }

            // Genesis is changed at runtime - restart immediately:
            // block is signed by majority, so old mempool session and its anchors are not needed

            let engine = (config_guard.engine_running.take())
                .context("cannot happen: engine must be started")?;
            self.cache.reset();

            drop(_guard);
            engine.stop().instrument(span.clone()).await;
            let _guard = span.enter();

            // a new genesis is created even when overlay-related part of config stays the same
            (config_guard.builder).set_genesis(new_cx.consensus_info.genesis_info);
            // so config simultaneously changes with genesis via mempool restart
            (config_guard.builder).set_consensus_config(&new_cx.consensus_config)?;

            let merged_config = config_guard.builder.build()?;
            config_guard.engine_running = Some(self.run(&merged_config, new_cx)?);

            return Ok(());
        }

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            id = %new_cx.mc_block_id.as_short_id(),
            new_cx = ?DebugStateUpdateContext(new_cx),
            "Will start mempool with state update from mc block"
        );

        if let Some(genesis_override) = (config_guard.builder.get_genesis())
            .filter(|genesis| genesis.overrides(&new_cx.consensus_info.genesis_info))
        {
            // Note: assume that global config is applied to mempool adapter
            //   before collator is run in synchronous code, so this method is called later

            // genesis does not have externals, so only strictly greater time and round
            // will be saved into next block, so genesis can have values GEQ than in prev block
            anyhow::ensure!(
                genesis_override.start_round >= new_cx.top_processed_to_anchor_id
                    && genesis_override.genesis_millis >= new_cx.mc_block_chain_time,
                "new {genesis_override:?} should be >= \
                    top processed_to_anchor_id {} and block gen chain_time {}",
                new_cx.top_processed_to_anchor_id,
                new_cx.mc_block_chain_time,
            );

            tracing::warn!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                value = ?genesis_override,
                "Using genesis override from global config"
            );
            let message = match config_guard.builder.get_consensus_config() {
                Some(cc) if cc == &new_cx.consensus_config => {
                    "consensus config from global config is the same as in mc block"
                }
                Some(_) => "consensus config from global config overrides one from mc block",
                None => {
                    (config_guard.builder).set_consensus_config(&new_cx.consensus_config)?;
                    "no consensus config in global config, using one from mc block"
                }
            };
            // "message" is a reserved field in macro
            tracing::warn!(target: tracing_targets::MEMPOOL_ADAPTER, message);
        } else {
            (config_guard.builder).set_genesis(new_cx.consensus_info.genesis_info);
            (config_guard.builder).set_consensus_config(&new_cx.consensus_config)?;
        };

        let merged_config = config_guard.builder.build()?;
        config_guard.engine_running = Some(self.run(&merged_config, new_cx)?);

        Ok(())
    }

    /// Runs mempool engine
    fn run(
        &self,
        merged_conf: &MempoolMergedConfig,
        ctx: &StateUpdateContext,
    ) -> Result<EngineRunning> {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Starting mempool engine...");

        let (anchor_tx, anchor_rx) = mpsc::unbounded_channel();

        self.input_buffer.apply_config(merged_conf.consensus());

        // Note: mempool is always run from applied mc block
        self.top_known_anchor
            .set_max_raw(ctx.top_processed_to_anchor_id);

        let bind = EngineBinding {
            mempool_adapter_store: self.store.clone(),
            input_buffer: self.input_buffer.clone(),
            top_known_anchor: self.top_known_anchor.clone(),
            output: anchor_tx,
        };

        let engine = EngineCreated::new(bind, &self.net_args, merged_conf);

        // actual oldest sync round will be not less than this
        let estimated_sync_bottom = ctx
            .top_processed_to_anchor_id
            .saturating_sub(merged_conf.consensus().reset_rounds())
            .max(merged_conf.genesis_info().start_round);
        if estimated_sync_bottom >= ctx.consensus_info.vset_switch_round {
            if ctx.prev_validator_set.is_some() {
                tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "will not use prev vset");
            }
            ConfigAdapter::apply_curr_vset(engine.handle(), ctx)?;
            ConfigAdapter::apply_next_vset(engine.handle(), ctx);
        } else if estimated_sync_bottom >= ctx.consensus_info.prev_vset_switch_round {
            ConfigAdapter::apply_prev_vset(engine.handle(), ctx)?;
            ConfigAdapter::apply_curr_vset(engine.handle(), ctx)?;
            if ctx.next_validator_set.is_some() {
                tracing::warn!(target: tracing_targets::MEMPOOL_ADAPTER, "cannot use next vset");
            }
        } else {
            bail!(
                "cannot start from outdated peer sets (too short mempool epoch(s)): \
                 estimated sync bottom {estimated_sync_bottom} \
                 is older than prev vset switch round {}; \
                 start round {}, top processed to anchor {} in block {}",
                ctx.consensus_info.prev_vset_switch_round,
                merged_conf.genesis_info().start_round,
                ctx.top_processed_to_anchor_id,
                ctx.mc_block_id,
            )
        };

        let (engine_stop_tx, mut engine_stop_rx) = oneshot::channel();
        let engine = engine.run(engine_stop_tx);

        let mut anchor_task = AnchorHandler::new(merged_conf.consensus(), anchor_rx)
            .run(self.cache.clone(), self.store.clone())
            .boxed();

        tokio::spawn(async move {
            tokio::select! {
                () = &mut anchor_task => {}, // just poll
                engine_result = &mut engine_stop_rx => match engine_result {
                    Ok(()) => tracing::info!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        "Mempool main task is stopped: some subtask was cancelled"
                    ),
                    Err(_recv_error) => tracing::info!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        "Mempool main task is cancelled"
                    ),
                },
            }
        });

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool started");

        Ok(engine)
    }

    pub fn send_external(&self, message: Bytes) {
        self.input_buffer.push(message);
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
        // assume first block versions are monotonic by both top anchor and seqno
        // and there may be a second block version out of particular order,
        // but strictly before `handle_top_processed_to_anchor()` is called;
        // handle_top_processed_to_anchor() is called with monotonically increasing anchors
        let mut config_guard = self.config.lock().await;

        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            full_id = %new_cx.mc_block_id,
            "Received state update from mc block",
        );

        config_guard.state_update_queue.push(new_cx);

        Ok(())
    }

    async fn handle_top_processed_to_anchor(
        &self,
        mc_block_seqno: BlockSeqno,
        _anchor_id: MempoolAnchorId,
    ) -> Result<()> {
        let mut config_guard = self.config.lock().await;
        let queued = config_guard.state_update_queue.drain(..=mc_block_seqno)?;

        for ctx in queued {
            self.handle_state_update(&mut config_guard, &ctx).await?;
            self.top_known_anchor
                .set_max_raw(ctx.top_processed_to_anchor_id);
        }
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

    fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        self.cache.clear(before_anchor_id);
        Ok(())
    }
}
