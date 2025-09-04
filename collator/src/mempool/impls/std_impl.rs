mod anchor_handler;
mod cache;
mod config;
mod deduplicator;
mod parser;
mod state_update_queue;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::FutureExt;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::Instrument;
use tycho_consensus::prelude::*;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::{Network, OverlayService, PeerResolver};
use tycho_storage::StorageContext;

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
    mempool_db: Arc<MempoolDb>,
    input_buffer: InputBuffer,
    top_known_anchor: RoundWatch<TopKnownAnchor>,
}
impl MempoolAdapterStdImpl {
    pub fn new(
        key_pair: Arc<KeyPair>,
        network: &Network,
        peer_resolver: &PeerResolver,
        overlay_service: &OverlayService,
        storage_context: &StorageContext,
        mempool_node_config: &MempoolNodeConfig,
    ) -> Result<Self> {
        let config_builder = MempoolConfigBuilder::new(mempool_node_config);
        Ok(Self {
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
                engine_session: None,
            }),
            mempool_db: MempoolDb::open(storage_context.clone(), RoundWatch::default())
                .context("failed to create mempool adapter storage")?,
            input_buffer: InputBuffer::default(),
            top_known_anchor: RoundWatch::default(),
        })
    }
    /// **Warning:** changes from `GlobalConfig` may be rewritten by applied mc state
    /// only if applied mc state has greater time and GEQ round
    pub async fn set_config<F, R>(&self, fun: F) -> R
    where
        F: FnOnce(&mut MempoolConfigBuilder) -> R,
    {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(set_config)),
            file!(),
            line!(),
        );
        let mut config_guard = {
            __guard.end_section(line!());
            let __result = self.config.lock().await;
            __guard.start_section(line!());
            __result
        };
        fun(&mut config_guard.builder)
    }
    async fn process_state_update(
        &self,
        config_guard: &mut ConfigAdapter,
        new_cx: &StateUpdateContext,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(process_state_update)),
            file!(),
            line!(),
        );
        let span = tracing::error_span!("mc_state_update", seq_no = new_cx.mc_block_id.seqno);
        let _guard = span.enter();
        if let Some(session) = config_guard.engine_session.as_ref() {
            tracing::debug!(
                target : tracing_targets::MEMPOOL_ADAPTER, id = % new_cx.mc_block_id
                .as_short_id(), new_cx = ? DebugStateUpdateContext(new_cx),
                "Processing state update from mc block",
            );
            if session.genesis_info() == new_cx.consensus_info.genesis_info {
                session.set_peers(ConfigAdapter::init_peers(new_cx)?);
                return Ok(());
            }
            let session = (config_guard.engine_session.take())
                .context("cannot happen: engine must be started")?;
            self.cache.reset();
            drop(_guard);
            {
                __guard.end_section(line!());
                let __result = session.stop().instrument(span.clone()).await;
                __guard.start_section(line!());
                __result
            };
            let _guard = span.enter();
            (config_guard.builder).set_genesis(new_cx.consensus_info.genesis_info);
            (config_guard.builder).set_consensus_config(&new_cx.consensus_config)?;
            let merged_config = config_guard.builder.build()?;
            config_guard.engine_session = Some(self.start(&merged_config, new_cx)?);
            return Ok(());
        }
        tracing::info!(
            target : tracing_targets::MEMPOOL_ADAPTER, id = % new_cx.mc_block_id
            .as_short_id(), new_cx = ? DebugStateUpdateContext(new_cx),
            "Will start mempool with state update from mc block"
        );
        if let Some(genesis_override) = (config_guard.builder.get_genesis())
            .filter(|genesis| genesis.overrides(&new_cx.consensus_info.genesis_info))
        {
            anyhow::ensure!(
                genesis_override.start_round >= new_cx.top_processed_to_anchor_id
                    && genesis_override.genesis_millis >= new_cx.mc_block_chain_time,
                "new {genesis_override:?} should be >= \
                    top processed_to_anchor_id {} and block gen chain_time {}",
                new_cx.top_processed_to_anchor_id,
                new_cx.mc_block_chain_time,
            );
            tracing::warn!(
                target : tracing_targets::MEMPOOL_ADAPTER, value = ? genesis_override,
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
            tracing::warn!(target : tracing_targets::MEMPOOL_ADAPTER, message);
        } else {
            (config_guard.builder).set_genesis(new_cx.consensus_info.genesis_info);
            (config_guard.builder).set_consensus_config(&new_cx.consensus_config)?;
        };
        let merged_config = config_guard.builder.build()?;
        config_guard.engine_session = Some(self.start(&merged_config, new_cx)?);
        Ok(())
    }
    /// Runs mempool engine session
    fn start(
        &self,
        merged_conf: &MempoolMergedConfig,
        ctx: &StateUpdateContext,
    ) -> Result<EngineSession> {
        tracing::info!(
            target : tracing_targets::MEMPOOL_ADAPTER, "Starting mempool engine..."
        );
        let (anchor_tx, anchor_rx) = mpsc::unbounded_channel();
        self.input_buffer.apply_config(merged_conf.consensus());
        self.top_known_anchor
            .set_max_raw(ctx.top_processed_to_anchor_id);
        let bind = EngineBinding {
            mempool_db: self.mempool_db.clone(),
            input_buffer: self.input_buffer.clone(),
            top_known_anchor: self.top_known_anchor.clone(),
            output: anchor_tx,
        };
        let estimated_sync_bottom = ctx
            .top_processed_to_anchor_id
            .saturating_sub(merged_conf.consensus().reset_rounds())
            .max(merged_conf.genesis_info().start_round);
        anyhow::ensure!(
            estimated_sync_bottom >= ctx.consensus_info.prev_vset_switch_round,
            "cannot start from outdated peer sets (too short mempool epoch(s)): \
                 estimated sync bottom {estimated_sync_bottom} \
                 is older than prev vset switch round {}; \
                 start round {}, top processed to anchor {} in block {}",
            ctx.consensus_info.prev_vset_switch_round,
            merged_conf.genesis_info().start_round,
            ctx.top_processed_to_anchor_id,
            ctx.mc_block_id,
        );
        let (engine_stop_tx, mut engine_stop_rx) = oneshot::channel();
        let session = EngineSession::new(
            bind,
            &self.net_args,
            merged_conf,
            ConfigAdapter::init_peers(ctx)?,
            engine_stop_tx,
        );
        let mut anchor_task = AnchorHandler::new(merged_conf.consensus(), anchor_rx)
            .run(self.cache.clone(), self.mempool_db.clone())
            .boxed();
        tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            {
                __guard.end_section(line!());
                let __result = tokio::select! {
                    () = & mut anchor_task => {}, engine_result = & mut engine_stop_rx =>
                    match engine_result { Ok(()) => tracing::info!(target :
                    tracing_targets::MEMPOOL_ADAPTER,
                    "Mempool main task is stopped: some subtask was cancelled"),
                    Err(_recv_error) => tracing::info!(target :
                    tracing_targets::MEMPOOL_ADAPTER, "Mempool main task is cancelled"),
                    },
                };
                __guard.start_section(line!());
                __result
            }
        });
        tracing::info!(target : tracing_targets::MEMPOOL_ADAPTER, "Mempool started");
        Ok(session)
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_mc_state_update)),
            file!(),
            line!(),
        );
        let mut config_guard = {
            __guard.end_section(line!());
            let __result = self.config.lock().await;
            __guard.start_section(line!());
            __result
        };
        tracing::debug!(
            target : tracing_targets::MEMPOOL_ADAPTER, full_id = % new_cx.mc_block_id,
            "Received state update from mc block",
        );
        if let Some(ctx) = config_guard.state_update_queue.push(new_cx)? {
            {
                __guard.end_section(line!());
                let __result = self.process_state_update(&mut config_guard, &ctx).await;
                __guard.start_section(line!());
                __result
            }?;
            self.top_known_anchor
                .set_max_raw(ctx.top_processed_to_anchor_id);
        }
        Ok(())
    }
    async fn handle_signed_mc_block(&self, mc_block_seqno: BlockSeqno) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_signed_mc_block)),
            file!(),
            line!(),
        );
        let mut config_guard = {
            __guard.end_section(line!());
            let __result = self.config.lock().await;
            __guard.start_section(line!());
            __result
        };
        for ctx in config_guard.state_update_queue.signed(mc_block_seqno)? {
            {
                __guard.end_section(line!());
                let __result = self.process_state_update(&mut config_guard, &ctx).await;
                __guard.start_section(line!());
                __result
            }?;
            self.top_known_anchor
                .set_max_raw(ctx.top_processed_to_anchor_id);
        }
        Ok(())
    }
    async fn get_anchor_by_id(&self, anchor_id: MempoolAnchorId) -> Result<GetAnchorResult> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_anchor_by_id)),
            file!(),
            line!(),
        );
        tracing::debug!(
            target : tracing_targets::MEMPOOL_ADAPTER, % anchor_id, "get_anchor_by_id"
        );
        let result = match {
            __guard.end_section(line!());
            let __result = self.cache.get_anchor_by_id(anchor_id).await;
            __guard.start_section(line!());
            __result
        } {
            Some(anchor) => GetAnchorResult::Exist(anchor),
            None => GetAnchorResult::NotExist,
        };
        Ok(result)
    }
    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<GetAnchorResult> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_next_anchor)),
            file!(),
            line!(),
        );
        tracing::debug!(
            target : tracing_targets::MEMPOOL_ADAPTER, % prev_anchor_id,
            "get_next_anchor"
        );
        let result = match {
            __guard.end_section(line!());
            let __result = self.cache.get_next_anchor(prev_anchor_id).await;
            __guard.start_section(line!());
            __result
        }? {
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
