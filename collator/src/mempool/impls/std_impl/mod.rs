mod adapter_impl;
mod anchor_handler;
mod state_update_queue;

use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::FutureExt;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::Instrument;
use tycho_consensus::prelude::*;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::{Network, OverlayService, PeerResolver};
use tycho_slasher_traits::{MempoolEventsCache, MempoolEventsListener};

use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::v_set_adapter::VSetAdapter;
use crate::mempool::impls::std_impl::anchor_handler::StdAnchorHandler;
use crate::mempool::impls::std_impl::state_update_queue::StateUpdateQueue;
use crate::mempool::{DebugStateUpdateContext, StateUpdateContext};
use crate::tracing_targets;

pub struct MempoolAdapterStdImpl {
    cache: Arc<Cache>,
    net_args: EngineNetworkArgs,

    config: Mutex<StdConfigAdapter>,

    mempool_db: Arc<MempoolDb>,
    input_buffer: InputBuffer,
    stats_tx: Arc<dyn MempoolEventsListener>,
    stats_rx: Arc<dyn MempoolEventsCache>,
    top_known_anchor: RoundWatch<TopKnownAnchor>,
}

struct StdConfigAdapter {
    builder: MempoolConfigBuilder,
    state_update_queue: StateUpdateQueue,
    engine_session: Option<EngineSession>,
}

impl MempoolAdapterStdImpl {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_pair: Arc<KeyPair>,
        network: &Network,
        peer_resolver: &PeerResolver,
        overlay_service: &OverlayService,
        mempool_db: Arc<MempoolDb>,
        moderator: Moderator,
        stats_tx: Arc<dyn MempoolEventsListener>,
        stats_rx: Arc<dyn MempoolEventsCache>,
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
                moderator,
            },
            config: Mutex::new(StdConfigAdapter {
                builder: config_builder,
                state_update_queue: Default::default(),
                engine_session: None,
            }),
            mempool_db,
            input_buffer: InputBuffer::default(),

            stats_tx,
            stats_rx,
            top_known_anchor: RoundWatch::default(),
        })
    }

    async fn process_state_update(
        &self,
        config_guard: &mut StdConfigAdapter,
        new_cx: &StateUpdateContext,
    ) -> Result<()> {
        // method is called in a for-cycle, so `seq_no` may differ
        let span = tracing::error_span!("mc_state_update", seq_no = new_cx.mc_block_id.seqno);
        let _guard = span.enter();

        if let Some(session) = config_guard.engine_session.as_ref() {
            tracing::debug!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                id = %new_cx.mc_block_id.as_short_id(),
                new_cx = ?DebugStateUpdateContext(new_cx),
                "Processing state update from mc block",
            );

            // when genesis doesn't change - just (re-)schedule v_set change as defined by collator
            if session.genesis_info() == new_cx.consensus_info.genesis_info {
                session.set_peers(VSetAdapter::init_peers(new_cx)?);
                return Ok(());
            }

            // Genesis is changed at runtime - restart immediately:
            // block is signed by majority, so old mempool session and its anchors are not needed

            let session = (config_guard.engine_session.take())
                .context("cannot happen: engine must be started")?;
            self.cache.reset();

            drop(_guard);
            session.stop().instrument(span.clone()).await;
            let _guard = span.enter();

            // a new genesis is created even when overlay-related part of config stays the same
            (config_guard.builder).set_genesis(new_cx.consensus_info.genesis_info);
            // so config simultaneously changes with genesis via mempool restart
            (config_guard.builder).set_consensus_config(&new_cx.consensus_config)?;

            let merged_config = config_guard.builder.build()?;
            config_guard.engine_session = Some(self.start(&merged_config, new_cx)?);

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
        config_guard.engine_session = Some(self.start(&merged_config, new_cx)?);

        Ok(())
    }

    /// Runs mempool engine session
    fn start(
        &self,
        merged_conf: &MempoolMergedConfig,
        ctx: &StateUpdateContext,
    ) -> Result<EngineSession> {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Starting mempool engine...");

        let (anchors_tx, anchors_rx) = mpsc::unbounded_channel();

        self.input_buffer.apply_config(&merged_conf.conf.consensus);

        // Note: mempool is always run from applied mc block
        self.top_known_anchor
            .set_max_raw(ctx.top_processed_to_anchor_id);

        let commit_finished = RoundWatch::default();

        let bind = EngineBinding {
            mempool_db: self.mempool_db.clone(),
            input_buffer: self.input_buffer.clone(),
            top_known_anchor: self.top_known_anchor.clone(),
            commit_finished: commit_finished.clone(),
            anchors_tx,
            stats_tx: self.stats_tx.clone(),
        };

        // actual oldest sync round will be not less than this
        let estimated_sync_bottom = ctx
            .top_processed_to_anchor_id
            .saturating_sub(merged_conf.conf.consensus.reset_rounds())
            .max(merged_conf.genesis_info.start_round);
        anyhow::ensure!(
            estimated_sync_bottom >= ctx.consensus_info.prev_vset_switch_round,
            "cannot start from outdated peer sets (too short mempool epoch(s)): \
                 estimated sync bottom {estimated_sync_bottom} \
                 is older than prev vset switch round {}; \
                 start round {}, top processed to anchor {} in block {}",
            ctx.consensus_info.prev_vset_switch_round,
            merged_conf.genesis_info.start_round,
            ctx.top_processed_to_anchor_id,
            ctx.mc_block_id,
        );

        let init_peers = VSetAdapter::init_peers(ctx)?;
        if init_peers.curr_v_set.len() == 1 {
            anyhow::bail!("pass `single-node` cli flag to run network of 1 node");
        } else if init_peers.curr_v_set.len() == 2 {
            anyhow::bail!("cannot run mempool with 2 nodes, gen network with either 1 or 3 nodes");
        };

        let (engine_stop_tx, mut engine_stop_rx) = oneshot::channel();
        let session = EngineSession::new(
            bind,
            &self.net_args,
            merged_conf,
            init_peers,
            engine_stop_tx,
        );

        let mut anchors_task = StdAnchorHandler::new(&merged_conf.conf.consensus, anchors_rx)
            .run(self.cache.clone(), self.mempool_db.clone(), commit_finished)
            .boxed();

        tokio::spawn(async move {
            tokio::select! {
                () = &mut anchors_task => {}, // just poll
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

        Ok(session)
    }
}
