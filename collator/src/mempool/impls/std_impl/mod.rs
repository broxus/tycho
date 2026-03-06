mod adapter_impl;
mod anchor_handler;
mod session_keeper;
mod state_update_queue;

use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::FutureExt;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::Instrument;
use tycho_consensus::prelude::*;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::{Network, OverlayService, PeerResolver};
use tycho_storage::StorageContext;

use crate::mempool::StateUpdateContext;
use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::v_set_adapter::VSetAdapter;
use crate::mempool::impls::std_impl::anchor_handler::StdAnchorHandler;
use crate::mempool::impls::std_impl::session_keeper::StdSessionKeeper;
use crate::tracing_targets;

pub struct MempoolAdapterStdImpl {
    cache: Arc<Cache>,
    net_args: EngineNetworkArgs,

    keeper: Mutex<StdSessionKeeper>,

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
        let mempool_db =
            MempoolDb::open(storage_context.clone()).context("failed to create mempool db")?;

        Ok(Self {
            cache: Default::default(),
            net_args: EngineNetworkArgs {
                key_pair,
                network: network.clone(),
                peer_resolver: peer_resolver.clone(),
                overlay_service: overlay_service.clone(),
            },
            keeper: Mutex::new(StdSessionKeeper::new(mempool_node_config)),
            mempool_db,
            input_buffer: InputBuffer::default(),
            top_known_anchor: RoundWatch::default(),
        })
    }

    async fn process_state_update(
        &self,
        keeper: &mut StdSessionKeeper,
        new_cx: &StateUpdateContext,
    ) -> Result<()> {
        // method is called in a for-cycle, so `seq_no` may differ
        let span = tracing::error_span!("tka", seq_no = new_cx.top_processed_to_anchor_id);

        let has_session_after_update = keeper.has_session_after_update(&self.cache, new_cx);

        if !has_session_after_update.instrument(span.clone()).await? {
            let _guard = span.enter();
            let merged_config = keeper.config_builder.build()?;
            keeper.engine_session = Some(self.start(&merged_config, new_cx)?);
        }
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

        let mut anchors_task = StdAnchorHandler::new(
            anchors_rx,
            commit_finished,
            &self.cache,
            &merged_conf.conf.consensus,
        )
        .run(self.mempool_db.clone())
        .boxed();

        tokio::spawn(async move {
            tokio::select! {
                handler_result = &mut anchors_task => match handler_result {
                    Err(error) => tracing::warn!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        "Mempool anchor handler stopped: {error}"
                    )
                },
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
