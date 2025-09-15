mod anchor_single_node_handler;

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;
use tycho_consensus::prelude::*;
use tycho_network::PeerId;
use tycho_types::models::{ConsensusConfig, GenesisInfo};
use tycho_util::futures::JoinTask;

use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::config::ConfigAdapter;
use crate::mempool::impls::single_node_impl::anchor_single_node_handler::{
    ANCHOR_ID_STEP, AnchorSingleNodeHandler,
};
use crate::mempool::{GetAnchorResult, MempoolAdapter, MempoolAnchorId, StateUpdateContext};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

pub struct MempoolAdapterSingleNodeImpl {
    cache: Arc<Cache>,
    config: Mutex<SingleNodeConfigAdapter>,
    local_peer_id: PeerId,

    input_buffer: InputBuffer,
}

struct SingleNodeConfigAdapter {
    builder: MempoolConfigBuilder,
    inner_process: Option<JoinTask<()>>,
}

impl MempoolAdapterSingleNodeImpl {
    pub fn new(mempool_node_config: &MempoolNodeConfig, local_peer_id: PeerId) -> Result<Self> {
        let config_builder = MempoolConfigBuilder::new(mempool_node_config);

        Ok(Self {
            cache: Default::default(),
            config: Mutex::new(SingleNodeConfigAdapter {
                builder: config_builder,
                inner_process: None,
            }),
            local_peer_id,
            input_buffer: InputBuffer::default(),
        })
    }
}

impl MempoolAdapterSingleNodeImpl {
    fn process_start(
        &self,
        config_guard: &mut SingleNodeConfigAdapter,
        ctx: &StateUpdateContext,
    ) -> Result<()> {
        let merged_conf = config_guard.builder.build()?;

        self.input_buffer.apply_config(&merged_conf.conf.consensus);

        let timeout = merged_conf.conf.consensus.broadcast_retry_millis.get() as u64
            * merged_conf.conf.consensus.min_sign_attempts.get() as u64
            * ANCHOR_ID_STEP as u64;

        let mut interval = tokio::time::interval(std::time::Duration::from_millis(timeout));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let input_buffer = self.input_buffer.clone();

        let mut anchor_handler = AnchorSingleNodeHandler::new(
            self.cache.clone(),
            self.local_peer_id,
            ctx.top_processed_to_anchor_id,
            &merged_conf.conf.consensus,
        );

        let v_set_len = ConfigAdapter::init_peers(ctx)?.curr_v_set.len();
        if v_set_len == 2 {
            anyhow::bail!("cannot run mempool with 2 nodes, gen network with either 1 or 3 nodes");
        } else if v_set_len > 2 {
            anyhow::bail!("cannot run {v_set_len} nodes with `single-node` cli flag");
        };

        config_guard.inner_process = Some(JoinTask::new(async move {
            scopeguard::defer!(tracing::warn!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                "Single node Mempool task stopped"
            ));

            loop {
                interval.tick().await;

                let external_messages = input_buffer.fetch(true);

                anchor_handler = anchor_handler.handle(external_messages).await;
            }
        }));

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Single node Mempool task started");

        Ok(())
    }
}

#[async_trait::async_trait]
impl MempoolAdapter for MempoolAdapterSingleNodeImpl {
    async fn handle_mc_state_update(&self, new_cx: StateUpdateContext) -> Result<()> {
        let mut config_guard = self.config.lock().await;

        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            full_id = %new_cx.mc_block_id,
            "Received state update from mc block",
        );

        let cfg = &new_cx.consensus_config;
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "handle_mc_state_update: consensus config={:?}", cfg
        );

        // we don't use state update queue and assume every block is signed by ourselves

        (config_guard.builder).set_genesis(new_cx.consensus_info.genesis_info);
        (config_guard.builder).set_consensus_config(&new_cx.consensus_config)?;

        if config_guard.inner_process.is_none() {
            self.process_start(&mut config_guard, &new_cx)?;
        }

        Ok(())
    }

    async fn handle_signed_mc_block(&self, mc_block_seqno: BlockSeqno) -> Result<()> {
        let _span = tracing::error_span!("mc_state_update", seq_no = mc_block_seqno).entered();
        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "call handle_signed_mc_block"
        );

        Ok(())
    }

    async fn get_anchor_by_id(&self, anchor_id: MempoolAnchorId) -> Result<GetAnchorResult> {
        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            %anchor_id,
            "get_anchor_by_id"
        );

        let result = match self.cache.get_anchor_by_id(anchor_id).await {
            Some(anchor) => GetAnchorResult::Exist(anchor),
            None => GetAnchorResult::NotExist,
        };

        Ok(result)
    }

    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<GetAnchorResult> {
        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
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

    fn accept_external(&self, message: Bytes) {
        self.input_buffer.push(message);
    }

    async fn update_delayed_config(
        &self,
        consensus_config: Option<&ConsensusConfig>,
        genesis_info: &GenesisInfo,
    ) -> Result<()> {
        let mut config_guard = self.config.lock().await;
        if let Some(consensus_config) = consensus_config {
            (config_guard.builder).set_consensus_config(consensus_config)?;
        } // else: will be set from mc state after sync

        config_guard.builder.set_genesis(*genesis_info);
        Ok(())
    }
}
