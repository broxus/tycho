mod adapter_impl;
mod anchor_handler;
use std::sync::Arc;
use anyhow::Result;
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;
use tycho_consensus::prelude::*;
use tycho_network::PeerId;
use tycho_util::futures::JoinTask;
use crate::mempool::StateUpdateContext;
use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::v_set_adapter::VSetAdapter;
use crate::mempool::impls::single_node_impl::anchor_handler::{
    ANCHOR_ID_STEP, SingleNodeAnchorHandler,
};
use crate::tracing_targets;
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
    pub fn new(
        mempool_node_config: &MempoolNodeConfig,
        local_peer_id: PeerId,
    ) -> Result<Self> {
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
        let mut interval = tokio::time::interval(
            std::time::Duration::from_millis(timeout),
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let input_buffer = self.input_buffer.clone();
        let mut anchor_handler = SingleNodeAnchorHandler::new(
            self.cache.clone(),
            self.local_peer_id,
            ctx.top_processed_to_anchor_id,
            &merged_conf.conf.consensus,
        );
        let v_set_len = VSetAdapter::init_peers(ctx)?.curr_v_set.len();
        if v_set_len == 2 {
            anyhow::bail!(
                "cannot run mempool with 2 nodes, gen network with either 1 or 3 nodes"
            );
        } else if v_set_len > 2 {
            anyhow::bail!("cannot run {v_set_len} nodes with `single-node` cli flag");
        }
        config_guard.inner_process = Some(
            JoinTask::new(async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    83u32,
                );
                scopeguard::defer!(
                    tracing::warn!(target : tracing_targets::MEMPOOL_ADAPTER,
                    "Single node Mempool task stopped")
                );
                loop {
                    __guard.checkpoint(89u32);
                    {
                        __guard.end_section(90u32);
                        let __result = interval.tick().await;
                        __guard.start_section(90u32);
                        __result
                    };
                    let external_messages = input_buffer.fetch(true);
                    anchor_handler = {
                        __guard.end_section(94u32);
                        let __result = anchor_handler.handle(external_messages).await;
                        __guard.start_section(94u32);
                        __result
                    };
                }
            }),
        );
        tracing::info!(
            target : tracing_targets::MEMPOOL_ADAPTER, "Single node Mempool task started"
        );
        Ok(())
    }
}
