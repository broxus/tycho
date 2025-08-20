mod anchor_single_node_handler;

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use anyhow::Result;
use bytes::Bytes;
use futures_util::FutureExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tycho_consensus::prelude::*;
use tycho_network::PeerId;
use tycho_types::models::{ConsensusConfig, GenesisInfo};

use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::config::ConfigAdapter;
use crate::mempool::impls::single_node_impl::anchor_single_node_handler::{
    ANCHOR_ID_DEFAULT, ANCHOR_ID_STEP, AnchorSingleNodeHandler,
};
use crate::mempool::{GetAnchorResult, MempoolAdapter};
use crate::tracing_targets;

pub struct MempoolAdapterSingleNodeImpl(Arc<Inner>);

struct Inner {
    cache: Arc<Cache>,
    config: Mutex<ConfigAdapter>,
    local_peer_id: PeerId,

    unprocessed_message_queue: InputBuffer,
    anchor_id: AtomicU32,

    once_flag: std::sync::Once,
    join_handle: Mutex<Cell<Option<JoinHandle<()>>>>,
}

impl Inner {
    pub async fn start_process(&self) {
        scopeguard::defer! {
            tracing::info!("mempool background task stopped");
        }

        let consensus_config = {
            let config_guard = self.config.lock().await;
            let consensus_config = config_guard
                .builder
                .get_consensus_config()
                .expect("There is no consensus config.");
            consensus_config.clone()
        };
        self.unprocessed_message_queue
            .apply_config(&consensus_config);

        let timeout = (consensus_config.broadcast_retry_millis
            * (consensus_config.broadcast_retry_attempts as u16)) as u64;

        let mut interval = tokio::time::interval(std::time::Duration::from_millis(timeout));

        loop {
            interval.tick().await;

            let external_messages = self.unprocessed_message_queue.fetch(true);

            let anchor_id = self
                .anchor_id
                .fetch_add(ANCHOR_ID_STEP, std::sync::atomic::Ordering::Relaxed);

            let mut anchor_task =
                AnchorSingleNodeHandler::new(&consensus_config, self.local_peer_id, anchor_id)
                    .run(self.cache.clone(), external_messages)
                    .boxed();

            tokio::select! {
                () = &mut anchor_task => {}, // just poll
            }

            tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool started");
        }
    }
}

impl MempoolAdapterSingleNodeImpl {
    pub fn new(mempool_node_config: &MempoolNodeConfig, local_peer_id: &PeerId) -> Result<Self> {
        let config_builder = MempoolConfigBuilder::new(mempool_node_config);
        Ok(Self(Arc::new(Inner {
            cache: Default::default(),
            config: Mutex::new(ConfigAdapter {
                builder: config_builder,
                state_update_queue: Default::default(),
                engine_session: None,
            }),
            local_peer_id: local_peer_id.clone(),
            unprocessed_message_queue: InputBuffer::default(),
            anchor_id: AtomicU32::new(ANCHOR_ID_DEFAULT),
            once_flag: std::sync::Once::new(),
            join_handle: Mutex::new(Cell::new(None)),
        })))
    }
}

#[async_trait::async_trait]
impl MempoolAdapter for MempoolAdapterSingleNodeImpl {
    async fn handle_mc_state_update(
        &self,
        new_cx: crate::mempool::StateUpdateContext,
    ) -> anyhow::Result<()> {
        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            full_id = %new_cx.mc_block_id,
            "Received state update from mc block",
        );

        {
            let cfg = &new_cx.consensus_config;
            tracing::info!("handle_mc_state_update: consensus config={:?}", cfg);

            let mut config_guard = self.0.config.lock().await;
            config_guard
                .builder
                .set_consensus_config(&new_cx.consensus_config)?;
        }

        self.0.once_flag.call_once(|| {
            let inner = self.0.clone();
            let join_handle = tokio::spawn(async move {
                let proccess_task = inner.start_process().boxed();
                tokio::select! {
                    () = proccess_task => {},
                }
            });

            let inner = self.0.clone();
            tokio::spawn(async move {
                let guard = inner.join_handle.lock().await;
                guard.replace(Some(join_handle));
            });
        });

        tracing::info!("future for start_process was created");

        Ok(())
    }

    async fn handle_signed_mc_block(
        &self,
        _mc_block_seqno: crate::types::processed_upto::BlockSeqno,
    ) -> anyhow::Result<()> {
        tracing::info!("call handle_signed_mc_block");
        Ok(())
    }

    async fn get_anchor_by_id(
        &self,
        anchor_id: crate::mempool::MempoolAnchorId,
    ) -> anyhow::Result<crate::mempool::GetAnchorResult> {
        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            %anchor_id,
            "get_anchor_by_id"
        );

        let result = match self.0.cache.get_anchor_by_id(anchor_id).await {
            Some(anchor) => GetAnchorResult::Exist(anchor),
            None => GetAnchorResult::NotExist,
        };

        Ok(result)
    }

    async fn get_next_anchor(
        &self,
        prev_anchor_id: crate::mempool::MempoolAnchorId,
    ) -> anyhow::Result<crate::mempool::GetAnchorResult> {
        tracing::debug!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            %prev_anchor_id,
            "get_next_anchor"
        );

        let result = match self.0.cache.get_next_anchor(prev_anchor_id).await? {
            Some(anchor) => GetAnchorResult::Exist(anchor),
            None => GetAnchorResult::NotExist,
        };

        Ok(result)
    }

    fn clear_anchors_cache(
        &self,
        before_anchor_id: crate::mempool::MempoolAnchorId,
    ) -> anyhow::Result<()> {
        self.0.cache.clear(before_anchor_id);
        Ok(())
    }

    fn send_external(&self, message: Bytes) {
        self.0.unprocessed_message_queue.push(message);
    }

    async fn update_delayed_config(
        &self,
        consensus_config: Option<&ConsensusConfig>,
        genesis_info: &GenesisInfo,
    ) -> Result<()> {
        let mut config_guard = self.0.config.lock().await;
        if let Some(consensus_config) = consensus_config {
            config_guard
                .builder
                .set_consensus_config(consensus_config)?;
        } // else: will be set from mc state after sync

        config_guard.builder.set_genesis(*genesis_info);
        Ok::<_, anyhow::Error>(())
    }
}
