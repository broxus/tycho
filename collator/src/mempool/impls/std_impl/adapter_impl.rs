use anyhow::Result;
use bytes::Bytes;
use tycho_types::models::{ConsensusConfig, GenesisInfo};

use crate::mempool::{
    GetAnchorResult, MempoolAdapter, MempoolAdapterStdImpl, MempoolAnchorId, StateUpdateContext,
};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

#[async_trait::async_trait]
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

        if let Some(ctx) = config_guard.state_update_queue.push(new_cx)? {
            self.process_state_update(&mut config_guard, &ctx).await?;
            self.top_known_anchor
                .set_max_raw(ctx.top_processed_to_anchor_id);
        }

        Ok(())
    }

    async fn handle_signed_mc_block(&self, mc_block_seqno: BlockSeqno) -> Result<()> {
        let mut config_guard = self.config.lock().await;

        for ctx in config_guard.state_update_queue.signed(mc_block_seqno)? {
            self.process_state_update(&mut config_guard, &ctx).await?;
            let prev_top_id = self.top_known_anchor.get().0;
            self.top_known_anchor
                .set_max_raw(ctx.top_processed_to_anchor_id);
            let stats = (self.stats_rx)
                .get_stats(prev_top_id..=ctx.top_processed_to_anchor_id)
                .await;
            for (peer_id, stats) in stats {
                macro_rules! emit_counters {
                    ($prefix:literal, $stats:expr, $labels:expr, [ $($field:ident),* $(,)? ]) => {
                        $(
                            metrics::counter!(concat!($prefix, stringify!($field)), $labels)
                                .increment(u64::from($stats.$field));
                        )*
                    };
                }
                let labels = [("peer_id", format!("{:.4}", peer_id))];
                metrics::counter!("tycho_mempool_onchain_stats_filled_rounds", &labels)
                    .increment(u64::from(stats.filled_rounds()));
                if let Some(counters) = stats.counters() {
                    metrics::gauge!("tycho_mempool_onchain_stats_last_round", &labels)
                        .set(f64::from(counters.last_round));
                    emit_counters!("tycho_mempool_onchain_stats_", counters, &labels, [
                        was_leader,
                        was_not_leader,
                        skipped_rounds,
                        valid_points,
                        equivocated,
                        invalid_points,
                        ill_formed_points,
                        references_skipped,
                    ]);
                }
            }
        }
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
