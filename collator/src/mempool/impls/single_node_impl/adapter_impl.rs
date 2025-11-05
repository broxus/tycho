use anyhow::Result;
use bytes::Bytes;
use tycho_types::models::{ConsensusConfig, GenesisInfo};
use crate::mempool::{
    GetAnchorResult, MempoolAdapter, MempoolAdapterSingleNodeImpl, MempoolAnchorId,
    StateUpdateContext,
};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;
#[async_trait::async_trait]
impl MempoolAdapter for MempoolAdapterSingleNodeImpl {
    async fn handle_mc_state_update(&self, new_cx: StateUpdateContext) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_mc_state_update)),
            file!(),
            14u32,
        );
        let new_cx = new_cx;
        let mut config_guard = {
            __guard.end_section(15u32);
            let __result = self.config.lock().await;
            __guard.start_section(15u32);
            __result
        };
        tracing::debug!(
            target : tracing_targets::MEMPOOL_ADAPTER, full_id = % new_cx.mc_block_id,
            "Received state update from mc block",
        );
        let cfg = &new_cx.consensus_config;
        tracing::info!(
            target : tracing_targets::MEMPOOL_ADAPTER,
            "handle_mc_state_update: consensus config={:?}", cfg
        );
        (config_guard.builder).set_genesis(new_cx.consensus_info.genesis_info);
        (config_guard.builder).set_consensus_config(&new_cx.consensus_config)?;
        if config_guard.inner_process.is_none() {
            self.process_start(&mut config_guard, &new_cx)?;
        }
        Ok(())
    }
    async fn handle_signed_mc_block(&self, mc_block_seqno: BlockSeqno) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(handle_signed_mc_block)),
            file!(),
            41u32,
        );
        let mc_block_seqno = mc_block_seqno;
        let _span = tracing::error_span!("mc_state_update", seq_no = mc_block_seqno)
            .entered();
        tracing::debug!(
            target : tracing_targets::MEMPOOL_ADAPTER, "call handle_signed_mc_block"
        );
        Ok(())
    }
    async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Result<GetAnchorResult> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_anchor_by_id)),
            file!(),
            51u32,
        );
        let anchor_id = anchor_id;
        tracing::debug!(
            target : tracing_targets::MEMPOOL_ADAPTER, % anchor_id, "get_anchor_by_id"
        );
        let result = match {
            __guard.end_section(58u32);
            let __result = self.cache.get_anchor_by_id(anchor_id).await;
            __guard.start_section(58u32);
            __result
        } {
            Some(anchor) => GetAnchorResult::Exist(anchor),
            None => GetAnchorResult::NotExist,
        };
        Ok(result)
    }
    async fn get_next_anchor(
        &self,
        prev_anchor_id: MempoolAnchorId,
    ) -> Result<GetAnchorResult> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(get_next_anchor)),
            file!(),
            66u32,
        );
        let prev_anchor_id = prev_anchor_id;
        tracing::debug!(
            target : tracing_targets::MEMPOOL_ADAPTER, % prev_anchor_id,
            "get_next_anchor"
        );
        let result = match {
            __guard.end_section(73u32);
            let __result = self.cache.get_next_anchor(prev_anchor_id).await;
            __guard.start_section(73u32);
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
    fn accept_external(&self, message: Bytes) {
        self.input_buffer.push(message);
    }
    async fn update_delayed_config(
        &self,
        consensus_config: Option<&ConsensusConfig>,
        genesis_info: &GenesisInfo,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(update_delayed_config)),
            file!(),
            94u32,
        );
        let consensus_config = consensus_config;
        let genesis_info = genesis_info;
        let mut config_guard = {
            __guard.end_section(95u32);
            let __result = self.config.lock().await;
            __guard.start_section(95u32);
            __result
        };
        if let Some(consensus_config) = consensus_config {
            (config_guard.builder).set_consensus_config(consensus_config)?;
        }
        config_guard.builder.set_genesis(*genesis_info);
        Ok(())
    }
}
