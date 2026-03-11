use anyhow::{Context, Result};
use tycho_consensus::prelude::{
    EngineSession, GenesisInfoExt, MempoolConfigBuilder, MempoolNodeConfig,
};

use crate::mempool::impls::common::cache::Cache;
use crate::mempool::impls::common::v_set_adapter::VSetAdapter;
use crate::mempool::impls::std_impl::state_update_queue::StateUpdateQueue;
use crate::mempool::{DebugStateUpdateContext, StateUpdateContext};
use crate::tracing_targets;
use crate::types::processed_upto::BlockSeqno;

pub struct StdSessionKeeper {
    pub config_builder: MempoolConfigBuilder,
    pub state_update_queue: StateUpdateQueue,
    pub engine_session: Option<EngineSession>,
    expect_genesis_change: Option<BlockSeqno>,
}

impl StdSessionKeeper {
    pub fn new(mempool_node_config: &MempoolNodeConfig) -> Self {
        Self {
            config_builder: MempoolConfigBuilder::new(mempool_node_config),
            state_update_queue: Default::default(),
            engine_session: None,
            expect_genesis_change: None,
        }
    }

    /// may be called even on non-yet-signed blocks
    pub fn check_expect_genesis_change(
        &mut self,
        cache: &Cache,
        new_cx: &StateUpdateContext,
    ) -> Result<()> {
        let Some(session) = self.engine_session.as_ref() else {
            return Ok(());
        };
        if !(new_cx.consensus_info.genesis_info).overrides(&session.genesis_info()) {
            return Ok(());
        };
        if (self.expect_genesis_change).is_some_and(|x| new_cx.mc_block_id.seqno >= x) {
            // if genesis change is cancelled in A' after A, mempool will restart with no changes
            return Ok(()); // support BAB' only: may reset to a lower value, don't if GEQ
        }

        let span = tracing::error_span!(
            "expect_genesis_change",
            tka = new_cx.top_processed_to_anchor_id
        );
        let _guard = span.enter();

        self.expect_genesis_change = Some(new_cx.mc_block_id.seqno);
        cache.close(new_cx.consensus_info.genesis_info.start_round_aligned());

        tracing::warn!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            seqno = %new_cx.mc_block_id.seqno,
            new_cx = ?DebugStateUpdateContext(new_cx),
            current = ?session.genesis_info(),
            "Mempool anchor cache closed for reading anchors after TKA",
        );

        Ok(())
    }

    pub async fn has_session_after_update(
        &mut self,
        cache: &Cache,
        new_cx: &StateUpdateContext,
    ) -> Result<bool> {
        if let Some(session) = self.engine_session.as_ref() {
            tracing::debug!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                seqno = %new_cx.mc_block_id.seqno,
                new_cx = ?DebugStateUpdateContext(new_cx),
                "Processing state update from mc block",
            );

            let apply_ctx_genesis =
                (new_cx.consensus_info.genesis_info).overrides(&session.genesis_info());

            if (self.expect_genesis_change).is_some_and(|x| new_cx.mc_block_id.seqno >= x) {
                // Collator may collate version with old genesis and sign with new genesis,
                // but won't change its mind (reversed) from new genesis to old (or any other) one.
                // Because of its workflow on consensus config change:
                // - collate mb1
                // - block mb1 is validated
                // - StateUpdate from mb1
                // - try to collate next mb2 and try to get next anchor
                // - block mb1 stored
                // - notify top_processed_to from mb1
                // That differs from its regular workflow:
                // - collate mb1
                // - StateUpdate from mb1
                // - try to collate next mb2 and try to get next anchor
                // - block mb1 is validated
                // - block mb1 stored
                // - notify top_processed_to from mb1
                self.expect_genesis_change = None;
                // regardless mempool restart: cannot use TKA to drop cache, it may be too old
                if let Some(after_anchor_id) = cache.reopen(apply_ctx_genesis) {
                    tracing::warn!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        seqno = %new_cx.mc_block_id.seqno,
                        drop_data = apply_ctx_genesis,
                        ?after_anchor_id,
                        "Mempool anchor cache reopened for reading anchors after TKA",
                    );
                } else {
                    tracing::error!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        seqno = %new_cx.mc_block_id.seqno,
                        ?apply_ctx_genesis,
                        "Mempool anchor cache was not closed to be reopened for reading anchors",
                    );
                }
            }

            // when genesis doesn't change - just (re-)schedule v_set change as defined by collator
            if new_cx.consensus_info.genesis_info == session.genesis_info() {
                session.set_peers(VSetAdapter::init_peers(new_cx)?);
                return Ok(true);
            }

            // rare case when node starts with empty DB to sync and genesis in GlobalConfig
            if !apply_ctx_genesis {
                tracing::warn!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    seqno = %new_cx.mc_block_id.seqno,
                    new_cx = ?DebugStateUpdateContext(new_cx),
                    current = ?session.genesis_info(),
                    "Ignoring new genesis: it does not override current, node state was deleted?",
                );
                return Ok(true);
            }

            // Genesis is changed at runtime - restart immediately:
            // block is signed by majority, so old mempool session and its anchors are not needed

            let session =
                (self.engine_session.take()).context("cannot happen: engine must be started")?;

            session.stop().await;

            // a new genesis is created even when overlay-related part of config stays the same
            self.config_builder
                .set_genesis(new_cx.consensus_info.genesis_info);
            // so config simultaneously changes with genesis via mempool restart
            self.config_builder
                .set_consensus_config(&new_cx.consensus_config)?;

            return Ok(false);
        }

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            seqno = %new_cx.mc_block_id.seqno,
            new_cx = ?DebugStateUpdateContext(new_cx),
            "Will start mempool with state update from mc block"
        );

        if let Some(genesis_override) = (self.config_builder.get_genesis())
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
            let message = match self.config_builder.get_consensus_config() {
                Some(cc) if cc == &new_cx.consensus_config => {
                    "consensus config from global config is the same as in mc block"
                }
                Some(_) => "consensus config from global config overrides one from mc block",
                None => {
                    (self.config_builder).set_consensus_config(&new_cx.consensus_config)?;
                    "no consensus config in global config, using one from mc block"
                }
            };
            // "message" is a reserved field in macro
            tracing::warn!(target: tracing_targets::MEMPOOL_ADAPTER, message);
        } else {
            (self.config_builder).set_genesis(new_cx.consensus_info.genesis_info);
            (self.config_builder).set_consensus_config(&new_cx.consensus_config)?;
        };

        Ok(false)
    }
}
