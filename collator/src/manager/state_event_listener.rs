use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tycho_block_util::state::ShardStateStuff;
use tycho_types::models::{BlockId, ProcessedUptoInfo};

use crate::manager::metrics_report_last_applied_block_and_anchor;
use crate::state_node::StateNodeEventListener;

pub enum StateEvent {
    OwnBlockApplied {
        state: ShardStateStuff,
        processed_upto: ProcessedUptoInfo,
    },
    ExternalBlockApplied {
        mc_block_id: BlockId,
        state: ShardStateStuff,
        processed_upto: ProcessedUptoInfo,
    },
}

pub(super) struct ChannelStateEventListener {
    sender: mpsc::Sender<StateEvent>,
}

impl ChannelStateEventListener {
    pub(super) fn build(buffer: usize) -> (Arc<Self>, mpsc::Receiver<StateEvent>) {
        let (sender, receiver) = mpsc::channel(buffer);

        (Arc::new(Self { sender }), receiver)
    }
}

#[async_trait]
impl StateNodeEventListener for ChannelStateEventListener {
    async fn on_block_accepted(
        &self,
        _mc_block_id: &BlockId,
        state: &ShardStateStuff,
    ) -> Result<()> {
        let processed_upto = state.state().processed_upto.load()?;

        metrics_report_last_applied_block_and_anchor(state, &processed_upto)?;

        self.sender
            .send(StateEvent::OwnBlockApplied {
                state: state.clone(),
                processed_upto,
            })
            .await?;

        Ok(())
    }

    async fn on_block_accepted_external(
        &self,
        mc_block_id: &BlockId,
        state: &ShardStateStuff,
    ) -> Result<()> {
        let processed_upto = state.state().processed_upto.load()?;

        metrics_report_last_applied_block_and_anchor(state, &processed_upto)?;

        self.sender
            .send(StateEvent::ExternalBlockApplied {
                mc_block_id: *mc_block_id,
                state: state.clone(),
                processed_upto,
            })
            .await?;

        Ok(())
    }
}
