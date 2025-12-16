use anyhow::{Context, Result};
use tycho_types::models::BlockId;

use crate::config::RpcConfig;
use crate::state::{RpcBlockSubscriber, RpcState, RpcStateSubscriber};

pub trait NodeBaseInitRpc: Send + Sync {
    fn init_simple_rpc_raw(
        &self,
        last_block_id: &BlockId,
        config: &RpcConfig,
    ) -> impl Future<Output = Result<RpcState>> + Send;

    fn init_simple_rpc(
        &self,
        last_block_id: &BlockId,
        config: &RpcConfig,
    ) -> impl Future<Output = Result<(RpcBlockSubscriber, RpcStateSubscriber)>> + Send {
        async move {
            self.init_simple_rpc_raw(last_block_id, config)
                .await
                .map(RpcState::split)
        }
    }

    fn init_simple_rpc_opt(
        &self,
        last_block_id: &BlockId,
        config: Option<&RpcConfig>,
    ) -> impl Future<Output = Result<(Option<RpcBlockSubscriber>, Option<RpcStateSubscriber>)>> + Send
    {
        async move {
            Ok(match config {
                Some(config) => Some(self.init_simple_rpc(last_block_id, config).await?).unzip(),
                None => (None, None),
            })
        }
    }
}

impl NodeBaseInitRpc for tycho_core::node::NodeBase {
    async fn init_simple_rpc_raw(
        &self,
        last_block_id: &BlockId,
        config: &RpcConfig,
    ) -> Result<RpcState> {
        let rpc_state = RpcState::builder()
            .with_config(config.clone())
            .with_storage(self.core_storage.clone())
            .with_blockchain_rpc_client(self.blockchain_rpc_client.clone())
            .with_zerostate_id(self.global_config.zerostate)
            .build()?;

        rpc_state.init(last_block_id).await?;

        let endpoint = rpc_state
            .bind_endpoint()
            .await
            .context("failed to setup RPC server endpoint")?;

        tracing::info!(listen_addr = %config.listen_addr, "RPC server started");
        tokio::task::spawn(async move {
            if let Err(e) = endpoint.serve().await {
                tracing::error!("RPC server failed: {e:?}");
            }
            tracing::info!("RPC server stopped");
        });

        Ok(rpc_state)
    }
}
