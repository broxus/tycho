use std::sync::Arc;

use bytes::Bytes;
use everscale_types::models::BlockId;
use futures_util::{StreamExt, TryStreamExt};
use parking_lot::Mutex;
use tycho_storage::Storage;

use crate::blockchain_rpc::BlockchainRpcClient;
use crate::proto::blockchain::PersistentStatePart;

const MAX_REQUEST_SIZE: u64 = 1 << 20;

pub struct StateDownloader {
    storage: Storage,
    blockchain_rpc_client: BlockchainRpcClient,
    parallel_requests: usize,
}

impl StateDownloader {
    pub fn new(
        storage: Storage,
        blockchain_rpc_client: BlockchainRpcClient,
        parallel_requests: usize,
    ) -> Self {
        Self {
            storage,
            blockchain_rpc_client,
            parallel_requests,
        }
    }

    pub async fn download_and_save_state(
        &self,
        block_id: &BlockId,
        mc_block_id: &BlockId,
    ) -> anyhow::Result<()> {
        let stream =
            futures_util::stream::iter((0..).step_by(MAX_REQUEST_SIZE as usize))
                .map(|offset| {
                    let blockchain_rpc_client = self.blockchain_rpc_client.clone();
                    async move {
                        download_part(blockchain_rpc_client, mc_block_id, block_id, offset).await
                    }
                })
                .buffered(self.parallel_requests);

        let state_store_op = self
            .storage
            .shard_state_storage()
            .begin_store_state_raw(block_id)?;

        // you can use &mut state_store_op, but we don't want to block runtime
        let state_store_op = Arc::new(Mutex::new(state_store_op));

        // not using while let because it will make polling switch between download and store futures,
        // and we want to do all of this stuff concurrently
        let mut stream = stream.try_filter_map(|x| {
            let state_store_op = Arc::clone(&state_store_op);
            async move {
                match x {
                    Some(bytes) => {
                        let res = tokio::task::spawn_blocking(move || {
                            let mut state_store_op = state_store_op.lock();
                            state_store_op.process_part(bytes)
                        })
                        .await
                        .expect("tokio::task::spawn_blocking failed");
                        res.map(Some)
                    }
                    None => Ok(None),
                }
            }
        });

        let mut stream = std::pin::pin!(stream);
        while stream.next().await.is_some() {}

        Ok(())
    }
}

async fn download_part(
    blockchain_rpc_client: BlockchainRpcClient,
    mc_block_id: &BlockId,
    block_id: &BlockId,
    offset: u64,
) -> anyhow::Result<Option<Bytes>> {
    let state_part = blockchain_rpc_client
        .get_persistent_state_part(block_id, mc_block_id, offset, MAX_REQUEST_SIZE)
        .await?;

    let data = match state_part.data() {
        PersistentStatePart::Found { data } => data.clone(),
        PersistentStatePart::NotFound => {
            return Ok(None);
        }
    };

    Ok(Some(data))
}
