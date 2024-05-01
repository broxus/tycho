use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use everscale_types::{
    cell::{CellBuilder, CellSliceRange, HashBytes},
    models::{ExtInMsgInfo, IntAddr, MsgInfo, OwnedMessage, StdAddr},
};
use rand::Rng;
use tycho_block_util::state::ShardStateStuff;

use crate::tracing_targets;

use super::types::{MempoolAnchor, MempoolAnchorId};

#[cfg(test)]
#[path = "tests/mempool_adapter_tests.rs"]
pub(super) mod tests;

// FACTORY

pub trait MempoolAdapterFactory {
    type Adapter: MempoolAdapter;

    fn create(&self, listener: Arc<dyn MempoolEventListener>) -> Self::Adapter;
}

impl<F, R> MempoolAdapterFactory for F
where
    F: Fn(Arc<dyn MempoolEventListener>) -> R,
    R: MempoolAdapter,
{
    type Adapter = R;

    fn create(&self, listener: Arc<dyn MempoolEventListener>) -> Self::Adapter {
        self(listener)
    }
}

// EVENTS LISTENER

#[async_trait]
pub trait MempoolEventListener: Send + Sync {
    /// Process new anchor from mempool
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()>;
}

// ADAPTER

#[async_trait]
pub trait MempoolAdapter: Send + Sync + 'static {
    /// Schedule task to process new master block state (may perform gc or nodes rotation)
    async fn enqueue_process_new_mc_block_state(&self, mc_state: ShardStateStuff) -> Result<()>;

    /// Request, await, and return anchor from connected mempool by id.
    /// Return None if the requested anchor does not exist.
    ///
    /// (TODO) Cache anchor to handle similar request from collator of another shard
    async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>>;

    /// Request, await, and return the next anchor after the specified previous one.
    /// If anchor was not produced yet then await until mempool does this.
    ///
    /// (TODO) ? Should return Error if mempool does not reply fro a long timeout
    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<Arc<MempoolAnchor>>;

    /// Clean cache from all anchors that before specified.
    /// We can do this for anchors that processed in blocks
    /// which included in signed master - we do not need them anymore
    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()>;
}

pub struct MempoolAdapterStdImpl {
    listener: Arc<dyn MempoolEventListener>,

    _stub_anchors_cache: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
}

impl MempoolAdapterStdImpl {
    pub fn new(listener: Arc<dyn MempoolEventListener>) -> Self {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");

        //TODO: make real implementation, currently runs stub task
        //      that produces the repeating set of anchors
        let stub_anchors_cache = Arc::new(RwLock::new(BTreeMap::new()));

        tokio::spawn({
            let listener = listener.clone();
            let stub_anchors_cache = stub_anchors_cache.clone();
            async move {
                let mut anchor_id = 0;
                loop {
                    let rnd_round_interval = rand::thread_rng().gen_range(400..600);
                    tokio::time::sleep(tokio::time::Duration::from_millis(rnd_round_interval * 6))
                        .await;
                    anchor_id += 1;
                    let anchor = _stub_create_random_anchor_with_stub_externals(anchor_id);
                    {
                        let mut anchor_cache_rw = stub_anchors_cache
                            .write()
                            .map_err(|e| anyhow!("Poison error on write lock: {:?}", e))
                            .unwrap();
                        tracing::debug!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            "Random anchor (id: {}, chain_time: {}, externals: {}) added to cache",
                            anchor.id(),
                            anchor.chain_time(),
                            anchor.externals_count(),
                        );
                        anchor_cache_rw.insert(anchor_id, anchor.clone());
                    }
                    listener.on_new_anchor(anchor).await.unwrap();
                }
            }
        });
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Stub anchors generator started");

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool adapter created");

        Self {
            listener,
            _stub_anchors_cache: stub_anchors_cache,
        }
    }
}

#[async_trait]
impl MempoolAdapter for MempoolAdapterStdImpl {
    async fn enqueue_process_new_mc_block_state(&self, mc_state: ShardStateStuff) -> Result<()> {
        //TODO: make real implementation, currently does nothing
        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            "STUB: New masterchain state (block_id: {}) processing enqueued to mempool",
            mc_state.block_id().as_short_id(),
        );
        Ok(())
    }

    async fn get_anchor_by_id(
        &self,
        anchor_id: MempoolAnchorId,
    ) -> Result<Option<Arc<MempoolAnchor>>> {
        //TODO: make real implementation, currently only return anchor from local cache
        let res = {
            let anchors_cache_r = self
                ._stub_anchors_cache
                .read()
                .map_err(|e| anyhow!("Poison error on read lock: {:?}", e))?;
            anchors_cache_r.get(&anchor_id).cloned()
        };
        if res.is_some() {
            tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Requested anchor (id: {}) found in local cache", anchor_id);
        } else {
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                "Requested anchor (id: {}) was not found in local cache",
                anchor_id
            );
            tracing::trace!(target: tracing_targets::MEMPOOL_ADAPTER, "STUB: Requesting anchor (id: {}) in mempool...", anchor_id);
            let response_duration = tokio::time::Duration::from_millis(107);
            tokio::time::sleep(response_duration).await;
            tracing::info!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                "STUB: Requested anchor (id: {}) was not found in mempool (responded in {} ms)",
                anchor_id,
                response_duration.as_millis(),
            );
        }
        Ok(res)
    }

    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<Arc<MempoolAnchor>> {
        //TODO: make real implementation, currently only return anchor from local cache

        let mut stub_first_attempt = true;
        let mut request_timer = std::time::Instant::now();
        loop {
            {
                let anchors_cache_r = self
                    ._stub_anchors_cache
                    .read()
                    .map_err(|e| anyhow!("Poison error on read lock: {:?}", e))?;

                let mut range = anchors_cache_r.range((
                    std::ops::Bound::Excluded(prev_anchor_id),
                    std::ops::Bound::Unbounded,
                ));

                if let Some((next_id, next)) = range.next() {
                    if stub_first_attempt {
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            "Found in cache next anchor (id: {}) after specified previous (id: {})",
                            next_id,
                            prev_anchor_id,
                        );
                    } else {
                        tracing::info!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            "STUB: Returned next anchor (id: {}) after previous (id: {}) from mempool (responded in {} ms)",
                            next_id,
                            prev_anchor_id,
                            request_timer.elapsed().as_millis(),
                        );
                    }
                    return Ok(next.clone());
                } else if stub_first_attempt {
                    tracing::info!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        "There is no next anchor in cache after previous (id: {}). STUB: Requested it from mempool. Waiting...",
                        prev_anchor_id
                    );
                }
            }

            // stub waiting some time until new emulated anchors be added to cache
            if stub_first_attempt {
                request_timer = std::time::Instant::now();
            }
            stub_first_attempt = false;
            tokio::time::sleep(tokio::time::Duration::from_millis(1020)).await;
        }
    }

    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        let mut anchors_cache_rw = self
            ._stub_anchors_cache
            .write()
            .map_err(|e| anyhow!("Poison error on write lock: {:?}", e))?;
        anchors_cache_rw.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }
}

fn _stub_create_random_anchor_with_stub_externals(
    anchor_id: MempoolAnchorId,
) -> Arc<MempoolAnchor> {
    let chain_time = anchor_id as u64 * 471 * 6 % 1000000000;
    let externals_count = chain_time as i32 % 10;
    let mut externals = vec![];
    for i in 0..externals_count {
        let rand_addr = (0..32).map(|_| rand::random::<u8>()).collect::<Vec<u8>>();
        let rand_addr = HashBytes::from_slice(&rand_addr);
        let mut msg_cell_builder = CellBuilder::new();
        msg_cell_builder.store_u32(anchor_id).unwrap();
        msg_cell_builder.store_u64(chain_time).unwrap();
        msg_cell_builder.store_u32(i as u32).unwrap();
        let msg_cell = msg_cell_builder.build().unwrap();
        let msg_cell_range = CellSliceRange::full(&*msg_cell);
        let msg = OwnedMessage {
            info: MsgInfo::ExtIn(ExtInMsgInfo {
                dst: IntAddr::Std(StdAddr::new(0, rand_addr)),
                ..Default::default()
            }),
            body: (msg_cell, msg_cell_range),
            init: None,
            layout: None,
        };
        externals.push(Arc::new(msg));
    }

    Arc::new(MempoolAnchor::new(anchor_id, chain_time, externals))
}
