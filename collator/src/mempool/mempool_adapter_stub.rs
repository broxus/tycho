use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_types::cell::{CellBuilder, HashBytes};
use everscale_types::models::{ExtInMsgInfo, IntAddr, StdAddr};
use parking_lot::RwLock;
use rand::Rng;
use tycho_block_util::state::ShardStateStuff;

use super::types::{ExternalMessage, MempoolAnchor, MempoolAnchorId};
use crate::mempool::mempool_adapter::{MempoolAdapter, MempoolEventListener};
use crate::tracing_targets;

#[cfg(test)]
#[path = "tests/mempool_adapter_stub_tests.rs"]
pub(super) mod tests;

// FACTORY

pub struct MempoolAdapterStubImpl {
    _listener: Arc<dyn MempoolEventListener>,

    _stub_anchors_cache: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
}

impl MempoolAdapterStubImpl {
    pub fn new(listener: Arc<dyn MempoolEventListener>) -> Self {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");

        let stub_anchors_cache = Arc::new(RwLock::new(BTreeMap::new()));

        tokio::spawn({
            let listener = listener.clone();
            let stub_anchors_cache = stub_anchors_cache.clone();
            async move {
                let mut anchor_id = 0;
                loop {
                    let rnd_round_interval = rand::thread_rng().gen_range(400..600);
                    tokio::time::sleep(tokio::time::Duration::from_millis(rnd_round_interval * 5))
                        .await;
                    anchor_id += 1;
                    let anchor = _stub_create_random_anchor_with_stub_externals(anchor_id);
                    {
                        let mut anchor_cache_rw = stub_anchors_cache.write();
                        anchor_cache_rw.insert(anchor_id, anchor.clone());
                        tracing::debug!(
                            target: tracing_targets::MEMPOOL_ADAPTER,
                            "Random anchor (id: {}, chain_time: {}, externals: {}) was added to cache",
                            anchor.id(),
                            anchor.chain_time(),
                            anchor.externals_count(),
                        );
                    }
                    listener.on_new_anchor(anchor).await.unwrap();
                }
            }
        });
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Stub anchors generator started");

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool adapter created");

        Self {
            _listener: listener,
            _stub_anchors_cache: stub_anchors_cache,
        }
    }
}

#[async_trait]
impl MempoolAdapter for MempoolAdapterStubImpl {
    async fn enqueue_process_new_mc_block_state(&self, mc_state: ShardStateStuff) -> Result<()> {
        // TODO: make real implementation, currently does nothing
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
        stub_get_anchor_by_id(self._stub_anchors_cache.clone(), anchor_id).await
    }

    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<Arc<MempoolAnchor>> {
        stub_get_next_anchor(self._stub_anchors_cache.clone(), prev_anchor_id).await
    }

    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        let mut anchors_cache_rw = self._stub_anchors_cache.write();
        anchors_cache_rw.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }
}

pub(super) async fn stub_get_anchor_by_id(
    anchors_cache: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
    anchor_id: MempoolAnchorId,
) -> Result<Option<Arc<MempoolAnchor>>> {
    let mut first_attempt = true;
    let mut request_timer = std::time::Instant::now();

    loop {
        let res = {
            let anchors_cache_r = anchors_cache.read();

            anchors_cache_r.get(&anchor_id).cloned()
        };

        if res.is_some() {
            if first_attempt {
                tracing::debug!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    "Requested anchor (id: {}) found in local cache",
                    anchor_id,
                );
            } else {
                tracing::debug!(
                    target: tracing_targets::MEMPOOL_ADAPTER,
                    "STUB: Returned anchor (id: {}) from mempool (responded in {} ms)",
                    anchor_id,
                    request_timer.elapsed().as_millis(),
                );
            }
            return Ok(res);
        } else if first_attempt {
            tracing::debug!(
                target: tracing_targets::MEMPOOL_ADAPTER,
                "There is no required anchor (id: {}) in cache. STUB: Requested it from mempool. Waiting...",
                anchor_id,
            );
        }

        if first_attempt {
            request_timer = std::time::Instant::now();
        }
        first_attempt = false;
        tokio::time::sleep(tokio::time::Duration::from_millis(1020)).await;
    }
}

pub(super) async fn stub_get_next_anchor(
    anchors_cache: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
    prev_anchor_id: MempoolAnchorId,
) -> Result<Arc<MempoolAnchor>> {
    let mut stub_first_attempt = true;
    let mut request_timer = std::time::Instant::now();
    loop {
        {
            let anchors_cache_r = anchors_cache.read();

            let mut range = anchors_cache_r.range((
                std::ops::Bound::Excluded(prev_anchor_id),
                std::ops::Bound::Unbounded,
            ));

            if let Some((next_id, next)) = range.next() {
                if stub_first_attempt {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        "Found in cache next anchor (id: {}) after specified previous (id: {})",
                        next_id,
                        prev_anchor_id,
                    );
                } else {
                    tracing::debug!(
                        target: tracing_targets::MEMPOOL_ADAPTER,
                        "STUB: Returned next anchor (id: {}) after previous (id: {}) from mempool (responded in {} ms)",
                        next_id,
                        prev_anchor_id,
                        request_timer.elapsed().as_millis(),
                    );
                }
                return Ok(next.clone());
            } else if stub_first_attempt {
                tracing::debug!(
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

pub fn _stub_create_random_anchor_with_stub_externals(
    anchor_id: MempoolAnchorId,
) -> Arc<MempoolAnchor> {
    let chain_time = if anchor_id == 0 {
        2600
    } else {
        anchor_id as u64 * 2136 % 1000000000
    };
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
        let workchain_id = if i > 0 && i % 3 == 0 { -1 } else { 0 };
        let msg = ExternalMessage::new(msg_cell, ExtInMsgInfo {
            dst: IntAddr::Std(StdAddr::new(workchain_id, rand_addr)),
            ..Default::default()
        });
        externals.push(Arc::new(msg));
    }

    Arc::new(MempoolAnchor::new(anchor_id, chain_time, externals))
}
