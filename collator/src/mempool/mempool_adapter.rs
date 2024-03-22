use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use tycho_block_util::state::ShardStateStuff;

use super::types::{MempoolAnchor, MempoolAnchorId};

// EVENTS EMITTER AMD LISTENER

#[async_trait]
pub(crate) trait MempoolEventEmitter {
    /// When mempool produced new committed anchor
    async fn on_new_anchor_event(&self, anchor: Arc<MempoolAnchor>);
}

#[async_trait]
pub(crate) trait MempoolEventListener: Send + Sync {
    /// Process new anchor from mempool
    async fn on_new_anchor(&self, anchor: Arc<MempoolAnchor>) -> Result<()>;
}

// ADAPTER

#[async_trait]
pub(crate) trait MempoolAdapter: Send + Sync + 'static {
    /// Create an adapter, that connects to mempool then starts to listen mempool for new anchors,
    /// and handles requests to mempool from the collation process
    fn create(listener: Arc<dyn MempoolEventListener>) -> Self;

    /// Schedule task to process new master block state (may perform gc or nodes rotation)
    async fn enqueue_process_new_mc_block_state(
        &self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()>;

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

#[async_trait]
impl MempoolAdapter for MempoolAdapterStdImpl {
    fn create(listener: Arc<dyn MempoolEventListener>) -> Self {
        //TODO: make real implementation, currently runs stub task
        //      that produces the repeating set of anchors
        Self {
            listener,

            _stub_anchors_cache: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    async fn enqueue_process_new_mc_block_state(
        &self,
        mc_state: Arc<ShardStateStuff>,
    ) -> Result<()> {
        //TODO: make real implementation, currently does nothing
        tracing::trace!(
            "New masterchain state (block_id: {}) processing enqueued to mempool",
            mc_state.block_id()
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
                .map_err(|e| anyhow!("Poison error on write lock: {:?}", e))?;
            anchors_cache_r.get(&anchor_id).cloned()
        };
        if res.is_some() {
            tracing::info!("Requested anchor (id: {}) found in local cache", anchor_id);
        } else {
            tracing::trace!("Requesting anchor (id: {}) in mempool...", anchor_id);
            let response_duration = tokio::time::Duration::from_millis(107);
            tokio::time::sleep(response_duration).await;
            tracing::info!(
                "Requested anchor (id: {}) was not found in mempool (responded in {} ms)",
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
                    .map_err(|e| anyhow!("Poison error on write lock: {:?}", e))?;

                let mut range = anchors_cache_r.range((
                    std::ops::Bound::Excluded(prev_anchor_id),
                    std::ops::Bound::Unbounded,
                ));

                if let Some((next_id, next)) = range.next() {
                    if stub_first_attempt {
                        tracing::info!(
                            "Found in cache next anchor (id: {}) after specified previous (id: {})",
                            next_id,
                            prev_anchor_id,
                        );
                    } else {
                        tracing::info!(
                            "Returned next anchor (id: {}) after previous (id: {}) from mempool (responded in {} ms)",
                            next_id,
                            prev_anchor_id,
                            request_timer.elapsed().as_millis(),
                        );
                    }
                    return Ok(next.clone());
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
