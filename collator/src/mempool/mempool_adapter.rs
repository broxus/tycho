use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::boc::Boc;
use everscale_types::models::MsgInfo;
use everscale_types::prelude::Load;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tycho_block_util::state::ShardStateStuff;
use tycho_consensus::{InputBufferImpl, Point};
use tycho_network::{DhtClient, OverlayService, PeerId};
use tycho_util::FastHashSet;

use crate::mempool::external_message_cache::ExternalMessageCache;
use crate::mempool::types::ExternalMessage;
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;

#[cfg(test)]
#[path = "tests/mempool_adapter_tests.rs"]
pub(super) mod tests;

// FACTORY

pub trait MempoolAdapterFactory {
    type Adapter: MempoolAdapter;

    fn create(&self, listener: Arc<dyn MempoolEventListener>) -> Arc<Self::Adapter>;
}

impl<F, R> MempoolAdapterFactory for F
where
    F: Fn(Arc<dyn MempoolEventListener>) -> R,
    R: MempoolAdapter,
{
    type Adapter = R;

    fn create(&self, listener: Arc<dyn MempoolEventListener>) -> Arc<Self::Adapter> {
        Arc::new(self(listener))
    }
}

impl<T: MempoolAdapter> MempoolAdapterFactory for Arc<T> {
    type Adapter = T;

    fn create(&self, _listener: Arc<dyn MempoolEventListener>) -> Arc<Self::Adapter> {
        self.clone()
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

#[derive(Clone)]
pub struct MempoolAdapterStdImpl {
    // TODO: replace with rocksdb
    anchors: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
    externals_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
}

impl MempoolAdapterStdImpl {
    pub fn new(
        key_pair: Arc<KeyPair>,
        dht_client: DhtClient,
        overlay_service: OverlayService,
        peers: Vec<PeerId>,
    ) -> Arc<Self> {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");
        let anchors = Arc::new(RwLock::new(BTreeMap::new()));

        let (sender, receiver) =
            tokio::sync::mpsc::unbounded_channel::<(Arc<Point>, Vec<Arc<Point>>)>();

        // TODO receive from outside
        let (externals_tx, externals_rx) = mpsc::unbounded_channel();
        let mut engine = tycho_consensus::Engine::new(
            key_pair,
            &dht_client,
            &overlay_service,
            sender,
            InputBufferImpl::new(externals_rx),
        );

        tokio::spawn(async move {
            engine.init_with_genesis(&peers).await;
            engine.run().await;
        });

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool adapter created");

        let mempool_adapter = Arc::new(Self {
            anchors,
            externals_tx,
        });

        tokio::spawn(handle_anchors(mempool_adapter.clone(), receiver));

        mempool_adapter
    }

    pub fn send_external(&self, message: Bytes) {
        self.externals_tx.send(message).ok();
    }

    fn add_anchor(&self, anchor: Arc<MempoolAnchor>) {
        let mut guard = self.anchors.write();
        guard.insert(anchor.id(), anchor);
    }
}

pub async fn handle_anchors(
    adapter: Arc<MempoolAdapterStdImpl>,
    mut rx: UnboundedReceiver<(Arc<Point>, Vec<Arc<Point>>)>,
) {
    let mut cache = ExternalMessageCache::new(1000);
    while let Some((anchor, points)) = rx.recv().await {
        let mut repr_hashes = FastHashSet::default();
        let mut messages = Vec::new();

        for point in points {
            'message: for message in &point.body.payload {
                let cell = match Boc::decode(message) {
                    Ok(cell) => cell,
                    Err(e) => {
                        tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Failed to deserialize bytes into cell. Error: {e:?}"); // TODO: should handle errors properly?
                        continue 'message;
                    }
                };

                let mut slice = match cell.as_slice() {
                    Ok(slice) => slice,
                    Err(e) => {
                        tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Failed to make slice from cell. Error: {e:?}");
                        continue 'message;
                    }
                };

                let ext_in_message = match MsgInfo::load_from(&mut slice) {
                    Ok(MsgInfo::ExtIn(message)) => message,
                    Ok(info) => {
                        tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, ?info, "Bad message. Unexpected message variant");
                        continue 'message;
                    }
                    Err(e) => {
                        tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Bad cell. Failed to deserialize to ExtInMsgInfo. Err: {e:?}");
                        continue 'message;
                    }
                };

                if repr_hashes.insert(*cell.repr_hash()) {
                    messages.push(Arc::new(ExternalMessage::new(cell.clone(), ext_in_message)));
                }
            }
        }

        let added_messages = cache.add_dedup_messages(anchor.body.location.round.0, messages);

        let anchor = Arc::new(MempoolAnchor::new(
            anchor.body.location.round.0,
            anchor.body.time.as_u64(),
            added_messages,
        ));

        adapter.add_anchor(anchor);
    }
}

#[async_trait]
impl MempoolAdapter for MempoolAdapterStdImpl {
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
        // TODO: make real implementation, currently only return anchor from local cache
        let res = {
            let anchors_cache_r = self.anchors.read();

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
        // TODO: make real implementation, currently only return anchor from local cache

        let mut stub_first_attempt = true;
        let mut request_timer = std::time::Instant::now();
        loop {
            {
                let anchors_cache_r = self.anchors.read();

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
        let mut anchors_cache_rw = self.anchors.write();

        anchors_cache_rw.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }
}
