use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::boc::Boc;
use everscale_types::models::MsgInfo;
use everscale_types::prelude::Load;
use indexmap::IndexMap;
use parking_lot::RwLock;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, Notify};
use tycho_block_util::state::ShardStateStuff;
use tycho_consensus::{InputBufferImpl, Point};
use tycho_network::{DhtClient, OverlayService, PeerId};

use crate::mempool::external_message_cache::ExternalMessageCache;
use crate::mempool::types::ExternalMessage;
use crate::mempool::{MempoolAnchor, MempoolAnchorId};
use crate::tracing_targets;

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
    anchors: Arc<RwLock<IndexMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
    externals_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,

    anchor_added: Arc<tokio::sync::Notify>,
}

impl MempoolAdapterStdImpl {
    pub fn new(
        key_pair: Arc<KeyPair>,
        dht_client: DhtClient,
        overlay_service: OverlayService,
        peers: Vec<PeerId>,
    ) -> Arc<Self> {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");
        let anchors = Arc::new(RwLock::new(IndexMap::new()));

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
            anchor_added: Arc::new(Notify::new()),
        });

        tokio::spawn(handle_anchors(mempool_adapter.clone(), receiver));

        mempool_adapter
    }

    pub fn send_external(&self, message: Bytes) {
        self.externals_tx.send(message).ok();
    }

    fn add_anchor(&self, anchor: Arc<MempoolAnchor>) {
        {
            let mut guard = self.anchors.write();
            guard.insert(anchor.id(), anchor);
        }

        self.anchor_added.notify_waiters();
    }
}

impl MempoolAdapterFactory for Arc<MempoolAdapterStdImpl> {
    type Adapter = MempoolAdapterStdImpl;

    fn create(&self, _listener: Arc<dyn MempoolEventListener>) -> Arc<Self::Adapter> {
        self.clone()
    }
}

pub async fn handle_anchors(
    adapter: Arc<MempoolAdapterStdImpl>,
    mut rx: UnboundedReceiver<(Arc<Point>, Vec<Arc<Point>>)>,
) {
    let mut cache = ExternalMessageCache::new(1000);
    while let Some((anchor, points)) = rx.recv().await {
        let anchor_id: MempoolAnchorId = anchor.body.location.round.0;
        let mut messages = Vec::new();
        let mut total_messages = 0;
        let mut total_bytes = 0;
        let mut messages_bytes = 0;

        for point in points.iter() {
            total_messages += point.body.payload.len();
            'message: for message in &point.body.payload {
                total_bytes += message.len();
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

                if cache.check_unique(anchor_id, cell.repr_hash()) {
                    messages.push(Arc::new(ExternalMessage::new(cell.clone(), ext_in_message)));
                    messages_bytes += message.len();
                }
            }
        }

        metrics::gauge!("tycho_mempool_last_anchor_round").set(anchor.body.location.round.0);
        metrics::counter!("tycho_mempool_externals_count_total").increment(messages.len() as _);
        metrics::counter!("tycho_mempool_externals_bytes_total").increment(messages_bytes as _);
        metrics::counter!("tycho_mempool_duplicates_count_total")
            .increment((total_messages - messages.len()) as _);
        metrics::counter!("tycho_mempool_duplicates_bytes_total")
            .increment((total_bytes - messages_bytes) as _);

        tracing::info!(
            target: tracing_targets::MEMPOOL_ADAPTER,
            round = anchor_id,
            time = anchor.body.time.as_u64(),
            externals_unique = messages.len(),
            externals_skipped = total_messages - messages.len(),
            "new anchor"
        );

        let anchor = Arc::new(MempoolAnchor::new(
            anchor_id,
            anchor.body.time.as_u64(),
            messages,
        ));

        adapter.add_anchor(anchor);

        cache.clean(anchor_id);
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

        Ok(self.anchors.read().get(&anchor_id).cloned())
    }

    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<Arc<MempoolAnchor>> {
        // TODO: make real implementation, currently only return anchor from local cache
        loop {
            // NOTE: Subscribe to notification before checking
            let anchor_added = self.anchor_added.notified();

            {
                let anchors = self.anchors.read();

                match anchors.first() {
                    // Continue to wait for the first anchor on node start
                    None if prev_anchor_id == 0 => {}
                    // Return the first anchor on node start
                    Some((_, first)) if prev_anchor_id == 0 => return Ok(first.clone()),
                    // Trying to get anchor that is too old
                    Some((first_id, _)) if prev_anchor_id < *first_id => {
                        anyhow::bail!("Requested anchor {prev_anchor_id} is too old");
                    }
                    _ => {
                        // Find the index of the previous anchor
                        let Some(index) = anchors.get_index_of(&prev_anchor_id) else {
                            anyhow::bail!("Presented anchor {prev_anchor_id} is unknown");
                        };

                        // Try to get the next anchor
                        if let Some((_, value)) = anchors.get_index(index + 1) {
                            return Ok(value.clone());
                        }
                    }
                }
            }

            anchor_added.await;
        }
    }

    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        let mut anchors_cache_rw = self.anchors.write();

        anchors_cache_rw.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }
}
