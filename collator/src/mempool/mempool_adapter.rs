use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::boc::Boc;
use everscale_types::cell::HashBytes;
use everscale_types::models::ExtInMsgInfo;
use everscale_types::prelude::Load;
use parking_lot::RwLock;
use tokio::sync::mpsc::UnboundedReceiver;
use tycho_block_util::state::ShardStateStuff;
use tycho_consensus::Point;
use tycho_network::{DhtClient, OverlayService};

use super::mempool_adapter_stub::{stub_get_anchor_by_id, stub_get_next_anchor};
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

pub struct MempoolAdapterFactoryStd {
    key_pair: Arc<KeyPair>,
    dht_client: DhtClient,
    overlay_service: OverlayService,
}

impl MempoolAdapterFactoryStd {
    pub fn new(
        key_pair: Arc<KeyPair>,
        dht_client: DhtClient,
        overlay_service: OverlayService,
    ) -> MempoolAdapterFactoryStd {
        Self {
            key_pair,
            dht_client,
            overlay_service,
        }
    }
}

impl MempoolAdapterFactory for MempoolAdapterFactoryStd {
    type Adapter = MempoolAdapterStdImpl;

    fn create(&self, _: Arc<dyn MempoolEventListener>) -> Arc<Self::Adapter> {
        MempoolAdapterStdImpl::new(
            self.key_pair.clone(),
            self.dht_client.clone(),
            self.overlay_service.clone(),
        )
    }
}

pub struct MempoolAdapterStdImpl {
    // TODO: replace with rocksdb
    anchors: Arc<RwLock<BTreeMap<MempoolAnchorId, Arc<MempoolAnchor>>>>,
}

impl MempoolAdapterStdImpl {
    pub fn new(
        key_pair: Arc<KeyPair>,
        dht_client: DhtClient,
        overlay_service: OverlayService,
    ) -> Arc<Self> {
        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Creating mempool adapter...");
        let anchors = Arc::new(RwLock::new(BTreeMap::new()));

        let (sender, receiver) =
            tokio::sync::mpsc::unbounded_channel::<(Arc<Point>, Vec<Arc<Point>>)>();

        tokio::spawn(async move {
            let mut engine =
                tycho_consensus::Engine::new(key_pair, &dht_client, &overlay_service, sender);
            // TODO replace with some sensible init before run
            engine.init_with_genesis(&[]).await;
            engine.run().await;
        });

        tracing::info!(target: tracing_targets::MEMPOOL_ADAPTER, "Mempool adapter created");

        let mempool_adapter = Arc::new(Self { anchors });

        // start handling mempool anchors
        tokio::spawn(parse_points(mempool_adapter.clone(), receiver));

        mempool_adapter
    }

    fn add_anchor(&self, anchor: Arc<MempoolAnchor>) {
        let mut guard = self.anchors.write();
        guard.insert(anchor.id(), anchor);
    }
}

pub async fn parse_points(
    adapter: Arc<MempoolAdapterStdImpl>,
    mut rx: UnboundedReceiver<(Arc<Point>, Vec<Arc<Point>>)>,
) {
    while let Some((anchor, points)) = rx.recv().await {
        let mut external_messages = HashMap::<HashBytes, ExternalMessage>::new();

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

                let ext_in_message = match ExtInMsgInfo::load_from(&mut slice) {
                    Ok(message) => message,
                    Err(e) => {
                        tracing::error!(target: tracing_targets::MEMPOOL_ADAPTER, "Bad cell. Failed to deserialize to ExtInMsgInfo. Err: {e:?}");
                        continue 'message;
                    }
                };

                let external_message = ExternalMessage::new(cell.clone(), ext_in_message);
                external_messages.insert(*cell.repr_hash(), external_message);
            }
        }

        let messages = external_messages
            .into_iter()
            .map(|m| Arc::new(m.1))
            .collect::<Vec<_>>();

        let anchor = Arc::new(MempoolAnchor::new(
            anchor.body.location.round.0,
            anchor.body.time.as_u64(),
            messages,
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

        stub_get_anchor_by_id(self.anchors.clone(), anchor_id).await
    }

    async fn get_next_anchor(&self, prev_anchor_id: MempoolAnchorId) -> Result<Arc<MempoolAnchor>> {
        // TODO: make real implementation, currently only return anchor from local cache

        stub_get_next_anchor(self.anchors.clone(), prev_anchor_id).await
    }

    async fn clear_anchors_cache(&self, before_anchor_id: MempoolAnchorId) -> Result<()> {
        let mut anchors_cache_rw = self.anchors.write();

        anchors_cache_rw.retain(|anchor_id, _| anchor_id >= &before_anchor_id);
        Ok(())
    }
}
