use std::collections::hash_map;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use bytes::{Buf, Bytes};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use rand::RngCore;
use tl_proto::TlRead;
use tokio::sync::{broadcast, Semaphore};
use tokio::task::JoinHandle;
use tycho_util::realloc_box_enum;
use tycho_util::time::{now_sec, shifted_interval};

use self::query::{Query, StoreValue};
use self::routing::{RoutingTable, RoutingTableSource};
use self::storage::Storage;
use crate::network::{Network, WeakNetwork};
use crate::proto::dht::{
    rpc, NodeInfoResponse, NodeResponse, PeerValue, PeerValueKey, PeerValueKeyName,
    PeerValueKeyRef, PeerValueRef, Value, ValueRef, ValueResponseRaw,
};
use crate::types::{
    Address, PeerAffinity, PeerId, PeerInfo, Request, Response, Service, ServiceRequest,
};
use crate::util::{NetworkExt, Routable};

pub use self::config::DhtConfig;
pub use self::storage::{OverlayValueMerger, StorageError};

mod config;
mod query;
mod routing;
mod storage;

pub struct DhtClientBuilder {
    inner: Arc<DhtInner>,
    disable_background_tasks: bool,
}

impl DhtClientBuilder {
    pub fn disable_background_tasks(mut self) -> Self {
        self.disable_background_tasks = true;
        self
    }

    pub fn build(self, network: Network) -> DhtClient {
        if !self.disable_background_tasks {
            self.inner
                .start_background_tasks(Network::downgrade(&network));
        }

        DhtClient {
            inner: self.inner,
            network,
        }
    }
}

#[derive(Clone)]
pub struct DhtClient {
    inner: Arc<DhtInner>,
    network: Network,
}

impl DhtClient {
    pub fn network(&self) -> &Network {
        &self.network
    }

    pub fn add_peer(&self, peer: Arc<PeerInfo>) -> Result<bool> {
        self.inner
            .add_peer_info(&self.network, peer, RoutingTableSource::Trusted)
    }

    pub async fn get_node_info(&self, peer_id: &PeerId) -> Result<PeerInfo> {
        let res = self
            .network
            .query(peer_id, Request::from_tl(rpc::GetNodeInfo))
            .await?;
        let NodeInfoResponse { info } = res.parse_tl()?;
        Ok(info)
    }

    pub fn entry(&self, name: PeerValueKeyName) -> DhtQueryBuilder<'_> {
        DhtQueryBuilder {
            inner: &self.inner,
            network: &self.network,
            name,
            idx: 0,
        }
    }
}

#[derive(Clone, Copy)]
pub struct DhtQueryBuilder<'a> {
    inner: &'a DhtInner,
    network: &'a Network,
    name: PeerValueKeyName,
    idx: u32,
}

impl<'a> DhtQueryBuilder<'a> {
    #[inline]
    pub fn with_idx(&mut self, idx: u32) -> &mut Self {
        self.idx = idx;
        self
    }

    pub async fn find_value<T>(&self, peer_id: &PeerId) -> Result<T, FindValueError>
    where
        for<'tl> T: tl_proto::TlRead<'tl>,
    {
        let key_hash = tl_proto::hash(PeerValueKeyRef {
            name: self.name,
            peer_id,
        });

        match self.inner.find_value(self.network, &key_hash).await {
            Some(value) => match value.as_ref() {
                Value::Peer(value) => {
                    tl_proto::deserialize(&value.data).map_err(FindValueError::InvalidData)
                }
                Value::Overlay(_) => Err(FindValueError::InvalidData(
                    tl_proto::TlError::UnknownConstructor,
                )),
            },
            None => Err(FindValueError::NotFound),
        }
    }

    pub async fn find_peer_value_raw(
        &self,
        peer_id: &PeerId,
    ) -> Result<Box<PeerValue>, FindValueError> {
        let key_hash = tl_proto::hash(PeerValueKeyRef {
            name: self.name,
            peer_id,
        });

        match self.inner.find_value(self.network, &key_hash).await {
            Some(value) => {
                realloc_box_enum!(value, {
                    Value::Peer(value) => Box::new(value) => Ok(value),
                    Value::Overlay(_) => Err(FindValueError::InvalidData(
                        tl_proto::TlError::UnknownConstructor,
                    )),
                })
            }
            None => Err(FindValueError::NotFound),
        }
    }

    pub fn with_data<T>(&self, data: T) -> DhtQueryWithDataBuilder<'a>
    where
        T: tl_proto::TlWrite,
    {
        DhtQueryWithDataBuilder {
            inner: *self,
            data: tl_proto::serialize(&data),
            at: None,
            ttl: self.inner.config.max_stored_value_ttl.as_secs() as _,
            with_peer_info: false,
        }
    }
}

pub struct DhtQueryWithDataBuilder<'a> {
    inner: DhtQueryBuilder<'a>,
    data: Vec<u8>,
    at: Option<u32>,
    ttl: u32,
    with_peer_info: bool,
}

impl DhtQueryWithDataBuilder<'_> {
    pub fn with_time(&mut self, at: u32) -> &mut Self {
        self.at = Some(at);
        self
    }

    pub fn with_ttl(&mut self, ttl: u32) -> &mut Self {
        self.ttl = ttl;
        self
    }

    pub fn with_peer_info(&mut self, with_peer_info: bool) -> &mut Self {
        self.with_peer_info = with_peer_info;
        self
    }

    pub async fn store(&self) -> Result<()> {
        let dht = self.inner.inner;
        let network = self.inner.network;

        let mut value = PeerValueRef {
            key: PeerValueKeyRef {
                name: self.inner.name,
                peer_id: &dht.local_id,
            },
            data: &self.data,
            expires_at: self.at.unwrap_or_else(now_sec) + self.ttl,
            signature: &[0; 64],
        };
        let signature = network.sign_tl(&value);
        value.signature = &signature;

        dht.store_value(network, ValueRef::Peer(value), self.with_peer_info)
            .await
    }

    pub fn into_signed_value(self) -> PeerValue {
        let dht = self.inner.inner;
        let network = self.inner.network;

        let mut value = PeerValue {
            key: PeerValueKey {
                name: self.name,
                peer_id: dht.local_id,
            },
            data: self.data.into_boxed_slice(),
            expires_at: self.at.unwrap_or_else(now_sec) + self.ttl,
            signature: Box::new([0; 64]),
        };
        *value.signature = network.sign_tl(&value);
        value
    }
}

impl<'a> std::ops::Deref for DhtQueryWithDataBuilder<'a> {
    type Target = DhtQueryBuilder<'a>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> std::ops::DerefMut for DhtQueryWithDataBuilder<'a> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub struct DhtServiceBuilder {
    local_id: PeerId,
    config: Option<DhtConfig>,
    overlay_merger: Option<Arc<dyn OverlayValueMerger>>,
}

impl DhtServiceBuilder {
    pub fn with_config(mut self, config: DhtConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_overlay_value_merger<T: OverlayValueMerger>(mut self, merger: Arc<T>) -> Self {
        self.overlay_merger = Some(merger);
        self
    }

    pub fn build(self) -> (DhtClientBuilder, DhtService) {
        let config = self.config.unwrap_or_default();

        let storage = {
            let mut builder = Storage::builder()
                .with_max_capacity(config.max_storage_capacity)
                .with_max_ttl(config.max_stored_value_ttl);

            if let Some(time_to_idle) = config.storage_item_time_to_idle {
                builder = builder.with_max_idle(time_to_idle);
            }

            if let Some(ref merger) = self.overlay_merger {
                builder = builder.with_overlay_value_merger(merger);
            }

            builder.build()
        };

        let (announced_peers, _) = broadcast::channel(config.announced_peers_channel_capacity);

        let inner = Arc::new(DhtInner {
            local_id: self.local_id,
            routing_table: Mutex::new(RoutingTable::new(self.local_id)),
            storage,
            local_peer_info: Mutex::new(None),
            config,
            announced_peers,
        });

        let client_builder = DhtClientBuilder {
            inner: inner.clone(),
            disable_background_tasks: false,
        };

        (client_builder, DhtService(inner))
    }
}

#[derive(Clone)]
pub struct DhtService(Arc<DhtInner>);

impl DhtService {
    pub fn builder(local_id: PeerId) -> DhtServiceBuilder {
        DhtServiceBuilder {
            local_id,
            config: None,
            overlay_merger: None,
        }
    }
}

impl Service<ServiceRequest> for DhtService {
    type QueryResponse = Response;
    type OnQueryFuture = futures_util::future::Ready<Option<Self::QueryResponse>>;
    type OnMessageFuture = futures_util::future::Ready<()>;
    type OnDatagramFuture = futures_util::future::Ready<()>;

    #[tracing::instrument(
        level = "debug",
        name = "on_dht_query",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_query(&self, req: ServiceRequest) -> Self::OnQueryFuture {
        let (constructor, body) = match self.0.try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize query: {e:?}");
                return futures_util::future::ready(None);
            }
        };

        let response = crate::match_tl_request!(body, tag = constructor, {
            rpc::FindNode as ref r => {
                tracing::debug!(key = %PeerId::wrap(&r.key), k = r.k, "find_node");

                let res = self.0.handle_find_node(r);
                Some(tl_proto::serialize(res))
            },
            rpc::FindValue as ref r => {
                tracing::debug!(key = %PeerId::wrap(&r.key), k = r.k, "find_value");

                let res = self.0.handle_find_value(r);
                Some(tl_proto::serialize(res))
            },
            rpc::GetNodeInfo as _ => {
                tracing::debug!("get_node_info");

                self.0.handle_get_node_info().map(tl_proto::serialize)
            },
        }, e => {
            tracing::debug!("failed to deserialize query: {e:?}");
            None
        });

        futures_util::future::ready(response.map(|body| Response {
            version: Default::default(),
            body: Bytes::from(body),
        }))
    }

    #[tracing::instrument(
        level = "debug",
        name = "on_dht_message",
        skip_all,
        fields(peer_id = %req.metadata.peer_id, addr = %req.metadata.remote_address)
    )]
    fn on_message(&self, req: ServiceRequest) -> Self::OnMessageFuture {
        let (constructor, body) = match self.0.try_handle_prefix(&req) {
            Ok(rest) => rest,
            Err(e) => {
                tracing::debug!("failed to deserialize message: {e:?}");
                return futures_util::future::ready(());
            }
        };

        crate::match_tl_request!(body, tag = constructor, {
            rpc::StoreRef<'_> as ref r => {
                tracing::debug!("store");

                if let Err(e) = self.0.handle_store(r) {
                    tracing::debug!("failed to store value: {e:?}");
                }
            }
        }, e => {
            tracing::debug!("failed to deserialize message: {e:?}");
        });

        futures_util::future::ready(())
    }

    #[inline]
    fn on_datagram(&self, _req: ServiceRequest) -> Self::OnDatagramFuture {
        futures_util::future::ready(())
    }
}

impl Routable for DhtService {
    fn query_ids(&self) -> impl IntoIterator<Item = u32> {
        [
            rpc::WithPeerInfo::TL_ID,
            rpc::FindNode::TL_ID,
            rpc::FindValue::TL_ID,
            rpc::GetNodeInfo::TL_ID,
        ]
    }

    fn message_ids(&self) -> impl IntoIterator<Item = u32> {
        [rpc::WithPeerInfo::TL_ID, rpc::Store::TL_ID]
    }
}

struct DhtInner {
    local_id: PeerId,
    routing_table: Mutex<RoutingTable>,
    storage: Storage,
    local_peer_info: Mutex<Option<PeerInfo>>,
    config: DhtConfig,
    announced_peers: broadcast::Sender<Arc<PeerInfo>>,
}

impl DhtInner {
    fn start_background_tasks(self: &Arc<Self>, network: WeakNetwork) {
        enum Action {
            RefreshLocalPeerInfo,
            AnnounceLocalPeerInfo,
            RefreshRoutingTable,
            AddPeer(Arc<PeerInfo>),
        }

        let mut refresh_peer_info_interval =
            tokio::time::interval(self.config.local_info_refresh_period);
        let mut announce_peer_info_interval = shifted_interval(
            self.config.local_info_announce_period,
            self.config.max_local_info_announce_period_jitter,
        );
        let mut refresh_routing_table_interval = shifted_interval(
            self.config.routing_table_refresh_period,
            self.config.max_routing_table_refresh_period_jitter,
        );

        let mut announced_peers = self.announced_peers.subscribe();

        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            tracing::debug!("background DHT loop started");

            let mut prev_refresh_routing_table_fut = None::<JoinHandle<()>>;
            loop {
                let action = tokio::select! {
                    _ = refresh_peer_info_interval.tick() => Action::RefreshLocalPeerInfo,
                    _ = announce_peer_info_interval.tick() => Action::AnnounceLocalPeerInfo,
                    _ = refresh_routing_table_interval.tick() => Action::RefreshRoutingTable,
                    peer = announced_peers.recv() => match peer {
                        Ok(peer) => Action::AddPeer(peer),
                        Err(_) => continue,
                    }
                };

                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade()) else {
                    break;
                };

                match action {
                    Action::RefreshLocalPeerInfo => {
                        this.refresh_local_peer_info(&network);
                    }
                    Action::AnnounceLocalPeerInfo => {
                        // Always refresh peer info before announcing
                        this.refresh_local_peer_info(&network);
                        refresh_peer_info_interval.reset();

                        if let Err(e) = this.announce_local_peer_info(&network).await {
                            tracing::error!("failed to announce local DHT node info: {e:?}");
                        }
                    }
                    Action::RefreshRoutingTable => {
                        if let Some(fut) = prev_refresh_routing_table_fut.take() {
                            if let Err(e) = fut.await {
                                if e.is_panic() {
                                    std::panic::resume_unwind(e.into_panic());
                                }
                            }
                        }

                        prev_refresh_routing_table_fut = Some(tokio::spawn(async move {
                            this.refresh_routing_table(&network).await;
                        }));
                    }
                    Action::AddPeer(peer_info) => {
                        tracing::info!(peer_id = %peer_info.id, "received peer info");
                        if let Err(e) =
                            this.add_peer_info(&network, peer_info, RoutingTableSource::Untrusted)
                        {
                            tracing::error!("failed to add peer to the routing table: {e:?}");
                        }
                    }
                }
            }
            tracing::debug!("background DHT loop finished");
        });
    }

    fn refresh_local_peer_info(&self, network: &Network) {
        let peer_info = self.make_local_peer_info(network, now_sec());
        *self.local_peer_info.lock().unwrap() = Some(peer_info);
    }

    #[tracing::instrument(level = "debug", skip_all, fields(local_id = % self.local_id))]
    async fn announce_local_peer_info(&self, network: &Network) -> Result<()> {
        let data = tl_proto::serialize(&[network.local_addr().into()] as &[Address]);

        let mut value = self.make_unsigned_peer_value(
            PeerValueKeyName::NodeInfo,
            &data,
            now_sec() + self.config.max_peer_info_ttl.as_secs() as u32,
        );
        let signature = network.sign_tl(&value);
        value.signature = &signature;

        self.store_value(network, ValueRef::Peer(value), true).await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(local_id = % self.local_id))]
    async fn refresh_routing_table(&self, network: &Network) {
        const PARALLEL_QUERIES: usize = 3;
        const MAX_DISTANCE: usize = 15;
        const QUERY_DEPTH: usize = 3;

        // Prepare futures for each bucket
        let semaphore = Semaphore::new(PARALLEL_QUERIES);
        let mut futures = FuturesUnordered::new();
        {
            let rng = &mut rand::thread_rng();

            let mut routing_table = self.routing_table.lock().unwrap();

            // Filter out expired nodes
            let now = now_sec();
            for (_, bucket) in routing_table.buckets.range_mut(..=MAX_DISTANCE) {
                bucket.retain_nodes(|node| !node.is_expired(now, &self.config.max_peer_info_ttl));
            }

            // Iterate over the first buckets up until some distance (`MAX_DISTANCE`)
            // or up to the last non-empty bucket (?).
            for (&distance, bucket) in routing_table.buckets.range(..=MAX_DISTANCE).rev() {
                // TODO: Should we skip empty buckets?
                if bucket.is_empty() {
                    continue;
                }

                // Query the K closest nodes for a random ID at the specified distance from the local ID.
                let random_id = random_key_at_distance(&routing_table.local_id, distance, rng);
                let query = Query::new(
                    network.clone(),
                    &routing_table,
                    random_id.as_bytes(),
                    self.config.max_k,
                );

                futures.push(async {
                    let _permit = semaphore.acquire().await.unwrap();
                    query.find_peers(Some(QUERY_DEPTH)).await
                });
            }
        }

        // Receive initial set of peers
        let Some(mut peers) = futures.next().await else {
            tracing::debug!("no new peers found");
            return;
        };

        // Merge new peers into the result set
        while let Some(new_peers) = futures.next().await {
            for (peer_id, peer) in new_peers {
                match peers.entry(peer_id) {
                    // Just insert the peer if it's new
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(peer);
                    }
                    // Replace the peer if it's newer (by creation time)
                    hash_map::Entry::Occupied(mut entry) => {
                        if entry.get().created_at < peer.created_at {
                            entry.insert(peer);
                        }
                    }
                }
            }
        }

        let mut routing_table = self.routing_table.lock().unwrap();
        let mut count = 0usize;
        for peer in peers.into_values() {
            if peer.id == self.local_id {
                continue;
            }

            let is_new = routing_table.add(
                peer.clone(),
                self.config.max_k,
                &self.config.max_peer_info_ttl,
                RoutingTableSource::Trusted,
            );
            if is_new {
                network.known_peers().insert(peer, PeerAffinity::Allowed);
                count += 1;
            }
        }

        tracing::debug!(count, "found new peers");
    }

    async fn find_value(&self, network: &Network, key_hash: &[u8; 32]) -> Option<Box<Value>> {
        // TODO: deduplicate shared futures
        let query = Query::new(
            network.clone(),
            &self.routing_table.lock().unwrap(),
            key_hash,
            self.config.max_k,
        );

        // NOTE: expression is intentionally split to drop the routing table guard
        query.find_value().await
    }

    async fn store_value(
        &self,
        network: &Network,
        value: ValueRef<'_>,
        with_peer_info: bool,
    ) -> Result<()> {
        self.storage.insert(&value)?;

        let local_peer_info = if with_peer_info {
            let mut node_info = self.local_peer_info.lock().unwrap();
            Some(
                node_info
                    .get_or_insert_with(|| self.make_local_peer_info(network, now_sec()))
                    .clone(),
            )
        } else {
            None
        };

        let query = StoreValue::new(
            network.clone(),
            &self.routing_table.lock().unwrap(),
            value,
            self.config.max_k,
            local_peer_info.as_ref(),
        );

        // NOTE: expression is intentionally split to drop the routing table guard
        query.run().await;
        Ok(())
    }

    fn add_peer_info(
        &self,
        network: &Network,
        peer_info: Arc<PeerInfo>,
        source: RoutingTableSource,
    ) -> Result<bool> {
        anyhow::ensure!(peer_info.is_valid(now_sec()), "invalid peer info");

        if peer_info.id == self.local_id {
            return Ok(false);
        }

        let mut routing_table = self.routing_table.lock().unwrap();
        let is_new = routing_table.add(
            peer_info.clone(),
            self.config.max_k,
            &self.config.max_peer_info_ttl,
            source,
        );
        if is_new {
            network
                .known_peers()
                .insert(peer_info, PeerAffinity::Allowed);
        }
        Ok(is_new)
    }

    fn make_unsigned_peer_value<'a>(
        &'a self,
        name: PeerValueKeyName,
        data: &'a [u8],
        expires_at: u32,
    ) -> PeerValueRef<'a> {
        PeerValueRef {
            key: PeerValueKeyRef {
                name,
                peer_id: &self.local_id,
            },
            data,
            expires_at,
            signature: &[0; 64],
        }
    }

    fn make_local_peer_info(&self, network: &Network, now: u32) -> PeerInfo {
        let mut peer_info = PeerInfo {
            id: self.local_id,
            address_list: vec![network.local_addr().into()].into_boxed_slice(),
            created_at: now,
            expires_at: now + self.config.max_peer_info_ttl.as_secs() as u32,
            signature: Box::new([0; 64]),
        };
        *peer_info.signature = network.sign_tl(&peer_info);
        peer_info
    }

    fn try_handle_prefix<'a>(&self, req: &'a ServiceRequest) -> Result<(u32, &'a [u8])> {
        let mut body = req.as_ref();
        anyhow::ensure!(body.len() >= 4, tl_proto::TlError::UnexpectedEof);

        // NOTE: read constructor without advancing the body
        let mut constructor = std::convert::identity(body).get_u32_le();
        let mut offset = 0;

        if constructor == rpc::WithPeerInfo::TL_ID {
            let peer_info = rpc::WithPeerInfo::read_from(body, &mut offset)?.peer_info;
            anyhow::ensure!(
                peer_info.id == req.metadata.peer_id,
                "suggested peer ID does not belong to the sender"
            );
            self.announced_peers.send(peer_info).ok();

            body = &body[offset..];
            anyhow::ensure!(body.len() >= 4, tl_proto::TlError::UnexpectedEof);

            // NOTE: read constructor without advancing the body
            constructor = std::convert::identity(body).get_u32_le();
        }

        Ok((constructor, body))
    }

    fn handle_store(&self, req: &rpc::StoreRef<'_>) -> Result<bool, StorageError> {
        self.storage.insert(&req.value)
    }

    fn handle_find_node(&self, req: &rpc::FindNode) -> NodeResponse {
        let nodes = self
            .routing_table
            .lock()
            .unwrap()
            .closest(&req.key, (req.k as usize).min(self.config.max_k));

        NodeResponse { nodes }
    }

    fn handle_find_value(&self, req: &rpc::FindValue) -> ValueResponseRaw {
        if let Some(value) = self.storage.get(&req.key) {
            ValueResponseRaw::Found(value)
        } else {
            let nodes = self
                .routing_table
                .lock()
                .unwrap()
                .closest(&req.key, (req.k as usize).min(self.config.max_k));

            ValueResponseRaw::NotFound(nodes)
        }
    }

    fn handle_get_node_info(&self) -> Option<NodeInfoResponse> {
        self.local_peer_info
            .lock()
            .unwrap()
            .clone()
            .map(|info| NodeInfoResponse { info })
    }
}

fn random_key_at_distance(from: &PeerId, distance: usize, rng: &mut impl RngCore) -> PeerId {
    let mut result = *from;
    rng.fill_bytes(&mut result.0[distance..]);
    result
}

pub fn xor_distance(left: &PeerId, right: &PeerId) -> usize {
    for (i, (left, right)) in std::iter::zip(left.0.chunks(8), right.0.chunks(8)).enumerate() {
        let left = u64::from_be_bytes(left.try_into().unwrap());
        let right = u64::from_be_bytes(right.try_into().unwrap());
        let diff = left ^ right;
        if diff != 0 {
            return MAX_XOR_DISTANCE - (i * 64 + diff.leading_zeros() as usize);
        }
    }

    0
}

const MAX_XOR_DISTANCE: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum FindValueError {
    #[error("failed to deserialize value: {0}")]
    InvalidData(#[from] tl_proto::TlError),
    #[error("value not found")]
    NotFound,
}
