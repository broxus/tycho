use std::collections::hash_map;
use std::sync::Arc;
use anyhow::Result;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinHandle;
use tycho_util::time::{now_sec, shifted_interval};
use crate::dht::{DhtInner, DhtQueryMode, Query, random_key_at_distance};
use crate::network::{Network, WeakNetwork};
use crate::proto::dht::{PeerValueKeyName, ValueRef};
use crate::types::PeerInfo;
impl DhtInner {
    pub(crate) fn start_background_tasks(self: &Arc<Self>, network: WeakNetwork) {
        enum Action {
            RefreshLocalPeerInfo,
            AnnounceLocalPeerInfo,
            RefreshRoutingTable,
            AddPeer(Arc<PeerInfo>),
        }
        let mut refresh_peer_info_interval = tokio::time::interval(
            self.config.local_info_refresh_period,
        );
        let mut announce_peer_info_interval = shifted_interval(
            self.config.local_info_announce_period,
            self.config.local_info_announce_period_max_jitter,
        );
        let mut refresh_routing_table_interval = shifted_interval(
            self.config.routing_table_refresh_period,
            self.config.routing_table_refresh_period_max_jitter,
        );
        let mut announced_peers = self.announced_peers.subscribe();
        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                39u32,
            );
            tracing::debug!("background DHT loop started");
            let mut prev_refresh_routing_table_fut = None::<JoinHandle<()>>;
            loop {
                __guard.checkpoint(43u32);
                let action = {
                    __guard.end_section(44u32);
                    let __result = tokio::select! {
                        _ = refresh_peer_info_interval.tick() =>
                        Action::RefreshLocalPeerInfo, _ = announce_peer_info_interval
                        .tick() => Action::AnnounceLocalPeerInfo, _ =
                        refresh_routing_table_interval.tick() =>
                        Action::RefreshRoutingTable, peer = announced_peers.recv() =>
                        match peer { Ok(peer) => Action::AddPeer(peer),
                        Err(broadcast::error::RecvError::Closed) => return,
                        Err(broadcast::error::RecvError::Lagged(lag)) => {
                        tracing::warn!(lag, "announced peers channel lagged"); continue
                        }, }
                    };
                    __guard.start_section(44u32);
                    __result
                };
                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade())
                else {
                    {
                        __guard.end_section(59u32);
                        __guard.start_section(59u32);
                        break;
                    };
                };
                match action {
                    Action::RefreshLocalPeerInfo => {
                        this.refresh_local_peer_info(&network);
                    }
                    Action::AnnounceLocalPeerInfo => {
                        refresh_peer_info_interval.reset();
                        if let Err(e) = {
                            __guard.end_section(70u32);
                            let __result = this.announce_local_peer_info(&network).await;
                            __guard.start_section(70u32);
                            __result
                        } {
                            tracing::error!(
                                "failed to announce local DHT node info: {e}"
                            );
                        }
                    }
                    Action::RefreshRoutingTable => {
                        if let Some(fut) = prev_refresh_routing_table_fut.take()
                            && let Err(e) = {
                                __guard.end_section(76u32);
                                let __result = fut.await;
                                __guard.start_section(76u32);
                                __result
                            } && e.is_panic()
                        {
                            std::panic::resume_unwind(e.into_panic());
                        }
                        prev_refresh_routing_table_fut = Some(
                            tokio::spawn(async move {
                                let mut __guard = crate::__async_profile_guard__::Guard::new(
                                    concat!(module_path!(), "::async_block"),
                                    file!(),
                                    82u32,
                                );
                                {
                                    __guard.end_section(83u32);
                                    let __result = this.refresh_routing_table(&network).await;
                                    __guard.start_section(83u32);
                                    __result
                                };
                            }),
                        );
                    }
                    Action::AddPeer(peer_info) => {
                        let peer_id = peer_info.id;
                        let mut signature_checked = false;
                        if peer_info.verify_ext(now_sec(), &mut signature_checked) {
                            let added = this.add_peer_info(&network, peer_info);
                            tracing::debug!(
                                local_id = % this.local_id, % peer_id, added,
                                "received peer info",
                            );
                        } else {
                            tracing::warn!(% peer_id, "received invalid peer info");
                        }
                        {
                            __guard.end_section(102u32);
                            let __result = tycho_util::sync::yield_on_complex(
                                    signature_checked,
                                )
                                .await;
                            __guard.start_section(102u32);
                            __result
                        };
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
    #[tracing::instrument(level = "debug", skip_all, fields(local_id = %self.local_id))]
    async fn announce_local_peer_info(&self, network: &Network) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(announce_local_peer_info)),
            file!(),
            116u32,
        );
        let network = network;
        let now = now_sec();
        let data = {
            let peer_info = self.make_local_peer_info(network, now);
            let data = tl_proto::serialize(&peer_info);
            *self.local_peer_info.lock().unwrap() = Some(peer_info);
            data
        };
        let mut value = self
            .make_unsigned_peer_value(
                PeerValueKeyName::NodeInfo,
                &data,
                now + self.config.max_peer_info_ttl.as_secs() as u32,
            );
        let signature = network.sign_tl(&value);
        value.signature = &signature;
        {
            __guard.end_section(134u32);
            let __result = self.store_value(network, &ValueRef::Peer(value), true).await;
            __guard.start_section(134u32);
            __result
        }
    }
    #[tracing::instrument(level = "debug", skip_all, fields(local_id = %self.local_id))]
    async fn refresh_routing_table(&self, network: &Network) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(refresh_routing_table)),
            file!(),
            138u32,
        );
        let network = network;
        const PARALLEL_QUERIES: usize = 3;
        const MAX_BUCKETS: usize = 15;
        const QUERY_DEPTH: usize = 3;
        let semaphore = Semaphore::new(PARALLEL_QUERIES);
        let mut futures = FuturesUnordered::new();
        {
            let rng = &mut rand::rng();
            let mut routing_table = self.routing_table.lock().unwrap();
            let now = now_sec();
            for (_, bucket) in routing_table.buckets.iter_mut() {
                __guard.checkpoint(153u32);
                bucket
                    .retain_nodes(|node| {
                        !node.is_expired(now, &self.config.max_peer_info_ttl)
                    });
            }
            for (&distance, _) in routing_table
                .buckets
                .iter()
                .filter(|(distance, bucket)| **distance > 0 && !bucket.is_empty())
                .take(MAX_BUCKETS)
            {
                __guard.checkpoint(158u32);
                let random_id = random_key_at_distance(
                    &routing_table.local_id,
                    distance,
                    rng,
                );
                let query = Query::new(
                    network.clone(),
                    &routing_table,
                    random_id.as_bytes(),
                    self.config.max_k,
                    DhtQueryMode::Closest,
                );
                futures
                    .push(async {
                        let mut __guard = crate::__async_profile_guard__::Guard::new(
                            concat!(module_path!(), "::async_block"),
                            file!(),
                            174u32,
                        );
                        let _permit = {
                            __guard.end_section(175u32);
                            let __result = semaphore.acquire().await;
                            __guard.start_section(175u32);
                            __result
                        }
                            .unwrap();
                        {
                            __guard.end_section(176u32);
                            let __result = query.find_peers(Some(QUERY_DEPTH)).await;
                            __guard.start_section(176u32);
                            __result
                        }
                    });
            }
        }
        let Some(mut peers) = ({
            __guard.end_section(182u32);
            let __result = futures.next().await;
            __guard.start_section(182u32);
            __result
        }) else {
            tracing::debug!("no new peers found");
            {
                __guard.end_section(184u32);
                return;
            };
        };
        while let Some(new_peers) = {
            __guard.end_section(188u32);
            let __result = futures.next().await;
            __guard.start_section(188u32);
            __result
        } {
            __guard.checkpoint(188u32);
            for (peer_id, peer) in new_peers {
                __guard.checkpoint(189u32);
                match peers.entry(peer_id) {
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(peer);
                    }
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
            __guard.checkpoint(207u32);
            if peer.id == self.local_id {
                {
                    __guard.end_section(209u32);
                    __guard.start_section(209u32);
                    continue;
                };
            }
            let is_new = routing_table
                .add(
                    peer.clone(),
                    self.config.max_k,
                    &self.config.max_peer_info_ttl,
                    |peer_info| network.known_peers().insert(peer_info, false).ok(),
                );
            count += is_new as usize;
        }
        tracing::debug!(count, "found new peers");
    }
}
