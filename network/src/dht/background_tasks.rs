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
    pub(crate) fn start_background_tasks(
        self: &Arc<Self>,
        network: WeakNetwork,
        bootstrap_peers: Vec<Arc<PeerInfo>>,
    ) {
        enum Action {
            RefreshLocalPeerInfo,
            AnnounceLocalPeerInfo,
            RefreshRoutingTable,
            RefillBootstrapPeers,
            AddPeer(Arc<PeerInfo>),
        }

        let mut refresh_peer_info_interval =
            tokio::time::interval(self.config.local_info_refresh_period);
        let mut announce_peer_info_interval = shifted_interval(
            self.config.local_info_announce_period,
            self.config.local_info_announce_period_max_jitter,
        );
        let mut refresh_routing_table_interval = shifted_interval(
            self.config.routing_table_refresh_period,
            self.config.routing_table_refresh_period_max_jitter,
        );
        let mut refill_bootstrap_peers_interval = self
            .config
            .bootstrap_peers_refill_period
            .map(tokio::time::interval);

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
                        Err(broadcast::error::RecvError::Closed) => return,
                        Err(broadcast::error::RecvError::Lagged(lag)) => {
                            tracing::warn!(lag, "announced peers channel lagged");
                            continue
                        },
                    },
                    _ = async {
                        match refill_bootstrap_peers_interval {
                            Some(ref mut interval) => interval.tick().await,
                            None => std::future::pending().await,
                        }
                    }, if refill_bootstrap_peers_interval.is_some() => Action::RefillBootstrapPeers
                };

                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade()) else {
                    break;
                };

                match action {
                    Action::RefreshLocalPeerInfo => {
                        this.refresh_local_peer_info(&network);
                    }
                    Action::AnnounceLocalPeerInfo => {
                        // Peer info is always refreshed before announcing
                        refresh_peer_info_interval.reset();

                        if let Err(e) = this.announce_local_peer_info(&network).await {
                            tracing::error!("failed to announce local DHT node info: {e}");
                        }
                    }
                    Action::RefreshRoutingTable => {
                        if let Some(fut) = prev_refresh_routing_table_fut.take()
                            && let Err(e) = fut.await
                            && e.is_panic()
                        {
                            std::panic::resume_unwind(e.into_panic());
                        }

                        prev_refresh_routing_table_fut = Some(tokio::spawn(async move {
                            this.refresh_routing_table(&network).await;
                        }));
                    }
                    Action::RefillBootstrapPeers => {
                        this.refill_bootstrap_peers(&network, &bootstrap_peers)
                            .await;
                    }
                    Action::AddPeer(peer_info) => {
                        let peer_id = peer_info.id;
                        let mut signature_checked = false;
                        if peer_info.verify_ext(now_sec(), &mut signature_checked) {
                            let added = this.add_peer_info(&network, peer_info);
                            tracing::debug!(
                                local_id = %this.local_id,
                                %peer_id,
                                added,
                                "received peer info",
                            );
                        } else {
                            // TODO: Is it ok to WARN here since it can be spammed?
                            tracing::warn!(%peer_id, "received invalid peer info");
                        }

                        tycho_util::sync::yield_on_complex(signature_checked).await;
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
        let now = now_sec();
        let data = {
            let peer_info = self.make_local_peer_info(network, now);
            let data = tl_proto::serialize(&peer_info);
            *self.local_peer_info.lock().unwrap() = Some(peer_info);
            data
        };

        let mut value = self.make_unsigned_peer_value(
            PeerValueKeyName::NodeInfo,
            &data,
            now + self.config.max_peer_info_ttl.as_secs() as u32,
        );
        let signature = network.sign_tl(&value);
        value.signature = &signature;

        self.store_value(network, &ValueRef::Peer(value), true)
            .await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(local_id = %self.local_id))]
    async fn refresh_routing_table(&self, network: &Network) {
        const PARALLEL_QUERIES: usize = 3;
        const MAX_BUCKETS: usize = 15;
        const QUERY_DEPTH: usize = 3;

        // Prepare futures for each bucket
        let semaphore = Semaphore::new(PARALLEL_QUERIES);
        let mut futures = FuturesUnordered::new();
        {
            let rng = &mut rand::rng();

            let mut routing_table = self.routing_table.lock().unwrap();

            // Filter out expired nodes
            let now = now_sec();
            for (_, bucket) in routing_table.buckets.iter_mut() {
                bucket.retain_nodes(|node| !node.is_expired(now, &self.config.max_peer_info_ttl));
            }

            // Iterate over the first non-empty buckets (at most `MAX_BUCKETS`)
            for (&distance, _) in routing_table
                .buckets
                .iter()
                .filter(|(distance, bucket)| **distance > 0 && !bucket.is_empty())
                .take(MAX_BUCKETS)
            {
                // Query the K closest nodes for a random ID at the specified distance from the local ID.
                let random_id = random_key_at_distance(&routing_table.local_id, distance, rng);
                let query = Query::new(
                    network.clone(),
                    &routing_table,
                    random_id.as_bytes(),
                    self.config.max_k,
                    DhtQueryMode::Closest,
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
                |peer_info| network.known_peers().insert(peer_info, false).ok(),
            );
            count += is_new as usize;
        }

        tracing::debug!(count, "found new peers");
    }

    #[tracing::instrument(level = "debug", skip_all, fields(local_id = %self.local_id))]
    async fn refill_bootstrap_peers(&self, network: &Network, bootstrap_peers: &[Arc<PeerInfo>]) {
        let mut count = 0usize;

        for peer in bootstrap_peers {
            let is_new = self.add_allow_outdated_peer_info(network, peer.clone());
            count += is_new as usize;

            tokio::task::yield_now().await;
        }

        tracing::debug!(count, "refilled bootstrap peers");
    }
}
