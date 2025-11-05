use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use anyhow::Result;
use futures_util::StreamExt;
use indexmap::IndexSet;
use rand::Rng;
use tycho_util::futures::JoinTask;
use tycho_util::time::{now_sec, shifted_interval, shifted_interval_immediate};
use crate::dht::{DhtClient, DhtQueryMode, DhtService};
use crate::network::{KnownPeerHandle, Network, WeakNetwork};
use crate::overlay::tasks_stream::TasksStream;
use crate::overlay::{OverlayId, OverlayServiceInner, PublicEntry, PublicOverlayEntries};
use crate::proto::dht::{MergedValueKeyName, MergedValueKeyRef, Value};
use crate::proto::overlay::{
    PublicEntriesResponse, PublicEntryResponse, PublicEntryToSign, rpc,
};
use crate::types::Request;
use crate::util::NetworkExt;
impl OverlayServiceInner {
    pub(crate) fn start_background_tasks(
        self: &Arc<Self>,
        network: WeakNetwork,
        dht_service: Option<DhtService>,
    ) {
        enum Action<'a> {
            UpdatePublicOverlaysList(&'a mut PublicOverlaysState),
            ExchangePublicOverlayEntries {
                overlay_id: OverlayId,
                tasks: &'a mut TasksStream,
            },
            DiscoverPublicOverlayEntries {
                overlay_id: OverlayId,
                tasks: &'a mut TasksStream,
                force: bool,
            },
            CollectPublicEntries { overlay_id: OverlayId, tasks: &'a mut TasksStream },
            StorePublicEntries { overlay_id: OverlayId, tasks: &'a mut TasksStream },
        }
        struct PublicOverlaysState {
            exchange: TasksStream,
            discover: TasksStream,
            collect: TasksStream,
            store: TasksStream,
        }
        let public_overlays_notify = self.public_overlays_changed.clone();
        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                58u32,
            );
            tracing::debug!("background overlay loop started");
            let mut public_overlays_changed = Box::pin(
                public_overlays_notify.notified(),
            );
            let mut public_overlays_state = None::<PublicOverlaysState>;
            let dht_peer_added = dht_service
                .as_ref()
                .map(|s| s.peer_added())
                .cloned()
                .unwrap_or_default();
            let empty_overlays = OverlayIdsQueue::default();
            loop {
                __guard.checkpoint(72u32);
                let action = match &mut public_overlays_state {
                    None => {
                        Action::UpdatePublicOverlaysList(
                            public_overlays_state
                                .insert(PublicOverlaysState {
                                    exchange: TasksStream::new("exchange public overlay peers"),
                                    discover: TasksStream::new(
                                        "discover public overlay entries in DHT",
                                    ),
                                    collect: TasksStream::new("collect public overlay entries"),
                                    store: TasksStream::new(
                                        "store public overlay entries in DHT",
                                    ),
                                }),
                        )
                    }
                    Some(public_overlays_state) => {
                        __guard.end_section(85u32);
                        let __result = tokio::select! {
                            _ = & mut public_overlays_changed => {
                            public_overlays_changed = Box::pin(public_overlays_notify
                            .notified());
                            Action::UpdatePublicOverlaysList(public_overlays_state) },
                            overlay_id = public_overlays_state.exchange.next() => match
                            overlay_id { Some(id) => Action::ExchangePublicOverlayEntries
                            { overlay_id : id, tasks : & mut public_overlays_state
                            .exchange, }, None => continue, }, overlay_id =
                            public_overlays_state.discover.next() => match overlay_id {
                            Some(id) => Action::DiscoverPublicOverlayEntries { overlay_id
                            : id, tasks : & mut public_overlays_state.discover, force :
                            false, }, None => continue, }, overlay_id =
                            public_overlays_state.collect.next() => match overlay_id {
                            Some(id) => Action::CollectPublicEntries { overlay_id : id,
                            tasks : & mut public_overlays_state.collect, }, None =>
                            continue, }, overlay_id = public_overlays_state.store.next()
                            => match overlay_id { Some(id) => Action::StorePublicEntries
                            { overlay_id : id, tasks : & mut public_overlays_state.store,
                            }, None => continue, }, _ = dht_peer_added.notified(), if !
                            empty_overlays.is_empty() => { let Some(id) = empty_overlays
                            .pop() else { continue; }; tracing::debug!(overlay_id = % id,
                            "force discover public overlay peers on new DHT peer",);
                            Action::DiscoverPublicOverlayEntries { overlay_id : id, tasks
                            : & mut public_overlays_state.discover, force : true, } },
                        };
                        __guard.start_section(85u32);
                        __result
                    }
                };
                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade())
                else {
                    {
                        __guard.end_section(138u32);
                        __guard.start_section(138u32);
                        break;
                    };
                };
                match action {
                    Action::UpdatePublicOverlaysList(
                        PublicOverlaysState { exchange, discover, store, collect },
                    ) => {
                        let iter = this.public_overlays.iter().map(|item| *item.key());
                        exchange
                            .rebuild(
                                iter.clone(),
                                |_| {
                                    shifted_interval(
                                        this.config.public_overlay_peer_exchange_period,
                                        this.config.public_overlay_peer_exchange_max_jitter,
                                    )
                                },
                            );
                        discover
                            .rebuild(
                                iter.clone(),
                                |_| {
                                    shifted_interval_immediate(
                                        this.config.public_overlay_peer_discovery_period,
                                        this.config.public_overlay_peer_discovery_max_jitter,
                                    )
                                },
                            );
                        collect
                            .rebuild(
                                iter.clone(),
                                |_| {
                                    shifted_interval(
                                        this.config.public_overlay_peer_collect_period,
                                        this.config.public_overlay_peer_collect_max_jitter,
                                    )
                                },
                            );
                        store
                            .rebuild_ext(
                                iter,
                                |overlay_id| {
                                    if let Some(dht) = &dht_service {
                                        dht.insert_merger(
                                            overlay_id.as_bytes(),
                                            this.public_entries_merger.clone(),
                                        );
                                    }
                                    shifted_interval_immediate(
                                        this.config.public_overlay_peer_store_period,
                                        this.config.public_overlay_peer_store_max_jitter,
                                    )
                                },
                                |overlay_id| {
                                    if let Some(dht) = &dht_service {
                                        dht.remove_merger(overlay_id.as_bytes());
                                    }
                                },
                            );
                    }
                    Action::ExchangePublicOverlayEntries { overlay_id, tasks } => {
                        tasks
                            .spawn(
                                &overlay_id,
                                move || async move {
                                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                                        concat!(module_path!(), "::async_block"),
                                        file!(),
                                        194u32,
                                    );
                                    {
                                        __guard.end_section(195u32);
                                        let __result = this
                                            .exchange_public_entries(&network, &overlay_id)
                                            .await;
                                        __guard.start_section(195u32);
                                        __result
                                    }
                                },
                            );
                    }
                    Action::DiscoverPublicOverlayEntries {
                        overlay_id,
                        tasks,
                        force,
                    } => {
                        let Some(dht_service) = dht_service.clone() else {
                            {
                                __guard.end_section(204u32);
                                __guard.start_section(204u32);
                                continue;
                            };
                        };
                        let empty_overlays = empty_overlays.clone();
                        tasks
                            .spawn(
                                &overlay_id,
                                move || async move {
                                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                                        concat!(module_path!(), "::async_block"),
                                        file!(),
                                        208u32,
                                    );
                                    let status = {
                                        __guard.end_section(214u32);
                                        let __result = this
                                            .discover_public_entries(
                                                &dht_service.make_client(&network),
                                                &overlay_id,
                                            )
                                            .await;
                                        __guard.start_section(214u32);
                                        __result
                                    }?;
                                    let mut force_exchange = false;
                                    match status {
                                        DiscoveryStatus::Unchanged { is_empty } if is_empty => {
                                            if empty_overlays.insert(&overlay_id) {
                                                tracing::debug!(
                                                    % overlay_id,
                                                    "enqueued force public overlay peers discovery \
                                            on new DHT peer",
                                                );
                                            }
                                        }
                                        DiscoveryStatus::Changed => {
                                            {
                                                __guard.end_section(234u32);
                                                let __result = this
                                                    .store_public_entries(
                                                        &dht_service.make_client(&network),
                                                        &overlay_id,
                                                    )
                                                    .await;
                                                __guard.start_section(234u32);
                                                __result
                                            }?;
                                            force_exchange = force;
                                        }
                                        _ => {}
                                    }
                                    if force_exchange {
                                        {
                                            __guard.end_section(241u32);
                                            let __result = this
                                                .exchange_public_entries(&network, &overlay_id)
                                                .await;
                                            __guard.start_section(241u32);
                                            __result
                                        }?;
                                    }
                                    Ok(())
                                },
                            );
                    }
                    Action::CollectPublicEntries { overlay_id, tasks } => {
                        tasks
                            .spawn(
                                &overlay_id,
                                move || async move {
                                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                                        concat!(module_path!(), "::async_block"),
                                        file!(),
                                        247u32,
                                    );
                                    {
                                        __guard.end_section(248u32);
                                        let __result = this
                                            .collect_public_entries(&network, &overlay_id)
                                            .await;
                                        __guard.start_section(248u32);
                                        __result
                                    };
                                    Ok(())
                                },
                            );
                    }
                    Action::StorePublicEntries { overlay_id, tasks } => {
                        let Some(dht_service) = dht_service.clone() else {
                            {
                                __guard.end_section(254u32);
                                __guard.start_section(254u32);
                                continue;
                            };
                        };
                        tasks
                            .spawn(
                                &overlay_id,
                                move || async move {
                                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                                        concat!(module_path!(), "::async_block"),
                                        file!(),
                                        257u32,
                                    );
                                    {
                                        __guard.end_section(262u32);
                                        let __result = this
                                            .store_public_entries(
                                                &dht_service.make_client(&network),
                                                &overlay_id,
                                            )
                                            .await;
                                        __guard.start_section(262u32);
                                        __result
                                    }
                                },
                            );
                    }
                }
            }
            tracing::debug!("background overlay loop stopped");
        });
    }
    #[tracing::instrument(
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn exchange_public_entries(
        &self,
        network: &Network,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(exchange_public_entries)),
            file!(),
            280u32,
        );
        let network = network;
        let overlay_id = overlay_id;
        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::warn!("overlay not found");
            {
                __guard.end_section(285u32);
                return Ok(());
            };
        };
        overlay.remove_invalid_entries(now_sec());
        let n = std::cmp::max(self.config.exchange_public_entries_batch, 1);
        let mut entries = Vec::with_capacity(n);
        let own_signed_entry = Arc::new(
            self.make_local_public_overlay_entry(network, overlay_id, now_sec()),
        );
        overlay.set_own_signed_entry(own_signed_entry.clone());
        entries.push(own_signed_entry);
        let target_peer_handle;
        let target_peer_id;
        {
            let rng = &mut rand::rng();
            let all_entries = overlay.read_entries();
            match choose_random_resolved_peer(&all_entries, rng) {
                Some(handle) => {
                    target_peer_handle = handle;
                    target_peer_id = target_peer_handle.load_peer_info().id;
                }
                None => {
                    tracing::warn!(
                        "no resolved peers in the overlay to exchange entries with"
                    );
                    {
                        __guard.end_section(314u32);
                        return Ok(());
                    };
                }
            }
            entries
                .extend(
                    all_entries
                        .choose_multiple(rng, n)
                        .filter(|&item| item.entry.peer_id != target_peer_id)
                        .map(|item| item.entry.clone())
                        .take(n - 1),
                );
        };
        let response = {
            __guard.end_section(338u32);
            let __result = network
                .query(
                    &target_peer_id,
                    Request::from_tl(rpc::ExchangeRandomPublicEntries {
                        overlay_id: overlay_id.to_bytes(),
                        entries,
                    }),
                )
                .await;
            __guard.start_section(338u32);
            __result
        }?
            .parse_tl::<PublicEntriesResponse>()?;
        drop(target_peer_handle);
        match response {
            PublicEntriesResponse::PublicEntries(entries) => {
                tracing::debug!(
                    peer_id = % target_peer_id, count = entries.len(),
                    "received public entries"
                );
                overlay.add_untrusted_entries(&self.local_id, &entries, now_sec());
            }
            PublicEntriesResponse::OverlayNotFound => {
                tracing::debug!(
                    peer_id = % target_peer_id, "peer does not have the overlay",
                );
            }
        }
        Ok(())
    }
    #[tracing::instrument(
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn discover_public_entries(
        &self,
        dht_client: &DhtClient,
        overlay_id: &OverlayId,
    ) -> Result<DiscoveryStatus> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(discover_public_entries)),
            file!(),
            374u32,
        );
        let dht_client = dht_client;
        let overlay_id = overlay_id;
        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::warn!(% overlay_id, "overlay not found");
            {
                __guard.end_section(379u32);
                return Ok(DiscoveryStatus::OverlayNotFound);
            };
        };
        let key_hash = tl_proto::hash(MergedValueKeyRef {
            name: MergedValueKeyName::PublicOverlayEntries,
            group_id: overlay_id.as_bytes(),
        });
        let res = {
            __guard.end_section(387u32);
            let __result = dht_client.find_value(&key_hash, DhtQueryMode::Random).await;
            __guard.start_section(387u32);
            __result
        };
        let is_empty = overlay.read_entries().is_empty();
        let entries = match res {
            Some(value) => {
                match &*value {
                    Value::Merged(value) => {
                        tl_proto::deserialize::<Vec<Arc<PublicEntry>>>(&value.data)?
                    }
                    Value::Peer(_) => {
                        tracing::warn!(
                            "expected a `Value::Merged`, but got a `Value::Peer`"
                        );
                        {
                            __guard.end_section(397u32);
                            return Ok(DiscoveryStatus::Unchanged {
                                is_empty,
                            });
                        };
                    }
                }
            }
            None => {
                tracing::debug!("no public entries found in the DHT");
                {
                    __guard.end_section(402u32);
                    return Ok(DiscoveryStatus::Unchanged {
                        is_empty,
                    });
                };
            }
        };
        let updated = overlay.add_untrusted_entries(&self.local_id, &entries, now_sec());
        tracing::debug!(count = entries.len(), updated, "discovered public entries");
        Ok(
            if updated {
                DiscoveryStatus::Changed
            } else {
                DiscoveryStatus::Unchanged {
                    is_empty,
                }
            },
        )
    }
    #[tracing::instrument(
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn collect_public_entries(&self, network: &Network, overlay_id: &OverlayId) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(collect_public_entries)),
            file!(),
            420u32,
        );
        let network = network;
        let overlay_id = overlay_id;
        use futures_util::future::FutureExt;
        const QUERY_TIMEOUT: Duration = Duration::from_millis(100);
        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::warn!("overlay not found");
            {
                __guard.end_section(429u32);
                return;
            };
        };
        let Some(peers) = overlay.unknown_peers_queue().pop_multiple() else {
            tracing::debug!("no peers to collect");
            {
                __guard.end_section(434u32);
                return;
            };
        };
        tracing::info!(count = peers.len(), "found peers to collect");
        let mut futures = futures_util::stream::FuturesUnordered::new();
        {
            let req = Request::from_tl(rpc::GetPublicEntry {
                overlay_id: overlay_id.to_bytes(),
            });
            let all_entries = overlay.read_entries();
            for peer_id in peers {
                __guard.checkpoint(448u32);
                if !network.known_peers().contains(&peer_id)
                    || all_entries.contains(&peer_id)
                {
                    {
                        __guard.end_section(451u32);
                        __guard.start_section(451u32);
                        continue;
                    };
                }
                let network = network.clone();
                let req = req.clone();
                futures
                    .push(
                        JoinTask::new(
                            async move {
                                let mut __guard = crate::__async_profile_guard__::Guard::new(
                                    concat!(module_path!(), "::async_block"),
                                    file!(),
                                    457u32,
                                );
                                match {
                                    __guard.end_section(459u32);
                                    let __result = tokio::time::timeout(
                                            QUERY_TIMEOUT,
                                            network.query(&peer_id, req),
                                        )
                                        .await;
                                    __guard.start_section(459u32);
                                    __result
                                } {
                                    Ok(entry) => {
                                        match entry?.parse_tl()? {
                                            PublicEntryResponse::Found(entry) => {
                                                anyhow::ensure!(
                                                    entry.peer_id == peer_id, "public entry peer id mismatch"
                                                );
                                                Ok(entry)
                                            }
                                            PublicEntryResponse::OverlayNotFound => {
                                                anyhow::bail!(
                                                    "target peer doesn't known about this overlay"
                                                );
                                            }
                                        }
                                    }
                                    Err(_) => anyhow::bail!("public entry query timeout"),
                                }
                            }
                                .map(move |res| (peer_id, res)),
                        ),
                    );
            }
        }
        while let Some((peer_id, res)) = {
            __guard.end_section(483u32);
            let __result = futures.next().await;
            __guard.start_section(483u32);
            __result
        } {
            __guard.checkpoint(483u32);
            match res {
                Ok(entry) => {
                    tracing::debug!(% peer_id, "received public entry");
                    let any_added = overlay
                        .add_untrusted_entries(
                            &self.local_id,
                            std::slice::from_ref(&entry),
                            now_sec(),
                        );
                    if any_added {
                        {
                            __guard.end_section(496u32);
                            let __result = tokio::task::yield_now().await;
                            __guard.start_section(496u32);
                            __result
                        };
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        % peer_id, "failed to get peer public overlay entry: {e}"
                    )
                }
            }
        }
    }
    #[tracing::instrument(
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn store_public_entries(
        &self,
        dht_client: &DhtClient,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(store_public_entries)),
            file!(),
            512u32,
        );
        let dht_client = dht_client;
        let overlay_id = overlay_id;
        use crate::proto::dht;
        const DEFAULT_TTL: u32 = 3600;
        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::warn!(% overlay_id, "overlay not found");
            {
                __guard.end_section(521u32);
                return Ok(());
            };
        };
        let now = now_sec();
        let mut n = std::cmp::max(self.config.public_overlay_peer_store_max_entries, 1);
        let data = {
            let rng = &mut rand::rng();
            let mut entries = Vec::<Arc<PublicEntry>>::with_capacity(n);
            let own_signed_entry = Arc::new(
                self
                    .make_local_public_overlay_entry(
                        dht_client.network(),
                        overlay_id,
                        now,
                    ),
            );
            overlay.set_own_signed_entry(own_signed_entry.clone());
            entries.push(own_signed_entry);
            entries
                .extend(
                    overlay
                        .read_entries()
                        .choose_multiple(rng, n - 1)
                        .map(|item| item.entry.clone()),
                );
            n = entries.len();
            tl_proto::serialize(&entries)
        };
        let value = dht::ValueRef::Merged(dht::MergedValueRef {
            key: dht::MergedValueKeyRef {
                name: dht::MergedValueKeyName::PublicOverlayEntries,
                group_id: overlay_id.as_bytes(),
            },
            data: &data,
            expires_at: now + DEFAULT_TTL,
        });
        dht_client.service().store_value_locally(&value)?;
        tracing::debug!(count = n, "stored public entries in the DHT");
        Ok(())
    }
    fn make_local_public_overlay_entry(
        &self,
        network: &Network,
        overlay_id: &OverlayId,
        now: u32,
    ) -> PublicEntry {
        let signature = Box::new(
            network
                .sign_tl(PublicEntryToSign {
                    overlay_id: overlay_id.as_bytes(),
                    peer_id: &self.local_id,
                    created_at: now,
                }),
        );
        PublicEntry {
            peer_id: self.local_id,
            created_at: now,
            signature,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiscoveryStatus {
    OverlayNotFound,
    Unchanged { is_empty: bool },
    Changed,
}
#[derive(Default, Clone)]
struct OverlayIdsQueue(Arc<OverlayIdsQueueInner>);
impl OverlayIdsQueue {
    fn is_empty(&self) -> bool {
        self.0.is_empty.load(Ordering::Acquire)
    }
    fn insert(&self, overlay_id: &OverlayId) -> bool {
        let added = self.0.queue.lock().unwrap().insert(*overlay_id);
        self.0.is_empty.store(false, Ordering::Release);
        added
    }
    fn pop(&self) -> Option<OverlayId> {
        let mut queue = self.0.queue.lock().unwrap();
        let overlay_id = queue.pop();
        self.0.is_empty.store(queue.is_empty(), Ordering::Release);
        overlay_id
    }
}
struct OverlayIdsQueueInner {
    queue: Mutex<IndexSet<OverlayId, ahash::RandomState>>,
    is_empty: AtomicBool,
}
impl Default for OverlayIdsQueueInner {
    fn default() -> Self {
        Self {
            queue: Mutex::new(IndexSet::default()),
            is_empty: AtomicBool::new(true),
        }
    }
}
fn choose_random_resolved_peer<R>(
    entries: &PublicOverlayEntries,
    rng: &mut R,
) -> Option<KnownPeerHandle>
where
    R: Rng + ?Sized,
{
    entries
        .choose_all(rng)
        .find(|item| item.resolver_handle.is_resolved())
        .map(|item| {
            item.resolver_handle.load_handle().expect("invalid resolved flag state")
        })
}
