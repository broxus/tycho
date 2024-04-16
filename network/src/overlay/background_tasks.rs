use std::sync::Arc;

use anyhow::Result;
use rand::Rng;
use tycho_util::time::{now_sec, shifted_interval};

use crate::dht::{DhtClient, DhtQueryMode, DhtService};
use crate::network::{KnownPeerHandle, Network, WeakNetwork};
use crate::overlay::tasks_stream::TasksStream;
use crate::overlay::{OverlayId, OverlayServiceInner, PublicEntry, PublicOverlayEntries};
use crate::proto::dht::{MergedValueKeyName, MergedValueKeyRef, Value};
use crate::proto::overlay::{rpc, PublicEntriesResponse, PublicEntryToSign};
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
            },
            StorePublicEntries {
                overlay_id: OverlayId,
                tasks: &'a mut TasksStream,
            },
        }

        struct PublicOverlaysState {
            exchange: TasksStream,
            discover: TasksStream,
            store: TasksStream,
        }

        let public_overlays_notify = self.public_overlays_changed.clone();

        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            tracing::debug!("background overlay loop started");

            let mut public_overlays_changed = Box::pin(public_overlays_notify.notified());
            let mut public_overlays_state = None::<PublicOverlaysState>;

            loop {
                let action = match &mut public_overlays_state {
                    // Initial update for public overlays list
                    None => Action::UpdatePublicOverlaysList(public_overlays_state.insert(
                        PublicOverlaysState {
                            exchange: TasksStream::new("exchange public overlay peers"),
                            discover: TasksStream::new("discover public overlay entries in DHT"),
                            store: TasksStream::new("store public overlay entries in DHT"),
                        },
                    )),
                    // Default actions
                    Some(public_overlays_state) => {
                        tokio::select! {
                            _ = &mut public_overlays_changed => {
                                public_overlays_changed = Box::pin(public_overlays_notify.notified());
                                Action::UpdatePublicOverlaysList(public_overlays_state)
                            },
                            overlay_id = public_overlays_state.exchange.next() => match overlay_id {
                                Some(id) => Action::ExchangePublicOverlayEntries {
                                    overlay_id: id,
                                    tasks: &mut public_overlays_state.exchange,
                                },
                                None => continue,
                            },
                            overlay_id = public_overlays_state.discover.next() => match overlay_id {
                                Some(id) => Action::DiscoverPublicOverlayEntries {
                                    overlay_id: id,
                                    tasks: &mut public_overlays_state.discover,
                                },
                                None => continue,
                            },
                            overlay_id = public_overlays_state.store.next() => match overlay_id {
                                Some(id) => Action::StorePublicEntries {
                                    overlay_id: id,
                                    tasks: &mut public_overlays_state.store,
                                },
                                None => continue,
                            },
                        }
                    }
                };

                let (Some(this), Some(network)) = (this.upgrade(), network.upgrade()) else {
                    break;
                };

                match action {
                    Action::UpdatePublicOverlaysList(PublicOverlaysState {
                        exchange,
                        discover,
                        store,
                    }) => {
                        let iter = this.public_overlays.iter().map(|item| *item.key());
                        exchange.rebuild(iter.clone(), |_| {
                            shifted_interval(
                                this.config.public_overlay_peer_exchange_period,
                                this.config.public_overlay_peer_exchange_max_jitter,
                            )
                        });
                        discover.rebuild(iter.clone(), |_| {
                            shifted_interval(
                                this.config.public_overlay_peer_discovery_period,
                                this.config.public_overlay_peer_discovery_max_jitter,
                            )
                        });
                        store.rebuild_ext(
                            iter,
                            |overlay_id| {
                                // Insert merger for new overlays
                                if let Some(dht) = &dht_service {
                                    dht.insert_merger(
                                        overlay_id.as_bytes(),
                                        this.public_entries_merger.clone(),
                                    );
                                }

                                shifted_interval(
                                    this.config.public_overlay_peer_store_period,
                                    this.config.public_overlay_peer_store_max_jitter,
                                )
                            },
                            |overlay_id| {
                                // Remove merger for removed overlays
                                if let Some(dht) = &dht_service {
                                    dht.remove_merger(overlay_id.as_bytes());
                                }
                            },
                        );
                    }
                    Action::ExchangePublicOverlayEntries { overlay_id, tasks } => {
                        tasks.spawn(&overlay_id, move || async move {
                            this.exchange_public_entries(&network, &overlay_id).await
                        });
                    }
                    Action::DiscoverPublicOverlayEntries { overlay_id, tasks } => {
                        let Some(dht_service) = dht_service.clone() else {
                            continue;
                        };

                        tasks.spawn(&overlay_id, move || async move {
                            this.discover_public_entries(
                                &dht_service.make_client(&network),
                                &overlay_id,
                            )
                            .await
                        });
                    }
                    Action::StorePublicEntries { overlay_id, tasks } => {
                        let Some(dht_service) = dht_service.clone() else {
                            continue;
                        };

                        tasks.spawn(&overlay_id, move || async move {
                            this.store_public_entries(
                                &dht_service.make_client(&network),
                                &overlay_id,
                            )
                            .await
                        });
                    }
                }
            }

            tracing::debug!("background overlay loop stopped");
        });
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn exchange_public_entries(
        &self,
        network: &Network,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::warn!("overlay not found");
            return Ok(());
        };

        overlay.remove_invalid_entries(now_sec());

        let n = std::cmp::max(self.config.exchange_public_entries_batch, 1);
        let mut entries = Vec::with_capacity(n);

        // Always include us in the response
        entries.push(Arc::new(self.make_local_public_overlay_entry(
            network,
            overlay_id,
            now_sec(),
        )));

        // Choose a random target to send the request and additional random entries
        let target_peer_handle;
        let target_peer_id;
        {
            let rng = &mut rand::thread_rng();

            let all_entries = overlay.read_entries();

            match choose_random_resolved_peer(&all_entries, rng) {
                Some(handle) => {
                    target_peer_handle = handle;
                    target_peer_id = target_peer_handle.load_peer_info().id;
                }
                None => {
                    tracing::warn!("no resolved peers in the overlay to exchange entries with");
                    return Ok(());
                }
            }

            // Add additional random entries to the response.
            // NOTE: `n` instead of `n - 1` because we might ignore the target peer
            entries.extend(
                all_entries
                    .choose_multiple(rng, n)
                    .filter(|&item| (item.entry.peer_id != target_peer_id))
                    .map(|item| item.entry.clone())
                    .take(n - 1),
            );
        };

        // Send request
        let response = network
            .query(
                &target_peer_id,
                Request::from_tl(rpc::ExchangeRandomPublicEntries {
                    overlay_id: overlay_id.to_bytes(),
                    entries,
                }),
            )
            .await?
            .parse_tl::<PublicEntriesResponse>()?;

        // NOTE: Ensure that resolved peer handle is alive for enough time
        drop(target_peer_handle);

        // Populate the overlay with the response
        match response {
            PublicEntriesResponse::PublicEntries(entries) => {
                tracing::debug!(
                    peer_id = %target_peer_id,
                    count = entries.len(),
                    "received public entries"
                );
                overlay.add_untrusted_entries(&entries, now_sec());
            }
            PublicEntriesResponse::OverlayNotFound => {
                tracing::debug!(
                    peer_id = %target_peer_id,
                    "peer does not have the overlay",
                );
            }
        }

        // Done
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn discover_public_entries(
        &self,
        dht_client: &DhtClient,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::warn!(%overlay_id, "overlay not found");
            return Ok(());
        };

        let key_hash = tl_proto::hash(MergedValueKeyRef {
            name: MergedValueKeyName::PublicOverlayEntries,
            group_id: overlay_id.as_bytes(),
        });

        let entries = match dht_client.find_value(&key_hash, DhtQueryMode::Random).await {
            Some(value) => match &*value {
                Value::Merged(value) => {
                    tl_proto::deserialize::<Vec<Arc<PublicEntry>>>(&value.data)?
                }
                Value::Peer(_) => {
                    tracing::warn!("expected a `Value::Merged`, but got a `Value::Peer`");
                    return Ok(());
                }
            },
            None => {
                tracing::debug!("no public entries found in the DHT");
                return Ok(());
            }
        };

        overlay.add_untrusted_entries(&entries, now_sec());

        tracing::debug!(count = entries.len(), "discovered public entries");
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(local_id = %self.local_id, overlay_id = %overlay_id),
    )]
    async fn store_public_entries(
        &self,
        dht_client: &DhtClient,
        overlay_id: &OverlayId,
    ) -> Result<()> {
        use crate::proto::dht;

        const DEFAULT_TTL: u32 = 3600; // 1 hour

        let overlay = if let Some(overlay) = self.public_overlays.get(overlay_id) {
            overlay.value().clone()
        } else {
            tracing::warn!(%overlay_id, "overlay not found");
            return Ok(());
        };

        let now = now_sec();
        let mut n = std::cmp::max(self.config.public_overlay_peer_store_max_entries, 1);

        let data = {
            let rng = &mut rand::thread_rng();

            let mut entries = Vec::<Arc<PublicEntry>>::with_capacity(n);

            // Always include us in the list
            entries.push(Arc::new(self.make_local_public_overlay_entry(
                dht_client.network(),
                overlay_id,
                now,
            )));

            // Fill with random entries
            entries.extend(
                overlay
                    .read_entries()
                    .choose_multiple(rng, n - 1)
                    .map(|item| item.entry.clone()),
            );

            n = entries.len();

            // Serialize entries
            tl_proto::serialize(&entries)
        };

        // Store entries in the DHT
        let value = dht::ValueRef::Merged(dht::MergedValueRef {
            key: dht::MergedValueKeyRef {
                name: dht::MergedValueKeyName::PublicOverlayEntries,
                group_id: overlay_id.as_bytes(),
            },
            data: &data,
            expires_at: now + DEFAULT_TTL,
        });

        // TODO: Store the value on other nodes as well?
        dht_client.service().store_value_locally(&value)?;

        tracing::debug!(count = n, "stored public entries in the DHT",);
        Ok(())
    }

    fn make_local_public_overlay_entry(
        &self,
        network: &Network,
        overlay_id: &OverlayId,
        now: u32,
    ) -> PublicEntry {
        let signature = Box::new(network.sign_tl(PublicEntryToSign {
            overlay_id: overlay_id.as_bytes(),
            peer_id: &self.local_id,
            created_at: now,
        }));
        PublicEntry {
            peer_id: self.local_id,
            created_at: now,
            signature,
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
            item.resolver_handle
                .load_handle()
                .expect("invalid resolved flag state")
        })
}
