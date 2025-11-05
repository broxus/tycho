#[doc(hidden)]
#[allow(dead_code)]
mod __async_profile_guard__ {
    use std::time::{Duration, Instant};
    const THRESHOLD_MS: u64 = 10u64;
    pub struct Guard {
        name: &'static str,
        file: &'static str,
        from_line: u32,
        current_start: Option<Instant>,
        consecutive_hits: u32,
    }
    impl Guard {
        pub fn new(name: &'static str, file: &'static str, line: u32) -> Self {
            Guard {
                name,
                file,
                from_line: line,
                current_start: Some(Instant::now()),
                consecutive_hits: 0,
            }
        }
        pub fn checkpoint(&mut self, new_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{from}-{to}", file = self.file, from = self.from_line, to
                        = new_line
                    );
                    let wraparound = new_line < self.from_line;
                    if wraparound {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (iteration tail wraparound)"
                        );
                    } else {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (iteration tail)"
                        );
                    }
                } else {
                    self.consecutive_hits = 0;
                }
            }
            self.from_line = new_line;
            self.current_start = Some(Instant::now());
        }
        pub fn end_section(&mut self, to_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{from}-{to}", file = self.file, from = self.from_line, to
                        = to_line
                    );
                    let wraparound = to_line < self.from_line;
                    if wraparound {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll (loop wraparound)"
                        );
                    } else {
                        tracing::warn!(
                            elapsed_ms = elapsed.as_millis(), name = % self.name, span =
                            % span, hits = self.consecutive_hits, wraparound =
                            wraparound, "long poll"
                        );
                    }
                } else {
                    self.consecutive_hits = 0;
                }
            }
        }
        pub fn start_section(&mut self, new_line: u32) {
            self.from_line = new_line;
            self.current_start = Some(Instant::now());
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            if let Some(start) = self.current_start {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    self.consecutive_hits = self.consecutive_hits.saturating_add(1);
                    let span = format!(
                        "{file}:{line}-{line}", file = self.file, line = self.from_line
                    );
                    tracing::warn!(
                        elapsed_ms = elapsed.as_millis(), name = % self.name, span = %
                        span, hits = self.consecutive_hits, wraparound = false,
                        "long poll"
                    );
                }
            }
        }
    }
}
pub use dht::{
    DhtClient, DhtConfig, DhtQueryBuilder, DhtQueryMode, DhtQueryWithDataBuilder,
    DhtService, DhtServiceBackgroundTasks, DhtServiceBuilder, DhtValueMerger,
    DhtValueSource, FindValueError, PeerResolver, PeerResolverBuilder,
    PeerResolverConfig, PeerResolverHandle, StorageError, xor_distance,
};
pub use network::{
    BindError, CongestionAlgorithm, Connection, ConnectionError, ConnectionMetricsLevel,
    KnownPeerHandle, KnownPeers, KnownPeersError, Network, NetworkBuilder, NetworkConfig,
    Peer, PeerBannedError, QuicConfig, RecvStream, SendStream, ToSocket,
    WeakKnownPeerHandle, WeakNetwork,
};
pub use quinn;
pub use types::{
    Address, BoxCloneService, BoxService, Direction, DisconnectReason,
    InboundRequestMeta, PeerAffinity, PeerEvent, PeerEventData, PeerId, PeerInfo,
    Request, Response, RpcQuery, Service, ServiceExt, ServiceMessageFn, ServiceQueryFn,
    ServiceRequest, Version, service_message_fn, service_query_fn,
};
pub use self::overlay::{
    ChooseMultiplePrivateOverlayEntries, ChooseMultiplePublicOverlayEntries,
    OverlayConfig, OverlayId, OverlayService, OverlayServiceBackgroundTasks,
    OverlayServiceBuilder, PrivateOverlay, PrivateOverlayBuilder, PrivateOverlayEntries,
    PrivateOverlayEntriesEvent, PrivateOverlayEntriesReadGuard,
    PrivateOverlayEntriesWriteGuard, PrivateOverlayEntryData, PublicOverlay,
    PublicOverlayBuilder, PublicOverlayEntries, PublicOverlayEntriesReadGuard,
    PublicOverlayEntryData, UnknownPeersQueue,
};
pub use self::util::{
    NetworkExt, Routable, Router, RouterBuilder, UnknownPeerError, check_peer_signature,
    try_handle_prefix, try_handle_prefix_with_offset,
};
mod dht;
mod network;
mod overlay;
mod types;
mod util;
pub mod proto {
    pub mod dht;
    pub mod overlay;
}
#[doc(hidden)]
pub mod __internal {
    pub use tl_proto;
}
#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use super::*;
    #[tokio::test]
    async fn init_works() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(init_works)),
            file!(),
            57u32,
        );
        let keypair = rand::random::<tycho_crypto::ed25519::KeyPair>();
        let peer_id: PeerId = keypair.public_key.into();
        let (dht_tasks, dht_service) = DhtService::builder(peer_id).build();
        let (overlay_tasks, overlay_service) = OverlayService::builder(peer_id)
            .with_dht_service(dht_service.clone())
            .build();
        let router = Router::builder()
            .route(dht_service.clone())
            .route(overlay_service.clone())
            .build();
        let network = Network::builder()
            .with_random_private_key()
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();
        dht_tasks.spawn(&network);
        overlay_tasks.spawn(&network);
        let peer_resolver = dht_service.make_peer_resolver().build(&network);
        let private_overlay = PrivateOverlay::builder(rand::random())
            .with_peer_resolver(peer_resolver)
            .build(service_message_fn(|_| futures_util::future::ready(())));
        let public_overlay = PublicOverlay::builder(rand::random())
            .build(service_message_fn(|_| futures_util::future::ready(())));
        overlay_service.add_private_overlay(&private_overlay);
        overlay_service.add_public_overlay(&public_overlay);
    }
}
