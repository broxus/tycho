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
    }
    impl Guard {
        pub fn new(name: &'static str, file: &'static str, line: u32) -> Self {
            Guard {
                name,
                file,
                from_line: line,
                current_start: Some(Instant::now()),
            }
        }
        pub fn end_section(&mut self, to_line: u32) {
            if let Some(start) = self.current_start.take() {
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(THRESHOLD_MS) {
                    tracing::warn!(
                        elapsed_ms = elapsed.as_millis(), name = % self.name, file = %
                        self.file, from_line = % self.from_line, to_line = % to_line,
                        "long poll"
                    );
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
                    tracing::warn!(
                        elapsed_ms = elapsed.as_millis(), name = % self.name, file = %
                        self.file, from_line = % self.from_line, to_line = % self
                        .from_line, "long poll"
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
            line!(),
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
