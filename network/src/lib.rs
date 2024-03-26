pub use self::overlay::{
    OverlayConfig, OverlayId, OverlayService, OverlayServiceBackgroundTasks, OverlayServiceBuilder,
    PrivateOverlay, PrivateOverlayBuilder, PrivateOverlayEntries, PrivateOverlayEntriesEvent,
    PrivateOverlayEntriesReadGuard, PrivateOverlayEntriesWriteGuard, PublicOverlay,
    PublicOverlayBuilder, PublicOverlayEntries, PublicOverlayEntriesReadGuard,
};
pub use self::util::{check_peer_signature, NetworkExt, Routable, Router, RouterBuilder};
pub use dht::{
    xor_distance, DhtClient, DhtConfig, DhtQueryBuilder, DhtQueryWithDataBuilder, DhtService,
    DhtServiceBackgroundTasks, DhtServiceBuilder, FindValueError, PeerResolver,
    PeerResolverBuilder, PeerResolverHandle, StorageError, ValueMerger,
};
pub use network::{
    ActivePeers, Connection, KnownPeerHandle, KnownPeers, KnownPeersError, Network, NetworkBuilder,
    NetworkConfig, Peer, PeerBannedError, QuicConfig, RecvStream, SendStream, WeakActivePeers,
    WeakKnownPeerHandle, WeakNetwork,
};
pub use types::{
    service_datagram_fn, service_message_fn, service_query_fn, Address, BoxCloneService,
    BoxService, Direction, DisconnectReason, InboundRequestMeta, PeerAffinity, PeerEvent, PeerId,
    PeerInfo, Request, Response, RpcQuery, Service, ServiceDatagramFn, ServiceExt,
    ServiceMessageFn, ServiceQueryFn, ServiceRequest, Version,
};

pub use quinn;

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
        let keypair = everscale_crypto::ed25519::KeyPair::generate(&mut rand::thread_rng());
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
            .with_service_name("test-service")
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
