pub use self::overlay::{
    OverlayConfig, OverlayId, OverlayService, OverlayServiceBackgroundTasks, OverlayServiceBuilder,
    PrivateOverlay, PrivateOverlayBuilder, PrivateOverlayEntries, PrivateOverlayEntriesReadGuard,
    PrivateOverlayEntriesWriteGuard, PublicOverlay, PublicOverlayBuilder, PublicOverlayEntries,
    PublicOverlayEntriesReadGuard,
};
pub use self::util::{check_peer_signature, NetworkExt, Routable, Router, RouterBuilder};
pub use dht::{
    xor_distance, DhtClient, DhtClientBuilder, DhtConfig, DhtQueryBuilder, DhtQueryWithDataBuilder,
    DhtService, DhtServiceBuilder, FindValueError, OverlayValueMerger, StorageError,
};
pub use network::{
    ActivePeers, Connection, KnownPeer, KnownPeers, Network, NetworkBuilder, NetworkConfig, Peer,
    QuicConfig, RecvStream, SendStream, WeakActivePeers, WeakNetwork,
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

        let private_overlay = PrivateOverlay::builder(rand::random())
            .build(service_message_fn(|_| futures_util::future::ready(())));

        let public_overlay = PublicOverlay::builder(rand::random())
            .build(service_message_fn(|_| futures_util::future::ready(())));

        let (dht_client, dht) = DhtService::builder(peer_id).build();

        let (overlay_tasks, overlay_service) = OverlayService::builder(peer_id)
            .with_dht_service(dht.clone())
            .with_private_overlay(&private_overlay)
            .with_public_overlay(&public_overlay)
            .build();

        let router = Router::builder().route(dht).route(overlay_service).build();

        let network = Network::builder()
            .with_random_private_key()
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        let _dht_client = dht_client.build(network.clone());
        overlay_tasks.spawn(network);
    }
}
