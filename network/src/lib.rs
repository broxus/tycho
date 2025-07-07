pub use dht::{
    DhtClient, DhtConfig, DhtQueryBuilder, DhtQueryMode, DhtQueryWithDataBuilder, DhtService,
    DhtServiceBackgroundTasks, DhtServiceBuilder, DhtValueMerger, DhtValueSource, FindValueError,
    PeerResolver, PeerResolverBuilder, PeerResolverConfig, PeerResolverHandle, StorageError,
    xor_distance,
};
pub use network::{
    BindError, Connection, ConnectionError, KnownPeerHandle, KnownPeers, KnownPeersError, Network,
    NetworkBuilder, NetworkConfig, Peer, PeerBannedError, QuicConfig, RecvStream, SendStream,
    ToSocket, WeakKnownPeerHandle, WeakNetwork,
};
pub use quinn;
pub use types::{
    Address, BoxCloneService, BoxService, Direction, DisconnectReason, InboundRequestMeta,
    PeerAffinity, PeerEvent, PeerEventData, PeerId, PeerInfo, Request, Response, RpcQuery, Service,
    ServiceExt, ServiceMessageFn, ServiceQueryFn, ServiceRequest, Version, service_message_fn,
    service_query_fn,
};

pub use self::overlay::{
    ChooseMultiplePrivateOverlayEntries, ChooseMultiplePublicOverlayEntries, OverlayConfig,
    OverlayId, OverlayService, OverlayServiceBackgroundTasks, OverlayServiceBuilder,
    PrivateOverlay, PrivateOverlayBuilder, PrivateOverlayEntries, PrivateOverlayEntriesEvent,
    PrivateOverlayEntriesReadGuard, PrivateOverlayEntriesWriteGuard, PrivateOverlayEntryData,
    PublicOverlay, PublicOverlayBuilder, PublicOverlayEntries, PublicOverlayEntriesReadGuard,
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
