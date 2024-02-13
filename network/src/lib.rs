pub use self::util::{check_peer_signature, NetworkExt, Routable, Router, RouterBuilder};
pub use dht::{DhtClient, DhtClientBuilder, DhtService, DhtServiceBuilder, StorageBuilder};
pub use network::{
    ActivePeers, Connection, KnownPeers, Network, NetworkBuilder, NetworkConfig, Peer, QuicConfig,
    RecvStream, SendStream, WeakActivePeers, WeakNetwork,
};
pub use types::{
    service_datagram_fn, service_message_fn, service_query_fn, Address, AddressList,
    BoxCloneService, BoxService, Direction, DisconnectReason, InboundRequestMeta, PeerAffinity,
    PeerEvent, PeerId, PeerInfo, Request, Response, RpcQuery, Service, ServiceDatagramFn,
    ServiceExt, ServiceMessageFn, ServiceQueryFn, ServiceRequest, Version,
};

pub use quinn;

mod dht;
mod network;
mod types;
mod util;

pub mod proto {
    pub mod dht;
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

        let (dht_client, dht) = DhtService::builder(keypair.public_key.into())
            .with_storage(|builder| builder)
            .build();

        let router = Router::builder().route(dht).build();

        let network = Network::builder()
            .with_random_private_key()
            .with_service_name("test-service")
            .build((Ipv4Addr::LOCALHOST, 0), router)
            .unwrap();

        let _dht_client = dht_client.build(network);
    }
}
