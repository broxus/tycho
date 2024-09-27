use std::future::Future;

use anyhow::Result;

use crate::network::{Network, Peer};
use crate::types::{PeerId, Request, Response};

pub trait NetworkExt {
    fn query(
        &self,
        peer_id: &PeerId,
        request: Request,
    ) -> impl Future<Output = Result<Response>> + Send;

    fn send(&self, peer_id: &PeerId, request: Request) -> impl Future<Output = Result<()>> + Send;
}

impl NetworkExt for Network {
    async fn query(&self, peer_id: &PeerId, request: Request) -> Result<Response> {
        on_connected_peer(self, Peer::rpc, peer_id, request).await
    }

    async fn send(&self, peer_id: &PeerId, request: Request) -> Result<()> {
        on_connected_peer(self, Peer::send_message, peer_id, request).await
    }
}

async fn on_connected_peer<T, F>(
    network: &Network,
    f: F,
    peer_id: &PeerId,
    request: Request,
) -> Result<T>
where
    for<'a> F: PeerTask<'a, T>,
{
    let peer = 'peer: {
        // Interact if already connected
        if let Some(peer) = network.peer(peer_id) {
            break 'peer peer;
        }

        // Try to connect
        match network.known_peers().get(peer_id) {
            // Initiate a connection of it is a known peer
            Some(peer_info) => {
                // TODO: try multiple addresses
                let address = peer_info
                    .iter_addresses()
                    .next()
                    .cloned()
                    .expect("address list must have at least one item");

                network.connect(address, peer_id).await?
            }
            // Error otherwise
            None => anyhow::bail!("trying to interact with an unknown peer: {peer_id}"),
        }
    };

    f.call(&peer, request).await
}

trait PeerTask<'a, T> {
    type Output: Future<Output = Result<T>> + 'a;

    fn call(self, peer: &'a Peer, request: Request) -> Self::Output;
}

impl<'a, T, F, Fut> PeerTask<'a, T> for F
where
    F: FnOnce(&'a Peer, Request) -> Fut,
    Fut: Future<Output = Result<T>> + 'a,
{
    type Output = Fut;

    #[inline]
    fn call(self, peer: &'a Peer, request: Request) -> Fut {
        self(peer, request)
    }
}
