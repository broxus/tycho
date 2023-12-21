use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Result;
use bytes::Bytes;
use futures_util::FutureExt;

use crate::network::Network;
use crate::types::{PeerEvent, PeerId, Request, Response};

pub trait NetworkExt {
    fn query(&self, peer_id: &PeerId, request: Request<Bytes>) -> Query;
}

impl NetworkExt for Network {
    fn query(&self, peer_id: &PeerId, request: Request<Bytes>) -> Query {
        use tokio::sync::broadcast::error::RecvError;

        let network = self.clone();
        let peer_id = *peer_id;
        Query(Box::pin(async move {
            let mut peer_events = network.subscribe()?;

            // Make query if already connected
            if let Some(peer) = network.peer(&peer_id) {
                return peer.rpc(request).await;
            }

            match network.known_peers().get(&peer_id) {
                // Initiate a connection of it is a known peer
                Some(peer_info) => {
                    network
                        .connect_with_peer_id(peer_info.address, &peer_id)
                        .await?;
                }
                // Error otherwise
                None => anyhow::bail!("trying to query an unknown peer: {peer_id}"),
            }

            loop {
                match peer_events.recv().await {
                    Ok(PeerEvent::NewPeer(peer_id)) if peer_id == peer_id => {
                        if let Some(peer) = network.peer(&peer_id) {
                            return peer.rpc(request).await;
                        }
                    }
                    Ok(_) => {}
                    Err(RecvError::Closed) => anyhow::bail!("network subscription closed"),
                    Err(RecvError::Lagged(_)) => {
                        peer_events = peer_events.resubscribe();

                        if let Some(peer) = network.peer(&peer_id) {
                            return peer.rpc(request).await;
                        }
                    }
                }

                anyhow::ensure!(
                    network.known_peers().contains(&peer_id),
                    "waiting for a connection to an unknown peer: {peer_id}",
                );
            }
        }))
    }
}

// TODO: replace with RPITIT
pub struct Query(Pin<Box<dyn Future<Output = Result<Response<Bytes>>> + Send + 'static>>);

impl Future for Query {
    type Output = Result<Response<Bytes>>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}
