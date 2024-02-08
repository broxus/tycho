use std::future::Future;

use anyhow::Result;
use bytes::Bytes;

use crate::network::Network;
use crate::types::{PeerEvent, PeerId, Request, Response};

pub trait NetworkExt {
    fn query(
        &self,
        peer_id: &PeerId,
        request: Request<Bytes>,
    ) -> impl Future<Output = Result<Response<Bytes>>> + Send;
}

impl NetworkExt for Network {
    async fn query(&self, peer_id: &PeerId, request: Request<Bytes>) -> Result<Response<Bytes>> {
        use tokio::sync::broadcast::error::RecvError;

        let mut peer_events = self.subscribe()?;

        // Make query if already connected
        if let Some(peer) = self.peer(peer_id) {
            return peer.rpc(request).await;
        }

        match self.known_peers().get(peer_id) {
            // Initiate a connection of it is a known peer
            Some(peer_info) => {
                self.connect_with_peer_id(peer_info.address, peer_id)
                    .await?;
            }
            // Error otherwise
            None => anyhow::bail!("trying to query an unknown peer: {peer_id}"),
        }

        loop {
            match peer_events.recv().await {
                Ok(PeerEvent::NewPeer(new_peer_id)) if &new_peer_id == peer_id => {
                    if let Some(peer) = self.peer(peer_id) {
                        return peer.rpc(request).await;
                    }
                }
                Ok(_) => {}
                Err(RecvError::Closed) => anyhow::bail!("network subscription closed"),
                Err(RecvError::Lagged(_)) => {
                    peer_events = peer_events.resubscribe();

                    if let Some(peer) = self.peer(peer_id) {
                        return peer.rpc(request).await;
                    }
                }
            }

            anyhow::ensure!(
                self.known_peers().contains(peer_id),
                "waiting for a connection to an unknown peer: {peer_id}",
            );
        }
    }
}
