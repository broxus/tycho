use anyhow::{anyhow, Result};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;

use tycho_network::{DhtClient, Network, OverlayId, OverlayService, PeerId, PrivateOverlay};

use crate::intercom::core::dto::{MPRequest, MPResponse};
use crate::intercom::core::responder::Responder;
use crate::intercom::dto::PointByIdResponse;
use crate::models::{Point, PointId, Round};

#[derive(Clone)]
pub struct Dispatcher {
    pub overlay: PrivateOverlay,
    network: Network,
}

impl Dispatcher {
    const PRIVATE_OVERLAY_ID: OverlayId = OverlayId(*b"ac87b6945b4f6f736963f7f65d025943");

    pub fn new(
        dht_client: &DhtClient,
        overlay_service: &OverlayService,
        all_peers: &Vec<PeerId>,
        responder: Responder,
    ) -> Self {
        let dht_service = dht_client.service();
        let peer_resolver = dht_service.make_peer_resolver().build(dht_client.network());

        let private_overlay = PrivateOverlay::builder(Self::PRIVATE_OVERLAY_ID)
            .with_peer_resolver(peer_resolver)
            .with_entries(all_peers)
            .build(responder);

        overlay_service.add_private_overlay(&private_overlay);

        Self {
            overlay: private_overlay,
            network: dht_client.network().clone(),
        }
    }

    pub async fn point_by_id(&self, peer: &PeerId, id: &PointId) -> Result<PointByIdResponse> {
        let request = (&MPRequest::PointById(id.clone())).into();
        let response = self.overlay.query(&self.network, peer, request).await?;
        PointByIdResponse::try_from(MPResponse::try_from(&response)?)
    }

    pub fn broadcast_request(point: &Point) -> tycho_network::Request {
        (&MPRequest::Broadcast(point.clone())).into()
    }

    pub fn signature_request(round: &Round) -> tycho_network::Request {
        (&MPRequest::Signature(round.clone())).into()
    }

    pub fn request<T>(
        &self,
        peer_id: &PeerId,
        request: &tycho_network::Request,
    ) -> BoxFuture<'static, (PeerId, Result<T>)>
    where
        T: TryFrom<MPResponse, Error = anyhow::Error>,
    {
        let peer_id = peer_id.clone();
        let request = request.clone();
        let overlay = self.overlay.clone();
        let network = self.network.clone();
        async move {
            overlay
                .query(&network, &peer_id, request)
                .map(move |response| {
                    let response = response
                        .and_then(|r| MPResponse::try_from(&r))
                        .and_then(T::try_from)
                        .map_err(|e| anyhow!("response from peer {peer_id}: {e}"));
                    (peer_id, response)
                })
                .await
        }
        .boxed()
    }
}

/* FIXME
#[cfg(test)]
mod tests {
    use tycho_network::{Address, PeerInfo};
    use tycho_util::time::now_sec;

    use crate::engine::node_count::NodeCount;
    use crate::engine::peer_schedule::PeerSchedule;
    use crate::models::point::Digest;

    use super::*;

    fn make_peer_info(key: &ed25519::SecretKey, address: Address) -> PeerInfo {
        let keypair = ed25519::KeyPair::from(key);
        let peer_id = PeerId::from(keypair.public_key);

        let now = now_sec();
        let mut peer_info = PeerInfo {
            id: peer_id,
            address_list: vec![address].into_boxed_slice(),
            created_at: now,
            expires_at: u32::MAX,
            signature: Box::new([0; 64]),
        };
        *peer_info.signature = keypair.sign(&peer_info);
        peer_info
    }

    async fn make_network(node_count: usize) -> Vec<Dispatcher> {
        let keys = (0..node_count)
            .map(|_| ed25519::SecretKey::generate(&mut rand::thread_rng()))
            .collect::<Vec<_>>();

        let all_peers = keys
            .iter()
            .map(|s| PeerId::from(ed25519::KeyPair::from(s).public_key))
            .collect::<Vec<_>>();

        let nodes = keys
            .iter()
            .map(|s| Dispatcher::new((Ipv4Addr::LOCALHOST, 0), s, &all_peers))
            .collect::<Vec<_>>();

        let bootstrap_info = std::iter::zip(&keys, &nodes)
            .map(|(key, peer)| Arc::new(make_peer_info(key, peer.network.local_addr().into())))
            .collect::<Vec<_>>();

        let schedules = std::iter::zip(&all_peers, &nodes)
            .map(|(peer_id, peer)| PeerSchedule::new(Round(0), &all_peers, &peer.overlay, peer_id))
            .collect::<Vec<_>>();

        if let Some(node) = nodes.first() {
            for info in &bootstrap_info {
                if info.id == node.network.peer_id() {
                    continue;
                }
                node.dht_client.add_peer(info.clone()).unwrap();
            }
        }

        // let all_peers = FastHashSet::from_iter(all_peers.into_iter());
        for sch in &schedules {
            sch.wait_for_peers(Round(1), NodeCount::new(node_count))
                .await;
            tracing::info!("found peers for {}", sch.local_id);
        }

        nodes
    }

    #[tokio::test]
    async fn dispatcher_works() -> Result<()> {
        tracing_subscriber::fmt::try_init().ok();
        tracing::info!("dispatcher_works");

        let peers = make_network(3).await;

        let point_id = PointId {
            location: crate::models::point::Location {
                round: Round(0),
                author: PeerId([0u8; 32]),
            },
            digest: Digest([0u8; 32]),
        };

        // FIXME must connect only to resolved peers
        for i in 0..peers.len() {
            for j in 0..peers.len() {
                if i == j {
                    continue;
                }

                let left = &peers[i];
                let right = &peers[j];

                let point_opt = left
                    .point_by_id(right.network.peer_id(), point_id.clone())
                    .await?;
                assert!(point_opt.is_none());
            }
        }
        Ok(())
    }
}
*/
