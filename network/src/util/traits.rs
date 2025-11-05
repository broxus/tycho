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
    fn send(
        &self,
        peer_id: &PeerId,
        request: Request,
    ) -> impl Future<Output = Result<()>> + Send;
}
impl NetworkExt for Network {
    async fn query(&self, peer_id: &PeerId, request: Request) -> Result<Response> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(query)),
            file!(),
            19u32,
        );
        let peer_id = peer_id;
        let request = request;
        {
            __guard.end_section(20u32);
            let __result = on_connected_peer(self, Peer::rpc, peer_id, request).await;
            __guard.start_section(20u32);
            __result
        }
    }
    async fn send(&self, peer_id: &PeerId, request: Request) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(send)),
            file!(),
            23u32,
        );
        let peer_id = peer_id;
        let request = request;
        {
            __guard.end_section(24u32);
            let __result = on_connected_peer(self, Peer::send_message, peer_id, request)
                .await;
            __guard.start_section(24u32);
            __result
        }
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
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(on_connected_peer)),
        file!(),
        36u32,
    );
    let network = network;
    let f = f;
    let peer_id = peer_id;
    let request = request;
    let peer = 'peer: {
        if let Some(peer) = network.peer(peer_id) {
            {
                __guard.end_section(40u32);
                __guard.start_section(40u32);
                break 'peer peer;
            };
        }
        match network.known_peers().get(peer_id) {
            Some(peer_info) => {
                let address = peer_info
                    .iter_addresses()
                    .next()
                    .cloned()
                    .expect("address list must have at least one item");
                {
                    __guard.end_section(54u32);
                    let __result = network.connect(address, peer_id).await;
                    __guard.start_section(54u32);
                    __result
                }?
            }
            None => anyhow::bail!(UnknownPeerError { peer_id : * peer_id }),
        }
    };
    {
        __guard.end_section(61u32);
        let __result = f.call(&peer, request).await;
        __guard.start_section(61u32);
        __result
    }
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
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("trying to interact with an unknown peer: {peer_id}")]
pub struct UnknownPeerError {
    pub peer_id: PeerId,
}
