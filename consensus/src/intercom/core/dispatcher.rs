use anyhow::Result;
use tycho_network::{Network, PeerId, PrivateOverlay, Request, Response};

#[derive(Clone)]
pub struct Dispatcher {
    network: Network,
    overlay: PrivateOverlay,
}

impl Dispatcher {
    pub fn new(network: &Network, private_overlay: &PrivateOverlay) -> Self {
        Self {
            network: network.clone(),
            overlay: private_overlay.clone(),
        }
    }
    pub(super) async fn query(&self, peer_id: &PeerId, request: Request) -> Result<Response> {
        self.overlay.query(&self.network, peer_id, request).await
    }
}

#[cfg(feature = "mock-feedback")]
pub struct MockFeedbackMessage {
    dispatcher: Dispatcher,
    request: Request,
}
#[cfg(feature = "mock-feedback")]
impl MockFeedbackMessage {
    pub fn new(dispatcher: Dispatcher, round: crate::models::Round) -> Self {
        Self {
            dispatcher,
            request: Request::from_tl(crate::mock_feedback::RoundBoxed { round }),
        }
    }
    pub async fn send(&self, peer_id: &PeerId) -> anyhow::Result<()> {
        let overlay = &self.dispatcher.overlay;
        let network = &self.dispatcher.network;
        overlay.send(network, peer_id, self.request.clone()).await
    }
}
