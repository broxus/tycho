use std::sync::Arc;

use anyhow::Result;
use tycho_network::{Network, PeerId, PrivateOverlay, Request, Response};

use crate::engine::MempoolConfig;
use crate::intercom::core::download_rate_limit::DownloaderRateLimit;

#[derive(Clone)]
pub struct Dispatcher(Arc<DispatcherInner>);
pub struct DispatcherInner {
    network: Network,
    overlay: PrivateOverlay,
    download_rate_limit: DownloaderRateLimit,
}

impl Dispatcher {
    pub fn new(network: &Network, private_overlay: &PrivateOverlay, conf: &MempoolConfig) -> Self {
        Self(Arc::new(DispatcherInner {
            network: network.clone(),
            overlay: private_overlay.clone(),
            download_rate_limit: DownloaderRateLimit::new(conf),
        }))
    }

    pub(super) fn download_rate_limit(&self) -> &DownloaderRateLimit {
        &self.0.download_rate_limit
    }

    pub(super) async fn query(&self, peer_id: &PeerId, request: Request) -> Result<Response> {
        (self.0.overlay)
            .query(&self.0.network, peer_id, request)
            .await
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
        let overlay = &self.dispatcher.0.overlay;
        let network = &self.dispatcher.0.network;
        overlay.send(network, peer_id, self.request.clone()).await
    }
}
