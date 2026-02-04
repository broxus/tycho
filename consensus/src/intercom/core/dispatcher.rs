use std::sync::Arc;

use anyhow::Result;
use tycho_network::{Body, Network, PeerId, PrivateOverlay, Response};

use crate::engine::MempoolConfig;
use crate::intercom::core::bcast_rate_limit::BcastSenderLimits;
use crate::intercom::core::download_rate_limit::DownloaderLimits;

#[derive(Clone)]
pub struct Dispatcher(Arc<DispatcherInner>);
pub struct DispatcherInner {
    network: Network,
    overlay: PrivateOverlay,
    bcast_rate_limit: BcastSenderLimits,
    download_rate_limit: DownloaderLimits,
}

impl Dispatcher {
    pub fn new(network: &Network, private_overlay: &PrivateOverlay, conf: &MempoolConfig) -> Self {
        Self(Arc::new(DispatcherInner {
            network: network.clone(),
            overlay: private_overlay.clone(),
            bcast_rate_limit: BcastSenderLimits::new(conf),
            download_rate_limit: DownloaderLimits::new(conf),
        }))
    }

    pub(super) fn bcast_rate_limit(&self) -> &BcastSenderLimits {
        &self.0.bcast_rate_limit
    }

    pub(super) fn download_rate_limit(&self) -> &DownloaderLimits {
        &self.0.download_rate_limit
    }

    pub(super) async fn query(&self, peer_id: &PeerId, request: Body) -> Result<Response> {
        (self.0.overlay)
            .query(&self.0.network, peer_id, request)
            .await
    }

    pub fn forget_peers(&self, to_forget: &[PeerId]) {
        self.0.bcast_rate_limit.remove(to_forget);
        self.0.download_rate_limit.remove(to_forget);
    }
}

#[cfg(feature = "mock-feedback")]
pub struct MockFeedbackMessage {
    dispatcher: Dispatcher,
    request: bytes::Bytes,
}
#[cfg(feature = "mock-feedback")]
impl MockFeedbackMessage {
    pub fn new(dispatcher: Dispatcher, round: crate::models::Round) -> Self {
        let request = bytes::Bytes::from(tl_proto::serialize(crate::mock_feedback::RoundBoxed {
            round,
        }));
        Self {
            dispatcher,
            request,
        }
    }
    pub async fn send(&self, peer_id: &PeerId) -> anyhow::Result<()> {
        let overlay = &self.dispatcher.0.overlay;
        let network = &self.dispatcher.0.network;
        overlay
            .send(network, peer_id, self.request.clone().into())
            .await
    }
}
