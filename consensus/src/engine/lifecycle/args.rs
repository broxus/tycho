use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;
use tokio::sync::mpsc;
use tycho_network::{Network, OverlayId, OverlayService, PeerResolver, PrivateOverlay};

use crate::effects::MempoolAdapterStore;
use crate::engine::round_watch::{RoundWatch, TopKnownAnchor};
use crate::engine::{InputBuffer, MempoolMergedConfig};
use crate::intercom::{Dispatcher, PeerSchedule, Responder};
use crate::models::MempoolOutput;

#[derive(Clone)]
pub struct EngineBinding {
    pub mempool_adapter_store: MempoolAdapterStore,
    pub input_buffer: InputBuffer,
    pub top_known_anchor: RoundWatch<TopKnownAnchor>,
    pub output: mpsc::UnboundedSender<MempoolOutput>,
}

// may derive 'Clone' if needed
pub struct EngineNetworkArgs {
    pub key_pair: Arc<KeyPair>,
    pub network: Network,
    pub peer_resolver: PeerResolver,
    pub overlay_service: OverlayService,
}

// private to crate, do not impl `Clone`
pub struct EngineNetwork {
    pub peer_schedule: PeerSchedule,
    pub dispatcher: Dispatcher,
    /// dropped at full restart
    pub responder: Responder,
    overlay_service: OverlayService,
    overlay_id: OverlayId,
}

impl EngineNetwork {
    pub(super) fn new(net_args: &EngineNetworkArgs, merged_conf: &MempoolMergedConfig) -> Self {
        let responder = Responder::default();
        let overlay_id = merged_conf.overlay_id;

        let private_overlay = PrivateOverlay::builder(overlay_id)
            .with_peer_resolver(net_args.peer_resolver.clone())
            .named("tycho-consensus")
            .build(responder.clone());

        let overlay_service = net_args.overlay_service.clone();
        overlay_service.add_private_overlay(&private_overlay);

        tracing::info!(
            peer_id = %net_args.network.peer_id(),
            %overlay_id,
            "mempool overlay added"
        );

        let dispatcher = Dispatcher::new(&net_args.network, &private_overlay);
        let peer_schedule =
            PeerSchedule::new(net_args.key_pair.clone(), private_overlay, merged_conf);
        Self {
            peer_schedule,
            dispatcher,
            responder,
            overlay_service,
            overlay_id,
        }
    }
}

impl Drop for EngineNetwork {
    fn drop(&mut self) {
        self.overlay_service
            .remove_private_overlay(&self.overlay_id);
    }
}
