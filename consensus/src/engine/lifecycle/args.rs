use std::sync::Arc;

use tokio::sync::mpsc;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::{Network, OverlayService, PeerResolver, PrivateOverlay};

use crate::effects::{AltFormat, TaskTracker};
use crate::engine::round_watch::{Commit, RoundWatch, TopKnownAnchor};
use crate::engine::{InputBuffer, MempoolMergedConfig};
use crate::intercom::{Dispatcher, InitPeers, PeerSchedule, Responder};
use crate::models::MempoolOutput;
use crate::storage::MempoolDb;

#[derive(Clone)]
pub struct EngineBinding {
    pub mempool_db: Arc<MempoolDb>,
    pub input_buffer: InputBuffer,
    pub top_known_anchor: RoundWatch<TopKnownAnchor>,
    pub commit_finished: RoundWatch<Commit>,
    pub anchors_tx: mpsc::UnboundedSender<MempoolOutput>,
}

#[derive(Clone)]
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
}

impl EngineNetwork {
    pub(super) fn new(
        net_args: &EngineNetworkArgs,
        task_tracker: &TaskTracker,
        merged_conf: &MempoolMergedConfig,
        init_peers: &InitPeers,
    ) -> Self {
        let responder = Responder::default();

        let private_overlay = PrivateOverlay::builder(merged_conf.overlay_id)
            .with_peer_resolver(net_args.peer_resolver.clone())
            .named("tycho-consensus")
            .build(responder.clone());

        let overlay_service = net_args.overlay_service.clone();
        overlay_service.add_private_overlay(&private_overlay);

        tracing::info!(
            peer_id = %net_args.network.peer_id().alt(),
            overlay_id = %merged_conf.overlay_id,
            "mempool overlay added"
        );

        let dispatcher = Dispatcher::new(&net_args.network, &private_overlay, &merged_conf.conf);
        let peer_schedule =
            PeerSchedule::new(net_args.key_pair.clone(), private_overlay, task_tracker);
        peer_schedule.init(merged_conf, init_peers);

        Self {
            peer_schedule,
            dispatcher,
            responder,
        }
    }
}

/// Use `false` to read point history from old to new rounds, preserving their stored statuses.
/// This is the default mode to run [`Engine`](crate::engine::Engine).
///
/// Use `true` to read point statuses in inverted order of rounds, re-validating invalid points.
/// This allows to fix rare cases, when node synced slowly and began to validate new broadcasts
/// with all history being invalid because some point was not downloaded.
#[derive(Default, Clone, Copy)]
pub struct FixHistoryFlag(pub bool);
