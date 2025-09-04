use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio_util::task::AbortOnDropHandle;
use tycho_types::models::GenesisInfo;

use crate::effects::{Cancelled, TaskTracker};
use crate::engine::lifecycle::recover::{EngineRecoverLoop, RunAttributes};
use crate::engine::lifecycle::session::isolated::SpanFields;
use crate::engine::lifecycle::{
    EngineBinding, EngineError, EngineNetwork, EngineNetworkArgs, FixHistoryFlag,
};
use crate::engine::{Engine, MempoolMergedConfig};
use crate::intercom::InitPeers;

pub struct EngineSession {
    genesis_info: GenesisInfo,
    span_fields: SpanFields,
    recover_loop: AbortOnDropHandle<()>,
    run_attrs: Arc<Mutex<RunAttributes>>,
    stop_tx: oneshot::Sender<()>,
}

impl EngineSession {
    pub fn new(
        bind: EngineBinding,
        net_args: &EngineNetworkArgs,
        merged_conf: &MempoolMergedConfig,
        init_peers: InitPeers,
        engine_stop_tx: oneshot::Sender<()>,
    ) -> Self {
        let span_fields = SpanFields::new(net_args, merged_conf);

        let task_tracker = TaskTracker::default();
        let net = EngineNetwork::new(net_args, &task_tracker, merged_conf, &init_peers);
        let engine = Engine::new(
            &task_tracker,
            &bind,
            &net,
            merged_conf,
            FixHistoryFlag::default(),
        );
        let peer_schedule = net.peer_schedule.downgrade();
        let run_attrs = Arc::new(Mutex::new(RunAttributes {
            tracker: task_tracker.clone(),
            is_stopping: false,
            peer_schedule: peer_schedule.clone(),
            #[cfg(feature = "mock-feedback")]
            mock_feedback: {
                use crate::mock_feedback::MockFeedbackSender;
                net.responder.set_top_known_anchor(&bind.top_known_anchor);
                let sender = MockFeedbackSender::new(
                    net.dispatcher.clone(),
                    peer_schedule,
                    bind.top_known_anchor.clone(),
                    &init_peers,
                    net_args.network.peer_id(),
                );
                task_tracker.ctx().spawn(sender.run())
            },
            last_peers: init_peers,
        }));

        let recover_loop = AbortOnDropHandle::new(tokio::spawn(
            EngineRecoverLoop {
                bind,
                net_args: net_args.clone(),
                merged_conf: merged_conf.clone(),
                run_attrs: run_attrs.clone(),
            }
            .run_loop(task_tracker.ctx().spawn(async move {
                match engine.run().await {
                    Err(EngineError::Cancelled) => Err(Cancelled()),
                    Err(EngineError::HistoryConflict(e)) => Ok(Err(e)),
                }
            })),
        ));

        Self {
            genesis_info: merged_conf.genesis_info(),
            span_fields,
            stop_tx: engine_stop_tx,
            run_attrs,
            recover_loop,
        }
    }

    pub fn genesis_info(&self) -> GenesisInfo {
        self.genesis_info
    }

    pub fn set_peers(&self, peers: InitPeers) {
        let mut run_attrs = self.run_attrs.lock();
        if let Some(peer_schedule) = run_attrs.peer_schedule.upgrade() {
            peer_schedule.set_peers(&peers);
        }
        run_attrs.last_peers = peers;
    }

    pub async fn stop(self) {
        let span = self.span_fields.stop_span();

        span.in_scope(|| tracing::warn!("waiting engine threads to exit"));

        let engine_tracker = {
            let mut guard = self.run_attrs.lock();
            guard.is_stopping = true;
            guard.tracker.clone()
        };
        drop(self.run_attrs); // drops `PeerSchedule` clone inside
        engine_tracker.stop().await;
        self.recover_loop.await.ok();

        span.in_scope(|| tracing::warn!("stop completed"));

        self.stop_tx.send(()).ok();
    }
}

mod isolated {
    use tracing::Span;
    use tycho_network::{OverlayId, PeerId};

    use crate::effects::AltFormat;
    use crate::engine::MempoolMergedConfig;
    use crate::engine::lifecycle::EngineNetworkArgs;
    use crate::models::Round;

    pub struct SpanFields {
        peer_id: PeerId,
        overlay_id: OverlayId,
        genesis_round: Round,
    }

    impl SpanFields {
        pub fn new(net_args: &EngineNetworkArgs, merged_conf: &MempoolMergedConfig) -> Self {
            Self {
                peer_id: *net_args.network.peer_id(),
                overlay_id: merged_conf.overlay_id,
                genesis_round: merged_conf.conf.genesis_round,
            }
        }

        pub fn stop_span(&self) -> Span {
            tracing::error_span!(
                "mempool stop in progress",
                peer = %self.peer_id.alt(),
                genesis_round = self.genesis_round.0,
                overlay = %self.overlay_id,
            )
        }
    }
}
