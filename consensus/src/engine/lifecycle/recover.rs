use std::sync::Arc;

use futures_util::never::Never;
use parking_lot::Mutex;

use crate::dag::HistoryConflict;
use crate::effects::{AltFormat, Cancelled, Task, TaskTracker};
use crate::engine::lifecycle::{
    EngineBinding, EngineError, EngineNetwork, EngineNetworkArgs, FixHistoryFlag,
};
use crate::engine::{Engine, MempoolMergedConfig};
use crate::intercom::{InitPeers, WeakPeerSchedule};

pub struct EngineRecoverLoop {
    // to create new engine run
    pub bind: EngineBinding,
    pub net_args: EngineNetworkArgs,
    pub merged_conf: MempoolMergedConfig,
    // current run
    pub run_attrs: Arc<Mutex<RunAttributes>>,
}

impl Drop for EngineRecoverLoop {
    fn drop(&mut self) {
        self.net_args
            .overlay_service
            .remove_private_overlay(&self.merged_conf.overlay_id);
    }
}

pub struct RunAttributes {
    pub tracker: TaskTracker,
    pub is_stopping: bool,
    pub peer_schedule: WeakPeerSchedule,
    pub last_peers: InitPeers,
    #[cfg(feature = "mock-feedback")]
    pub mock_feedback: Task<Never>,
}

impl EngineRecoverLoop {
    pub async fn run_loop(self, mut engine_run: Task<Result<Never, HistoryConflict>>) {
        loop {
            tracing::info!(
                peer_id = %self.net_args.network.peer_id().alt(),
                overlay_id = %self.merged_conf.overlay_id,
                genesis_info = ?self.merged_conf.genesis_info,
                conf = ?self.merged_conf.conf,
                "mempool run"
            );
            metrics::gauge!(
                "tycho_mempool_engine_run_count",
                "genesis_round" => self.merged_conf.conf.genesis_round.0.to_string()
            )
            .increment(1);

            let never_ok = engine_run.await;

            self.net_args
                .overlay_service
                .remove_private_overlay(&self.merged_conf.overlay_id);

            let task_tracker = {
                let guard = self.run_attrs.lock();
                guard.tracker.clone()
            };

            task_tracker.stop().await;
            drop(task_tracker);

            let fix_history = match never_ok {
                Err(Cancelled()) => break,
                Ok(Err(HistoryConflict(_))) => FixHistoryFlag(true),
            };

            let (task_tracker, net) = {
                let mut guard = self.run_attrs.lock();
                if guard.is_stopping {
                    break; // do not update task tracker
                }
                guard.tracker = TaskTracker::default();
                let net = EngineNetwork::new(
                    &self.net_args,
                    &guard.tracker,
                    &self.merged_conf,
                    &guard.last_peers,
                );
                guard.peer_schedule = net.peer_schedule.downgrade();

                #[cfg(feature = "mock-feedback")]
                {
                    use crate::mock_feedback::MockFeedbackSender;
                    let sender = MockFeedbackSender::new(
                        net.dispatcher.clone(),
                        guard.peer_schedule.clone(),
                        self.bind.top_known_anchor.clone(),
                        &guard.last_peers,
                        self.net_args.network.peer_id(),
                    );
                    guard.mock_feedback = guard.tracker.ctx().spawn(sender.run());
                }

                (guard.tracker.clone(), net)
            };

            let engine = Engine::new(
                &task_tracker,
                &self.bind,
                &net,
                &self.merged_conf,
                fix_history,
            );

            engine_run = task_tracker.ctx().spawn(async move {
                match engine.run().await {
                    Err(EngineError::Cancelled) => Err(Cancelled()),
                    Err(EngineError::HistoryConflict(e)) => Ok(Err(e)),
                }
            });
        }
    }
}
