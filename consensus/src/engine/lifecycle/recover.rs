use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::effects::{AltFormat, Cancelled, TaskTracker};
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
    pub mock_feedback: crate::effects::Task<futures_util::never::Never>,
}

impl EngineRecoverLoop {
    pub async fn run_loop(self, mut task_tracker: TaskTracker, mut net: EngineNetwork) {
        let mut top_known_anchor_recv = self.bind.top_known_anchor.receiver();
        let mut engine_start_tka;
        let mut fix_history_flag = FixHistoryFlag(false);

        loop {
            let engine = Engine::new(
                net,
                &task_tracker,
                &self.bind,
                &self.merged_conf,
                fix_history_flag,
            );

            // prevent restart-loop: Engine will fail the same way until TKA changes
            engine_start_tka = top_known_anchor_recv.get();

            let engine_run = task_tracker.ctx().spawn(async move {
                match engine.run().await {
                    Err(EngineError::Cancelled) => Err(Cancelled()),
                    Err(EngineError::HistoryConflict(e)) => Ok(e),
                }
            });

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

            task_tracker.stop().await;
            drop(task_tracker);

            let history_conflict = match never_ok {
                Err(Cancelled()) => return,
                Ok(history_conflict) => history_conflict,
            };

            // prevent restart-loop: Engine will fail the same way until TKA changes
            let mut current_tka = top_known_anchor_recv.get();
            if engine_start_tka == current_tka {
                tracing::warn!(
                    peer_id = %self.net_args.network.peer_id().alt(),
                    overlay_id = %self.merged_conf.overlay_id,
                    top_known_anchor = current_tka.0,
                    err = %history_conflict,
                    "mempool failed twice at the same top known anchor; will retry when it changes"
                );

                'new_tka: loop {
                    tokio::select! {
                        next_tka = top_known_anchor_recv.next() => match next_tka {
                            Ok(top_known_anchor) => {
                                if current_tka != top_known_anchor {
                                    current_tka = top_known_anchor;
                                    break 'new_tka;
                                }
                            }
                            Err(Cancelled()) => return,
                        },
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {
                            if self.run_attrs.lock().is_stopping {
                                return;
                            }
                        },
                    }
                }
            }

            fix_history_flag = FixHistoryFlag(true);
            (task_tracker, net) = {
                let mut guard = self.run_attrs.lock();
                if guard.is_stopping {
                    return; // do not update task tracker
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
        }
    }
}
