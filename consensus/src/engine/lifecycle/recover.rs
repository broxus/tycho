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
    pub bind: EngineBinding,
    pub net_args: EngineNetworkArgs,
    pub merged_conf: MempoolMergedConfig,
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(run_loop)),
            file!(),
            41u32,
        );
        let mut engine_run = engine_run;
        loop {
            __guard.checkpoint(42u32);
            tracing::info!(
                peer_id = % self.net_args.network.peer_id().alt(), overlay_id = % self
                .merged_conf.overlay_id, genesis_info = ? self.merged_conf.genesis_info,
                conf = ? self.merged_conf.conf, "mempool run"
            );
            metrics::gauge!(
                "tycho_mempool_engine_run_count", "genesis_round" => self.merged_conf
                .conf.genesis_round.0.to_string()
            )
                .increment(1);
            let never_ok = {
                __guard.end_section(56u32);
                let __result = engine_run.await;
                __guard.start_section(56u32);
                __result
            };
            self.net_args
                .overlay_service
                .remove_private_overlay(&self.merged_conf.overlay_id);
            let task_tracker = {
                let guard = self.run_attrs.lock();
                guard.tracker.clone()
            };
            {
                __guard.end_section(67u32);
                let __result = task_tracker.stop().await;
                __guard.start_section(67u32);
                __result
            };
            drop(task_tracker);
            let fix_history = match never_ok {
                Err(Cancelled()) => {
                    __guard.end_section(71u32);
                    __guard.start_section(71u32);
                    break;
                }
                Ok(Err(HistoryConflict(_))) => FixHistoryFlag(true),
            };
            let (task_tracker, net) = {
                let mut guard = self.run_attrs.lock();
                if guard.is_stopping {
                    {
                        __guard.end_section(78u32);
                        __guard.start_section(78u32);
                        break;
                    };
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
            engine_run = task_tracker
                .ctx()
                .spawn(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        113u32,
                    );
                    match {
                        __guard.end_section(114u32);
                        let __result = engine.run().await;
                        __guard.start_section(114u32);
                        __result
                    } {
                        Err(EngineError::Cancelled) => Err(Cancelled()),
                        Err(EngineError::HistoryConflict(e)) => Ok(Err(e)),
                    }
                });
        }
    }
}
