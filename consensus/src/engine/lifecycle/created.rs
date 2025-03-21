use tokio::sync::oneshot;

use crate::effects::TaskTracker;
use crate::engine::lifecycle::{EngineNetwork, FixHistoryFlag};
use crate::engine::{Engine, MempoolMergedConfig};
use crate::intercom::InitPeers;
use crate::prelude::{EngineBinding, EngineHandle, EngineNetworkArgs, EngineRunning};

pub struct EngineCreated {
    pub(super) handle: EngineHandle,
    pub(super) engine: Engine,
    pub(super) engine_task_tracker: TaskTracker,
}

impl EngineCreated {
    /// some operations on [`EngineHandle`] must be applied when engine is created before it is run
    pub fn new(
        bind: EngineBinding,
        net_args: &EngineNetworkArgs,
        merged_conf: &MempoolMergedConfig,
        init_peers: &InitPeers,
    ) -> Self {
        let super_tracker = TaskTracker::default();
        let net = EngineNetwork::new(net_args, &super_tracker, merged_conf, init_peers);

        let handle = EngineHandle {
            super_tracker,
            bind,
            net,
            merged_conf: merged_conf.clone(),
        };
        let engine_task_tracker = TaskTracker::default();
        let engine = Engine::new(&handle, &engine_task_tracker, FixHistoryFlag::default());
        Self {
            handle,
            engine,
            engine_task_tracker,
        }
    }

    pub fn run(self, engine_stop_tx: oneshot::Sender<()>) -> EngineRunning {
        EngineRunning::new(self, engine_stop_tx)
    }
}
