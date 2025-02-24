use tokio::sync::oneshot;

use crate::engine::lifecycle::EngineNetwork;
use crate::engine::{Engine, MempoolMergedConfig};
use crate::prelude::{EngineBinding, EngineHandle, EngineNetworkArgs, EngineRunning};

pub struct EngineCreated {
    pub(super) handle: EngineHandle,
    pub(super) engine: Engine,
}

impl EngineCreated {
    /// some operations on [`EngineHandle`] must be applied when engine is created before it is run
    pub fn new(
        bind: EngineBinding,
        net_args: &EngineNetworkArgs,
        merged_conf: &MempoolMergedConfig,
    ) -> Self {
        let net = EngineNetwork::new(net_args, merged_conf);

        let handle = EngineHandle {
            bind,
            net,
            merged_conf: merged_conf.clone(),
        };
        let engine = Engine::new(&handle);
        Self { handle, engine }
    }

    pub fn handle(&self) -> &EngineHandle {
        &self.handle
    }

    pub fn run(self, engine_stop_tx: oneshot::Sender<()>) -> EngineRunning {
        EngineRunning::new(self, engine_stop_tx)
    }
}
