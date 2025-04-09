use std::sync::Arc;

use scopeguard::defer;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tycho_storage::Storage;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ManualCompactionTrigger {
    /// Trigger compaction for `BaseDb`.
    Base,
    /// Trigger compaction for `MempoolDb`.
    Mempool,
    /// Trigger compaction for `RpcDb`.
    Rpc,
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ManualCompaction {
    inner: Arc<Inner>,
}

impl ManualCompaction {
    pub fn new(storage: Storage) -> Self {
        let (compaction_trigger, manual_compaction_rx) =
            watch::channel(None::<ManualCompactionTrigger>);

        let watcher = tokio::spawn(Self::watcher(manual_compaction_rx, storage.clone()));

        Self {
            inner: Arc::new(Inner {
                trigger: compaction_trigger,
                handle: watcher.abort_handle(),
            }),
        }
    }

    pub fn trigger_compaction(&self, trigger: ManualCompactionTrigger) {
        self.inner.trigger.send_replace(Some(trigger));
    }

    #[tracing::instrument(skip_all)]
    async fn watcher(mut manual_rx: ManualTriggerRx, storage: Storage) {
        tracing::info!("manager started");
        defer! {
            tracing::info!("manager stopped");
        }

        loop {
            if manual_rx.changed().await.is_err() {
                break;
            }

            let Some(trigger) = *manual_rx.borrow_and_update() else {
                continue;
            };

            match trigger {
                ManualCompactionTrigger::Base => {
                    storage.base_db().trigger_compaction().await;
                }
                ManualCompactionTrigger::Mempool => {
                    storage.mempool_db().trigger_compaction().await;
                }
                ManualCompactionTrigger::Rpc => {
                    if let Some(rpc_db) = storage.rpc_db() {
                        rpc_db.trigger_compaction().await;
                    }
                }
            }
        }
    }
}

struct Inner {
    trigger: ManualTriggerTx,
    handle: AbortHandle,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

type ManualTriggerTx = watch::Sender<Option<ManualCompactionTrigger>>;
type ManualTriggerRx = watch::Receiver<Option<ManualCompactionTrigger>>;
