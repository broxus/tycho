use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use everscale_types::models::ShardIdent;
use tokio::task::AbortHandle;
use tokio::time::Duration;
use tycho_block_util::queue::QueueKey;
use tycho_util::metrics::HistogramGuard;

use crate::internal_queue::state::persistent_state::PersistentState;
use crate::internal_queue::types::InternalMessageValue;
use crate::tracing_targets;

pub struct GcManager {
    pub delete_until: Arc<Mutex<GcRange>>,
    pub abort_handle: AbortHandle,
}

impl GcManager {
    pub fn start<V: InternalMessageValue>(
        persistent_state: Arc<dyn PersistentState<V>>,
        execution_interval: Duration,
    ) -> Self {
        let delete_until = Arc::new(Mutex::new(GcRange::new()));

        let abort_handle = tokio::spawn({
            let delete_until = delete_until.clone();
            async move {
                let gc_state = Arc::new(Mutex::new(GcRange::new()));

                let mut interval = tokio::time::interval(execution_interval);
                loop {
                    interval.tick().await;

                    let delete_until = delete_until.lock().unwrap().clone();

                    let gc_state = gc_state.clone();
                    let persistent_state = persistent_state.clone();
                    tokio::task::spawn_blocking(move || {
                        gc_task(gc_state, persistent_state, delete_until);
                    })
                    .await
                    .unwrap();
                }
            }
        })
        .abort_handle();

        GcManager {
            delete_until,
            abort_handle,
        }
    }

    pub fn update_delete_until(&self, shard: ShardIdent, end_key: QueueKey) {
        self.delete_until.lock().unwrap().insert(shard, end_key);
    }
}

impl Drop for GcManager {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

fn gc_task<V: InternalMessageValue>(
    gc_state: Arc<Mutex<GcRange>>,
    persistent_state: Arc<dyn PersistentState<V> + Send + Sync>,
    delete_until: HashMap<ShardIdent, QueueKey>,
) {
    let _histogram = HistogramGuard::begin("tycho_internal_queue_gc_execute_task_time");

    let mut gc_state = gc_state.lock().unwrap();

    for (shard, current_last_key) in delete_until.iter() {
        let can_delete = gc_state
            .get(shard)
            .map_or(true, |last_key| *current_last_key > *last_key);

        if can_delete {
            if let Err(e) = persistent_state.delete_messages(*shard, current_last_key) {
                tracing::error!(target: tracing_targets::MQ, "failed to delete messages: {e:?}");
            }

            let labels = [("shard", shard.to_string())];
            metrics::gauge!("tycho_internal_queue_processed_upto", &labels)
                .set(current_last_key.lt as f64);

            gc_state.insert(*shard, *current_last_key);
        }
    }
}

type GcRange = HashMap<ShardIdent, QueueKey>;
