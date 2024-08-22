use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use everscale_types::models::ShardIdent;
use tokio::time::Duration;
use tycho_block_util::queue::QueueKey;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;

use crate::internal_queue::state::persistent_state::PersistentState;
use crate::internal_queue::types::InternalMessageValue;
use crate::tracing_targets;

pub struct GCManager {
    pub delete_until: Arc<Mutex<HashMap<ShardIdent, QueueKey>>>,
    pub handle: JoinTask<()>,
}

impl GCManager {
    pub fn create_and_run<V: InternalMessageValue>(
        persistent_state: Arc<dyn PersistentState<V> + Send + Sync>,
        execution_interval: Duration,
    ) -> Self {
        let delete_until = Arc::new(Mutex::new(HashMap::<ShardIdent, QueueKey>::new()));
        let delete_until_cloned = delete_until.clone();

        let handle = JoinTask::new(async move {
            let current_gc_state = Arc::new(Mutex::new(HashMap::<ShardIdent, QueueKey>::new()));

            loop {
                tokio::time::sleep(execution_interval).await;
                let delete_until = {
                    let delete_until = delete_until_cloned.lock().unwrap();
                    delete_until.clone()
                };

                let current_gc_state_cloned = current_gc_state.clone();
                let persistent_state_cloned = persistent_state.clone();

                tokio::task::spawn_blocking(move || {
                    let histogram =
                        HistogramGuard::begin("tycho_internal_queue_gc_execute_task_time");
                    gc_task(
                        current_gc_state_cloned,
                        persistent_state_cloned,
                        delete_until,
                    );
                    histogram.finish();
                })
                .await
                .unwrap();
            }
        });

        GCManager {
            delete_until,
            handle,
        }
    }

    pub fn update_delete_until(&self, shard: ShardIdent, end_key: QueueKey) {
        self.delete_until.lock().unwrap().insert(shard, end_key);
    }
}

fn gc_task<V: InternalMessageValue>(
    current_gc_state: Arc<Mutex<HashMap<ShardIdent, QueueKey>>>,
    persistent_state: Arc<dyn PersistentState<V> + Send + Sync>,
    delete_until: HashMap<ShardIdent, QueueKey>,
) {
    let mut current_gc_state_lock = current_gc_state.lock().unwrap();

    for (shard, current_last_key) in delete_until.iter() {
        let can_delete = current_gc_state_lock
            .get(shard)
            .map_or(true, |last_key| *current_last_key > *last_key);

        if can_delete {
            if let Err(err) = persistent_state.delete_messages(*shard, current_last_key) {
                tracing::error!(target: tracing_targets::MQ, ?err, "Failed to delete messages");
            }

            let labels = [("shard", shard.to_string())];
            metrics::gauge!("tycho_internal_queue_processed_upto", &labels)
                .set(current_last_key.lt as f64);

            current_gc_state_lock.insert(*shard, *current_last_key);
        }
    }
}
