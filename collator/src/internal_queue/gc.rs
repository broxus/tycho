use std::sync::{Arc, Mutex};

use ahash::HashMapExt;
use everscale_types::models::ShardIdent;
use tokio::task::AbortHandle;
use tokio::time::Duration;
use tycho_block_util::queue::{QueueKey, QueuePartitionIdx};
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashMap;

use crate::internal_queue::state::commited_state::CommittedState;
use crate::internal_queue::types::{InternalMessageValue, QueueShardRange};
use crate::tracing_targets;

pub struct GcManager {
    pub delete_until: Arc<Mutex<GcRange>>,
    pub abort_handle: AbortHandle,
}

impl GcManager {
    pub fn start<V: InternalMessageValue>(
        committed_state: Arc<dyn CommittedState<V>>,
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
                    tokio::task::spawn_blocking({
                        let gc_state = gc_state.clone();
                        let committed_state = committed_state.clone();
                        move || {
                            gc_task(gc_state, committed_state, delete_until);
                        }
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

    pub fn update_delete_until(
        &self,
        partitions: QueuePartitionIdx,
        shard: ShardIdent,
        end_key: QueueKey,
    ) {
        self.delete_until
            .lock()
            .unwrap()
            .entry(partitions)
            .or_default()
            .insert(shard, end_key);
    }
}

impl Drop for GcManager {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

fn gc_task<V: InternalMessageValue>(
    gc_state: Arc<Mutex<GcRange>>,
    committed_state: Arc<dyn CommittedState<V>>,
    delete_until: GcRange,
) {
    let _histogram = HistogramGuard::begin("tycho_internal_queue_gc_execute_task_time");

    let mut gc_state = gc_state.lock().unwrap();
    for (partition, delete_until) in &delete_until {
        for (shard, current_last_key) in delete_until.iter() {
            let can_delete = gc_state
                .get(partition)
                .unwrap_or(&FastHashMap::default())
                .get(shard)
                .map_or(true, |last_key| *current_last_key > *last_key);

            if can_delete {
                let range = vec![QueueShardRange {
                    shard_ident: *shard,
                    from: QueueKey::default(),
                    to: *current_last_key,
                }];

                if let Err(e) = committed_state.delete(*partition, range.as_slice()) {
                    tracing::error!(target: tracing_targets::MQ, "failed to delete messages: {e:?}");
                }

                let labels = [("workchain", shard.workchain().to_string())];
                metrics::gauge!("tycho_internal_queue_processed_upto", &labels)
                    .set(current_last_key.lt as f64);

                gc_state
                    .entry(*partition)
                    .or_default()
                    .insert(*shard, *current_last_key);
            }
        }
    }

    // the total number of entries in the GC state
    let total_entries = gc_state.values().map(|map| map.len()).sum::<usize>();
    metrics::gauge!("tycho_internal_queue_gc_state_size").set(total_entries as f64);
}

type GcRange = FastHashMap<QueuePartitionIdx, FastHashMap<ShardIdent, QueueKey>>;
