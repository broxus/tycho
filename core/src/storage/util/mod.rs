use cassadilia::{Cas, CasStats, DbStats, IndexStats};

pub use self::slot_subscriptions::*;
pub use self::stored_value::*;

mod slot_subscriptions;
mod stored_value;

pub(crate) fn spawn_cas_metrics_loop<K>(db: &Cas<K>)
where
    K: Send + Sync + 'static,
{
    tycho_util::metrics::spawn_metrics_loop(
        db.as_arc(),
        std::time::Duration::from_secs(10),
        |db| async move {
            let DbStats {
                cas:
                    CasStats {
                        unique_blobs,
                        total_bytes,
                    },
                index: IndexStats {
                    serialized_size_bytes,
                },
            } = db.stats();

            let path = db.root_path().to_string_lossy().into_owned();

            metrics::gauge!("cas_unique_blobs", "path" => path.clone()).set(unique_blobs as f64);
            metrics::gauge!("cas_total_bytes", "path" => path.clone()).set(total_bytes as f64);
            metrics::gauge!("cas_index_size", "path" => path).set(serialized_size_bytes as f64);
        },
    );
}
