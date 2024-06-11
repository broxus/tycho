use std::sync::Arc;
use std::time::Duration;

use futures_util::Future;

pub fn spawn_metrics_loop<T, F, FR>(context: &Arc<T>, interval: Duration, f: F)
where
    T: Send + Sync + 'static,
    F: Fn(Arc<T>) -> FR + Send + Sync + 'static,
    FR: Future<Output = ()> + Send + Sync + 'static,
{
    let context = Arc::downgrade(context);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        loop {
            interval.tick().await;
            if let Some(context) = context.upgrade() {
                f(context).await;
            } else {
                break;
            }
        }
    });
}
