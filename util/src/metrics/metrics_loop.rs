use std::sync::Arc;
use std::time::Duration;
use futures_util::Future;
use tokio::task::AbortHandle;
pub fn spawn_metrics_loop<T, F, FR>(
    context: &Arc<T>,
    interval: Duration,
    f: F,
) -> AbortHandle
where
    T: Send + Sync + 'static,
    F: Fn(Arc<T>) -> FR + Send + Sync + 'static,
    FR: Future<Output = ()> + Send + Sync + 'static,
{
    let context = Arc::downgrade(context);
    tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                14u32,
            );
            let mut interval = tokio::time::interval(interval);
            loop {
                __guard.checkpoint(16u32);
                {
                    __guard.end_section(17u32);
                    let __result = interval.tick().await;
                    __guard.start_section(17u32);
                    __result
                };
                if let Some(context) = context.upgrade() {
                    {
                        __guard.end_section(19u32);
                        let __result = f(context).await;
                        __guard.start_section(19u32);
                        __result
                    };
                } else {
                    {
                        __guard.end_section(21u32);
                        __guard.start_section(21u32);
                        break;
                    };
                }
            }
        })
        .abort_handle()
}
