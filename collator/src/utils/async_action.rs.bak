pub fn schedule_async_action<F, Fut>(
    delay: tokio::time::Duration,
    async_action: F,
    action_descr: String,
) where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
{
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        if let Err(e) = async_action().await {
            tracing::warn!(action_descr, "Error executing async action: {e:?}");
        }
    });
}
