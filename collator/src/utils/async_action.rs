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
        async_action().await.map_err(|err| {
            tracing::warn!("Error executing async action: {}", action_descr);
            err
        })
    });
}
