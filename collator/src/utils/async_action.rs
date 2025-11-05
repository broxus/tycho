pub fn schedule_async_action<F, Fut>(
    delay: tokio::time::Duration,
    async_action: F,
    action_descr: String,
)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
{
    tokio::spawn(async move {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::async_block"),
            file!(),
            9u32,
        );
        {
            __guard.end_section(10u32);
            let __result = tokio::time::sleep(delay).await;
            __guard.start_section(10u32);
            __result
        };
        if let Err(e) = {
            __guard.end_section(11u32);
            let __result = async_action().await;
            __guard.start_section(11u32);
            __result
        } {
            tracing::warn!(action_descr, "Error executing async action: {e:?}");
        }
    });
}
