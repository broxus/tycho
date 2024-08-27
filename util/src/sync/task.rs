/// Ensure that the runtime does not spend too much time without yielding.
pub async fn yield_on_complex(complex: bool) {
    if complex {
        tokio::task::yield_now().await;
    } else {
        tokio::task::consume_budget().await;
    }
}
