use std::sync::Arc;
use std::time::Duration;
use backon::{BackoffBuilder, ExponentialBuilder};
use anyhow::{Result};
use backon::Retryable;

#[derive(Clone)]
pub struct BackoffConfig {
    pub min_delay: Duration,
    pub max_delay: Duration,
    pub factor: f32,
}

pub struct RetryClient<T> {
    client: Arc<T>,
    config: BackoffConfig,
}

impl<T> RetryClient<T> {
    pub fn new(client: Arc<T>, config: BackoffConfig) -> Self {
        RetryClient { client, config }
    }

    pub async fn execute_with_retry<F, Fut, R>(&self, task: F) -> Result<R>
        where
            F: Fn(Arc<T>) -> Fut,
            Fut: std::future::Future<Output = Result<R>>,
    {
        let retry_policy = ExponentialBuilder::default()
            .with_min_delay(self.config.min_delay)
            .with_max_delay(self.config.max_delay)
            .with_factor(self.config.factor);

        (|| async {
            task(self.client.clone()).await
        })
            .retry(&retry_policy)
            .await
    }
}
