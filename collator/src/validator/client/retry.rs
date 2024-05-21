use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};

use crate::tracing_targets;

#[derive(Clone)]
pub struct BackoffConfig {
    pub min_delay: Duration,
    pub max_delay: Duration,
    pub factor: f32,
    pub max_times: usize,
}

pub struct RetryClient<T> {
    client: Arc<T>,
    retry_policy: ExponentialBuilder,
}

impl<T> RetryClient<T> {
    pub fn new(client: Arc<T>, config: BackoffConfig) -> Self {
        let retry_policy = ExponentialBuilder::default()
            .with_min_delay(config.min_delay)
            .with_max_delay(config.max_delay)
            .with_factor(config.factor)
            .with_max_times(config.max_times);
        RetryClient {
            client,
            retry_policy,
        }
    }

    pub async fn execute_with_retry<F, Fut, R>(&self, task: F) -> Result<R>
    where
        F: Fn(Arc<T>) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        (|| async {
            let response =  task(self.client.clone()).await;
            match response {
                Ok(response) => {
                    Ok(response)
                }
                Err(e) => {
                    tracing::warn!(target: tracing_targets::VALIDATOR, "Retry request with error: {:?}", e.to_string());
                    Err(e)
                }
            }
        })
            .retry(&self.retry_policy)
            .await
    }
}
