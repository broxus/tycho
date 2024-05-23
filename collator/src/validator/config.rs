use std::time::Duration;

use crate::validator::client::retry::BackoffConfig;

#[derive(Clone)]
pub struct ValidatorConfig {
    pub error_backoff_config: BackoffConfig,
    pub request_signatures_backoff_config: BackoffConfig,
    pub request_timeout: Duration,
    pub delay_between_requests: Duration,
}
