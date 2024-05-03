use std::time::Duration;

#[derive(Clone)]
pub struct ValidatorConfig {
    pub base_loop_delay: Duration,
    pub max_loop_delay: Duration,
}
