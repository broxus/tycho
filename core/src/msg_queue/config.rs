use super::cache_persistent::PersistentCacheConfig;

#[cfg(test)]
#[path = "tests/test_config.rs"]
pub(super) mod tests;

pub struct MessageQueueBaseConfig {}

pub struct MessageQueueConfig {
    base_config: MessageQueueBaseConfig,
    persistent_cache_config: Box<dyn PersistentCacheConfig>,
}

impl MessageQueueConfig {
    pub fn new(
        base_config: MessageQueueBaseConfig,
        persistent_cache_config: impl PersistentCacheConfig + 'static,
    ) -> Self {
        MessageQueueConfig {
            base_config,
            persistent_cache_config: Box::new(persistent_cache_config),
        }
    }

    pub fn base_config(&self) -> &MessageQueueBaseConfig {
        &self.base_config
    }
    pub fn persistent_cache_config_ref(&self) -> &dyn PersistentCacheConfig {
        self.persistent_cache_config.as_ref()
    }
}
