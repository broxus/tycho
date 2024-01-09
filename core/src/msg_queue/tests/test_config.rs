use super::MessageQueueConfig;

pub fn init_test_config() -> MessageQueueConfig {
    use super::super::cache_persistent::PersistentCacheConfigStubImpl;
    use super::MessageQueueBaseConfig;

    MessageQueueConfig::new(
        MessageQueueBaseConfig {},
        PersistentCacheConfigStubImpl {
            cfg_value1: "test_value_1".to_owned(),
        },
    )
}

#[test]
fn test_config_init() {
    use super::super::cache_persistent::PersistentCacheConfigStubImpl;

    let cfg = init_test_config();

    let p_cache_cfg = cfg.persistent_cache_config_ref().as_any();
    assert!(p_cache_cfg
        .downcast_ref::<PersistentCacheConfigStubImpl>()
        .is_some());
}
