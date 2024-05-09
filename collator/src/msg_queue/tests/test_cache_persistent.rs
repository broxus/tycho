use super::super::config::tests::init_test_config;

#[test]
fn test_persistent_cache_init() {
    use super::{PersistentCacheService, PersistentCacheServiceStubImpl};

    let cfg = init_test_config();

    let p_cache_impl =
        PersistentCacheServiceStubImpl::new(cfg.persistent_cache_config_ref()).unwrap();

    println!("persistent_cache_impl.config: {:?}", p_cache_impl.config);

    assert_eq!(p_cache_impl.config.cfg_value1.as_str(), "test_value_1");

    let p_cache_dyn: &dyn PersistentCacheService = &p_cache_impl;

    println!("persistent_cache_dyn: {:?}", p_cache_dyn);
}
