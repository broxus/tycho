use super::{super::config::tests::init_test_config, MessageQueue, MessageQueueImplOnStubs};

#[test]
fn test_queue_init() {
    let cfg = init_test_config();

    let queue = MessageQueueImplOnStubs::init(cfg).unwrap();
}
