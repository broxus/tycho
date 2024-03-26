use anyhow::Result;

use crate::{
    collator::{collator_processor::CollatorProcessorStdImpl, CollatorStdImpl},
    manager::{CollationManager, CollationManagerGenImpl},
    mempool::{MempoolAdapterBuilder, MempoolAdapterBuilderStdImpl, MempoolAdapterStdImpl},
    msg_queue::{MessageQueueAdapterStdImpl, QueueIteratorImpl},
    state_node::{
        StateNodeAdapterBuilder, StateNodeAdapterBuilderStdImpl, StateNodeAdapterStdImpl,
    },
    tests::try_init_test_tracing,
    types::CollationConfig,
    validator::{validator_processor::ValidatorProcessorStdImpl, ValidatorStdImpl},
};

#[tokio::test]
async fn test_collation_process_on_stubs() {
    try_init_test_tracing();

    type CollationManagerStdImplGenST<MQ, QI, MP, ST> = CollationManagerGenImpl<
        CollatorStdImpl<CollatorProcessorStdImpl<MQ, QI, MP, ST>, MQ, MP, ST>,
        ValidatorStdImpl<ValidatorProcessorStdImpl<ST>, ST>,
        MQ,
        MP,
        ST,
    >;
    type CollationManagerStdImpl = CollationManagerStdImplGenST<
        MessageQueueAdapterStdImpl,
        QueueIteratorImpl,
        MempoolAdapterStdImpl,
        StateNodeAdapterStdImpl,
    >;

    let config = CollationConfig {
        key_pair: everscale_crypto::ed25519::KeyPair::generate(&mut rand::thread_rng()),
        mc_block_min_interval_ms: 2000,
    };
    let mpool_adapter_builder = MempoolAdapterBuilderStdImpl::new();
    let state_node_adapter_builder = StateNodeAdapterBuilderStdImpl::new();

    tracing::info!("Trying to start CollationManager");

    let _manager =
        CollationManagerStdImpl::create(config, mpool_adapter_builder, state_node_adapter_builder);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!();
            println!("Ctrl-C received, shutting down the test");
        }
    }
}
