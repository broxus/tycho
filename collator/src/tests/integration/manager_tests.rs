use anyhow::Result;

use crate::validator::state::ValidationStateStdImpl;
use crate::{
    collator::{collator_processor::CollatorProcessorStdImpl, CollatorStdImpl},
    manager::{CollationManager, CollationManagerGenImpl},
    mempool::MempoolAdapterStdImpl,
    msg_queue::{MessageQueueAdapterStdImpl, QueueImpl},
    state_node::{
        StateNodeAdapterBuilder, StateNodeAdapterBuilderStdImpl, StateNodeAdapterStdImpl,
    },
    types::CollationConfig,
    validator::{validator_processor::ValidatorProcessorStdImpl, ValidatorStdImpl},
};

#[tokio::test]
async fn test_create_manager() -> Result<()> {
    type CollationManagerStdImplGenST<MQ, ST, VS> = CollationManagerGenImpl<
        CollatorStdImpl<CollatorProcessorStdImpl<MQ, ST>, MQ, ST>,
        ValidatorStdImpl<ValidatorProcessorStdImpl<ST, VS>, ST, VS>,
        MQ,
        MempoolAdapterStdImpl,
        ST,
    >;
    type CollationManagerStdImpl = CollationManagerStdImplGenST<
        MessageQueueAdapterStdImpl<QueueImpl>,
        StateNodeAdapterStdImpl,
        ValidationStateStdImpl,
    >;

    let config = CollationConfig {
        key_pair: everscale_crypto::ed25519::KeyPair::generate(&mut rand::thread_rng()),
        mc_block_min_interval_ms: 2000,
    };
    let mpool_adapter = MempoolAdapterStdImpl {};
    let state_node_adapter_builder = StateNodeAdapterBuilderStdImpl::new();

    // state_node_adapter_builder.
    let _manager =
        CollationManagerStdImpl::create(config, mpool_adapter, state_node_adapter_builder);

    Ok(())
}
