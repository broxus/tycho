use anyhow::Result;

use crate::{
    collator::CollatorStdImpl,
    manager::{self, CollationManager, CollationManagerGenImpl},
    mempool::MempoolAdapterStdImpl,
    msg_queue::{MessageQueueAdapterStdImpl, QueueImpl},
    state_node::{
        StateNodeAdapterBuilder, StateNodeAdapterBuilderStdImpl, StateNodeAdapterStdImpl,
    },
    types::CollationConfig,
    validator::{validator_processor::ValidatorProcessorStdImpl, ValidatorStdImpl},
};

#[test]
fn test_create_manager() -> Result<()> {
    type CollationManagerStdImplGenST<ST> = CollationManagerGenImpl<
        CollatorStdImpl,
        ValidatorStdImpl<ValidatorProcessorStdImpl<ST>, ST>,
        MessageQueueAdapterStdImpl<QueueImpl>,
        MempoolAdapterStdImpl,
        ST,
    >;
    type CollationManagerStdImpl = CollationManagerStdImplGenST<StateNodeAdapterStdImpl>;

    let config = CollationConfig {};
    let mpool_adapter = MempoolAdapterStdImpl {};
    let state_node_adapter_builder = StateNodeAdapterBuilderStdImpl::new();

    let _manager =
        CollationManagerStdImpl::create(config, mpool_adapter, state_node_adapter_builder);

    Ok(())
}
