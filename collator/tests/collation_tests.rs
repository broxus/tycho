use std::sync::Arc;
use tycho_block_util::state::MinRefMcStateTracker;
use tycho_collator::validator::validator_processor::ValidatorProcessor;
use tycho_collator::{
    mempool::{MempoolAdapterBuilder, MempoolAdapterBuilderStdImpl, MempoolAdapterStdImpl},
    state_node::{StateNodeAdapterBuilder, StateNodeAdapterBuilderStdImpl},
    test_utils::try_init_test_tracing,
    types::CollationConfig,
    validator_test_impl::ValidatorProcessorTestImpl,
};
use tycho_core::block_strider::subscriber::test::PrintSubscriber;
use tycho_core::block_strider::{prepare_state_apply, BlockStrider};
use tycho_storage::build_tmp_storage;

#[tokio::test]
async fn test_collation_process_on_stubs() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let (provider, storage) = prepare_state_apply().await.unwrap();

    let block_strider = BlockStrider::builder()
        .with_provider(provider)
        .with_subscriber(PrintSubscriber)
        .with_state(storage.clone())
        .build_with_state_applier(MinRefMcStateTracker::default(), storage.clone());

    block_strider.run().await.unwrap();

    let mpool_adapter_builder = MempoolAdapterBuilderStdImpl::<MempoolAdapterStdImpl>::new();
    let state_node_adapter_builder = StateNodeAdapterBuilderStdImpl::new(storage);

    let config = CollationConfig {
        key_pair: everscale_crypto::ed25519::KeyPair::generate(&mut rand::thread_rng()),
        mc_block_min_interval_ms: 10000,
        max_mc_block_delta_from_bc_to_await_own: 2,
    };

    tracing::info!("Trying to start CollationManager");

    let node_network = tycho_collator::test_utils::create_node_network();

    let _manager = tycho_collator::manager::create_std_manager_with_validator::<
        _,
        _,
        ValidatorProcessorTestImpl<_>,
    >(
        config,
        mpool_adapter_builder,
        state_node_adapter_builder,
        node_network,
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!();
            println!("Ctrl-C received, shutting down the test");
        },
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {
            println!();
            println!("Test timeout elapsed");
        }
    }
}
