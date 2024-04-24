use std::sync::Arc;

use anyhow::Result;

use everscale_types::{
    boc::Boc,
    cell::HashBytes,
    models::{BlockId, GlobalCapability, ShardStateUnsplit},
};
use futures_util::{future::BoxFuture, FutureExt};
use sha2::Digest;
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_collator::manager::CollationManager;
use tycho_collator::test_utils::prepare_test_storage;
use tycho_collator::{
    mempool::{MempoolAdapterBuilder, MempoolAdapterBuilderStdImpl, MempoolAdapterStdImpl},
    state_node::{StateNodeAdapterBuilder, StateNodeAdapterBuilderStdImpl},
    test_utils::try_init_test_tracing,
    types::CollationConfig,
    validator_test_impl::ValidatorProcessorTestImpl,
};
use tycho_core::block_strider::{
    prepare_state_apply, provider::BlockProvider, subscriber::test::PrintSubscriber, BlockStrider,
};
use tycho_core::block_strider::{
    provider::OptionalBlockStuff, test_provider::archive_provider::ArchiveProvider,
};
use tycho_storage::{BlockMetaData, Db, DbOptions, Storage};

#[tokio::test]
async fn test_collation_process_on_stubs() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let (provider, storage) = prepare_test_storage().await.unwrap();

    let block_strider = BlockStrider::builder()
        .with_provider(provider)
        .with_subscriber(PrintSubscriber)
        .with_state(storage.clone())
        .build_with_state_applier(MinRefMcStateTracker::default(), storage.clone());

    block_strider.run().await.unwrap();

    let mpool_adapter_builder = MempoolAdapterBuilderStdImpl::<MempoolAdapterStdImpl>::new();
    let state_node_adapter_builder = StateNodeAdapterBuilderStdImpl::new(storage.clone());

    let config = CollationConfig {
        key_pair: everscale_crypto::ed25519::KeyPair::generate(&mut rand::thread_rng()),
        mc_block_min_interval_ms: 10000,
        max_mc_block_delta_from_bc_to_await_own: 2,
        supported_block_version: 50,
        supported_capabilities: supported_capabilities(),
        max_collate_threads: 1,
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

    let state_node_adapter = _manager.get_state_node_adapter();

    let block_strider = BlockStrider::builder()
        .with_provider(state_node_adapter.clone())
        .with_subscriber(state_node_adapter)
        .with_state(storage.clone())
        .build_with_state_applier(MinRefMcStateTracker::default(), storage.clone());

    let strider_handle = block_strider.run();

    tokio::select! {
        _ = strider_handle => {
            println!();
            println!("block_strider finished");
        },
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

fn supported_capabilities() -> u64 {
    let caps = GlobalCapability::CapCreateStatsEnabled as u64
        | GlobalCapability::CapBounceMsgBody as u64
        | GlobalCapability::CapReportVersion as u64
        | GlobalCapability::CapShortDequeue as u64
        | GlobalCapability::CapRemp as u64
        | GlobalCapability::CapInitCodeHash as u64
        | GlobalCapability::CapOffHypercube as u64
        | GlobalCapability::CapFixTupleIndexBug as u64
        | GlobalCapability::CapFastStorageStat as u64
        | GlobalCapability::CapMyCode as u64
        | GlobalCapability::CapCopyleft as u64
        | GlobalCapability::CapFullBodyInBounced as u64
        | GlobalCapability::CapStorageFeeToTvm as u64
        | GlobalCapability::CapWorkchains as u64
        | GlobalCapability::CapStcontNewFormat as u64
        | GlobalCapability::CapFastStorageStatBugfix as u64
        | GlobalCapability::CapResolveMerkleCell as u64
        | GlobalCapability::CapFeeInGasUnits as u64
        | GlobalCapability::CapBounceAfterFailedAction as u64
        | GlobalCapability::CapSuspendedList as u64
        | GlobalCapability::CapsTvmBugfixes2022 as u64;
    caps
}
