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
        .with_provider(state_node_adapter)
        .with_subscriber(PrintSubscriber)
        .with_state(storage.clone())
        .build_with_state_applier(MinRefMcStateTracker::default(), storage.clone());

    block_strider.run().await.unwrap();

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

struct DummyArchiveProvider;
impl BlockProvider for DummyArchiveProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        futures_util::future::ready(None).boxed()
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        futures_util::future::ready(None).boxed()
    }
}

async fn prepare_test_storage() -> Result<(DummyArchiveProvider, Arc<Storage>)> {
    let provider = DummyArchiveProvider;
    let temp = tempfile::tempdir().unwrap();
    let db = Db::open(temp.path().to_path_buf(), DbOptions::default()).unwrap();
    let storage = Storage::new(db, temp.path().join("file"), 1_000_000).unwrap();
    let tracker = MinRefMcStateTracker::default();

    // master state
    let master_bytes = include_bytes!("../src/state_node/tests/data/test_state_2_master.boc");
    let master_file_hash: HashBytes = sha2::Sha256::digest(master_bytes).into();
    let master_root = Boc::decode(master_bytes)?;
    let master_root_hash = *master_root.repr_hash();
    let master_state = master_root.parse::<ShardStateUnsplit>()?;

    let mc_state_extra = master_state.load_custom()?;
    let mc_state_extra = mc_state_extra.unwrap();
    let mut shard_info_opt = None;
    for shard_info in mc_state_extra.shards.iter() {
        shard_info_opt = Some(shard_info?);
        break;
    }
    let shard_info = shard_info_opt.unwrap();

    let master_id = BlockId {
        shard: master_state.shard_ident,
        seqno: master_state.seqno,
        root_hash: master_root_hash,
        file_hash: master_file_hash,
    };
    let master_state_stuff =
        ShardStateStuff::from_state_and_root(master_id, master_state, master_root, &tracker)?;

    let (handle, _) = storage.block_handle_storage().create_or_load_handle(
        &master_id,
        BlockMetaData {
            is_key_block: mc_state_extra.after_key_block,
            gen_utime: master_state_stuff.state().gen_utime,
            mc_ref_seqno: Some(0),
        },
    )?;

    storage
        .shard_state_storage()
        .store_state(&handle, &master_state_stuff)
        .await?;

    // shard state
    let shard_bytes = include_bytes!("../src/state_node/tests/data/test_state_2_0:80.boc");
    let shard_file_hash: HashBytes = sha2::Sha256::digest(shard_bytes).into();
    let shard_root = Boc::decode(shard_bytes)?;
    let shard_root_hash = *shard_root.repr_hash();
    let shard_state = shard_root.parse::<ShardStateUnsplit>()?;
    let shard_id = BlockId {
        shard: shard_info.0,
        seqno: shard_info.1.seqno,
        root_hash: shard_info.1.root_hash,
        file_hash: shard_info.1.file_hash,
    };
    let shard_state_stuff =
        ShardStateStuff::from_state_and_root(shard_id, shard_state, shard_root, &tracker)?;

    let (handle, _) = storage.block_handle_storage().create_or_load_handle(
        &shard_id,
        BlockMetaData {
            is_key_block: false,
            gen_utime: shard_state_stuff.state().gen_utime,
            mc_ref_seqno: Some(0),
        },
    )?;

    storage
        .shard_state_storage()
        .store_state(&handle, &shard_state_stuff)
        .await?;

    storage
        .node_state()
        .store_last_mc_block_id(&master_id)
        .unwrap();

    Ok((provider, storage))
}
