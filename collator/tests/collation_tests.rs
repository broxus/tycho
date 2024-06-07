use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::models::{BlockId, GlobalCapability, ShardIdent};
use futures_util::future::BoxFuture;
use tycho_block_util::state::MinRefMcStateTracker;
use tycho_collator::collator::CollatorStdImplFactory;
use tycho_collator::internal_queue::queue::{QueueConfig, QueueFactory, QueueFactoryStdImpl};
use tycho_collator::internal_queue::state::persistent::persistent_state::{
    PersistentStateConfig, PersistentStateImplFactory,
};
use tycho_collator::internal_queue::state::session::session_state::SessionStateImplFactory;
use tycho_collator::manager::CollationManager;
use tycho_collator::mempool::MempoolAdapterStubImpl;
use tycho_collator::queue_adapter::MessageQueueAdapterStdImpl;
use tycho_collator::state_node::{StateNodeAdapter, StateNodeAdapterStdImpl};
use tycho_collator::test_utils::{prepare_test_storage, try_init_test_tracing};
use tycho_collator::types::CollationConfig;
use tycho_collator::validator::client::retry::BackoffConfig;
use tycho_collator::validator::config::ValidatorConfig;
use tycho_collator::validator::validator::ValidatorStdImplFactory;
use tycho_core::block_strider::{
    BlockProvider, BlockStrider, EmptyBlockProvider, OptionalBlockStuff,
    PersistentBlockStriderState, PrintSubscriber, StateSubscriber, StateSubscriberContext,
};

#[derive(Clone)]
struct StrangeBlockProvider {
    adapter: Arc<dyn StateNodeAdapter>,
}

impl BlockProvider for StrangeBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        tracing::info!("Get next block: {:?}", prev_block_id);
        self.adapter.wait_for_block(prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockId) -> Self::GetBlockFut<'a> {
        tracing::info!("Get block: {:?}", block_id);
        self.adapter.wait_for_block(block_id)
    }
}

impl StateSubscriber for StrangeBlockProvider {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        self.adapter.handle_state(&cx.state)
    }
}

/// run: `RUST_BACKTRACE=1 cargo test -p tycho-collator --features test --test collation_tests -- --nocapture`
#[tokio::test]
async fn test_collation_process_on_stubs() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);
    tycho_util::test::init_logger("test_collation_process_on_stubs", "debug");

    let storage = prepare_test_storage().await.unwrap();

    let zerostate_id = BlockId::default();

    let block_strider = BlockStrider::builder()
        .with_provider(EmptyBlockProvider)
        .with_state(PersistentBlockStriderState::new(
            zerostate_id,
            storage.clone(),
        ))
        .with_state_subscriber(
            MinRefMcStateTracker::default(),
            storage.clone(),
            PrintSubscriber,
        )
        .build();

    block_strider.run().await.unwrap();

    let mut rnd = rand::thread_rng();
    let node_1_keypair = Arc::new(everscale_crypto::ed25519::KeyPair::generate(&mut rnd));

    let config = CollationConfig {
        key_pair: node_1_keypair.clone(),
        mc_block_min_interval_ms: 10000,
        max_uncommitted_chain_length: 32,
        uncommitted_chain_to_import_next_anchor: 8,
        max_mc_block_delta_from_bc_to_await_own: 2,
        supported_block_version: 50,
        supported_capabilities: supported_capabilities(),
        max_collate_threads: 1,

        #[cfg(feature = "test")]
        test_validators_keypairs: vec![
            node_1_keypair,
            // Arc::new(everscale_crypto::ed25519::KeyPair::generate(&mut rnd)),
        ],
    };

    tracing::info!("Trying to start CollationManager");

    let node_network = tycho_collator::test_utils::create_node_network();
    let validator_config = ValidatorConfig {
        request_signatures_backoff_config: BackoffConfig {
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(150),
            factor: 2.0,
            max_times: 999999999,
        },
        error_backoff_config: BackoffConfig {
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
            factor: 2.0,
            max_times: 999999999,
        },
        request_timeout: Duration::from_millis(1000),
        delay_between_requests: Duration::from_millis(50),
    };

    let queue_config = QueueConfig {
        persistent_state_config: PersistentStateConfig {
            storage: storage.clone(),
        },
    };

    let shards = vec![ShardIdent::default()];
    let session_state_factory = SessionStateImplFactory::new(shards);
    let persistent_state_factory =
        PersistentStateImplFactory::new(queue_config.persistent_state_config.storage);

    let queue_factory = QueueFactoryStdImpl {
        session_state_factory,
        persistent_state_factory,
    };
    let queue = queue_factory.create();
    let message_queue_adapter = MessageQueueAdapterStdImpl::new(queue);

    let manager = CollationManager::start(
        config,
        Arc::new(message_queue_adapter),
        |listener| StateNodeAdapterStdImpl::new(listener, storage.clone()),
        |listener| MempoolAdapterStubImpl::new(listener),
        ValidatorStdImplFactory {
            network: node_network.clone().into(),
            config: validator_config,
        },
        CollatorStdImplFactory,
    );

    let state_node_adapter = StrangeBlockProvider {
        adapter: manager.state_node_adapter().clone(),
    };

    let block_strider = BlockStrider::builder()
        .with_provider(state_node_adapter.clone())
        .with_state(PersistentBlockStriderState::new(
            zerostate_id,
            storage.clone(),
        ))
        .with_state_subscriber(
            MinRefMcStateTracker::default(),
            storage.clone(),
            state_node_adapter,
        )
        .build();

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
