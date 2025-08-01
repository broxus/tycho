use std::sync::Arc;

use anyhow::Result;
use futures_util::future::BoxFuture;
use tycho_block_util::block::BlockIdRelation;
use tycho_collator::collator::CollatorStdImplFactory;
use tycho_collator::internal_queue::queue::{QueueFactory, QueueFactoryStdImpl};
use tycho_collator::internal_queue::state::storage::QueueStateImplFactory;
use tycho_collator::manager::CollationManager;
use tycho_collator::mempool::MempoolAdapterStubImpl;
use tycho_collator::queue_adapter::MessageQueueAdapterStdImpl;
use tycho_collator::state_node::{CollatorSyncContext, StateNodeAdapter, StateNodeAdapterStdImpl};
use tycho_collator::test_utils::{prepare_test_storage, try_init_test_tracing};
use tycho_collator::types::{CollatorConfig, supported_capabilities};
use tycho_collator::validator::ValidatorStdImpl;
use tycho_core::block_strider::{
    BlockProvider, BlockStrider, EmptyBlockProvider, OptionalBlockStuff,
    PersistentBlockStriderState, PrintSubscriber, StateSubscriber, StateSubscriberContext,
};
use tycho_crypto::ed25519;
use tycho_types::models::BlockId;

mod common;

#[derive(Clone)]
struct StrangeBlockProvider {
    adapter: Arc<dyn StateNodeAdapter>,
}

impl BlockProvider for StrangeBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = futures_util::future::Ready<Result<()>>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        tracing::info!("Get next block: {:?}", prev_block_id);
        self.adapter.wait_for_block(prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        tracing::info!("Get block: {:?}", block_id);
        self.adapter.wait_for_block(&block_id.block_id)
    }

    fn cleanup_until(&self, _mc_seqno: u32) -> Self::CleanupFut<'_> {
        futures_util::future::ready(Ok(()))
    }
}

impl StateSubscriber for StrangeBlockProvider {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        self.adapter.handle_state(&cx.state)
    }
}

/// run: `RUST_BACKTRACE=1 cargo test -p tycho-collator --features test --test collation_tests -- --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore] // TEMP: Ignoring until graceful shutdown is implemented properly
async fn test_collation_process_on_stubs() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);
    tycho_util::test::init_logger("test_collation_process_on_stubs", "debug");

    let (storage, _tmp_dir) = prepare_test_storage().await.unwrap();

    let zerostate_id = BlockId::default();

    let block_strider = BlockStrider::builder()
        .with_provider(EmptyBlockProvider)
        .with_state(PersistentBlockStriderState::new(
            zerostate_id,
            storage.clone(),
        ))
        .with_state_subscriber(storage.clone(), PrintSubscriber)
        .build();

    block_strider.run().await.unwrap();

    let node_1_secret = rand::random::<ed25519::SecretKey>();
    let node_1_keypair = Arc::new(ed25519::KeyPair::from(&node_1_secret));

    let validator_network = common::make_validator_network(&node_1_secret, &zerostate_id);

    let config = CollatorConfig {
        supported_block_version: 50,
        supported_capabilities: supported_capabilities(),
        min_mc_block_delta_from_bc_to_sync: 3,
        check_value_flow: false,
        validate_config: true,
        fast_sync: false,
        accounts_split_depth: 4,
        merkle_split_depth: 5,
    };

    tracing::info!("Trying to start CollationManager");

    let queue_state_factory = QueueStateImplFactory::new(storage.context().clone()).unwrap();

    let queue_factory = QueueFactoryStdImpl {
        state: queue_state_factory,
        config: Default::default(),
    };
    let queue = queue_factory.create().unwrap();
    let message_queue_adapter = MessageQueueAdapterStdImpl::new(queue);

    let now = tycho_util::time::now_millis();

    let manager = CollationManager::start(
        node_1_keypair.clone(),
        config,
        Arc::new(message_queue_adapter),
        |listener| {
            StateNodeAdapterStdImpl::new(listener, storage.clone(), CollatorSyncContext::Historical)
        },
        |listener| MempoolAdapterStubImpl::with_stub_externals(listener, Some(now)),
        ValidatorStdImpl::new(
            validator_network,
            node_1_keypair.clone(),
            Default::default(),
        ),
        CollatorStdImplFactory {
            wu_tuner_event_sender: None,
        },
        None,
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
        .with_state_subscriber(storage.clone(), state_node_adapter)
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
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(20)) => {
            println!();
            println!("Test timeout elapsed");
        }
    }
}
