use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures_util::future::{self, BoxFuture};
use tycho_block_util::block::BlockIdRelation;
use tycho_collator::collator::CollatorStdImplFactory;
use tycho_collator::manager::CollationManager;
use tycho_collator::mempool::MempoolAdapterStubImpl;
use tycho_collator::state_node::{CollatorSyncContext, StateNodeAdapter, StateNodeAdapterStdImpl};
use tycho_collator::test_utils::{load_storage_from_dump, try_init_test_tracing};
use tycho_collator::types::processed_upto::ProcessedUptoInfoExtension;
use tycho_collator::types::{CollatorConfig, McData, supported_capabilities};
use tycho_collator::validator::{AddSession, ValidationSessionId, ValidationStatus, Validator};
use tycho_core::block_strider::{
    BlockProvider, BlockProviderExt, BlockStrider, EmptyBlockProvider, OptionalBlockStuff,
    PersistentBlockStriderState, PrintSubscriber, ShardStateApplier, StateSubscriber,
    StateSubscriberContext,
};
use tycho_core::node::NodeKeys;
use tycho_crypto::ed25519;
use tycho_types::models::{BlockId, BlockIdShort};

mod common;

#[derive(Clone)]
struct ValidatorStub;

#[async_trait]
impl Validator for ValidatorStub {
    /// Adds a new session for the specified shard.
    fn add_session(&self, _info: AddSession<'_>) -> Result<()> {
        Ok(())
    }

    /// Collects signatures for the specified block.
    async fn validate(
        &self,
        _session_id: ValidationSessionId,
        _block_id: &BlockId,
    ) -> Result<ValidationStatus> {
        Ok(ValidationStatus::Skipped)
    }

    /// Cancels validation before the specified block.
    ///
    /// If `session_id` is provided, it will be used to directly cancel the specific session,
    /// avoiding unnecessary lookups and potential ambiguity.
    fn cancel_validation(
        &self,
        _before: &BlockIdShort,
        _session_id: Option<ValidationSessionId>,
    ) -> Result<()> {
        Ok(())
    }
}

struct CollatorStateSubscriber {
    adapter: Arc<dyn StateNodeAdapter>,
}

struct SetSyncContext {
    adapter: Arc<dyn StateNodeAdapter>,
    ctx: CollatorSyncContext,
}

impl BlockProvider for SetSyncContext {
    type GetNextBlockFut<'a> = futures_util::future::Ready<OptionalBlockStuff>;
    type GetBlockFut<'a> = futures_util::future::Ready<OptionalBlockStuff>;
    type CleanupFut<'a> = futures_util::future::Ready<Result<()>>;

    fn get_next_block<'a>(&'a self, _: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        self.adapter.set_sync_context(self.ctx);
        futures_util::future::ready(None)
    }

    fn get_block<'a>(&'a self, _: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        futures_util::future::ready(None)
    }

    fn cleanup_until(&self, _mc_seqno: u32) -> Self::CleanupFut<'_> {
        futures_util::future::ready(Ok(()))
    }
}

impl CollatorStateSubscriber {
    fn new_sync_point(&self, ctx: CollatorSyncContext) -> SetSyncContext {
        SetSyncContext {
            adapter: self.adapter.clone(),
            ctx,
        }
    }
}

impl StateSubscriber for CollatorStateSubscriber {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        self.adapter.handle_state(&cx.state)
    }
}

struct CollatorBlockProvider {
    adapter: Arc<dyn StateNodeAdapter>,
}

impl BlockProvider for CollatorBlockProvider {
    type GetNextBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type GetBlockFut<'a> = BoxFuture<'a, OptionalBlockStuff>;
    type CleanupFut<'a> = future::Ready<Result<()>>;

    fn get_next_block<'a>(&'a self, prev_block_id: &'a BlockId) -> Self::GetNextBlockFut<'a> {
        self.adapter.wait_for_block_next(prev_block_id)
    }

    fn get_block<'a>(&'a self, block_id_relation: &'a BlockIdRelation) -> Self::GetBlockFut<'a> {
        self.adapter.wait_for_block(&block_id_relation.block_id)
    }

    fn cleanup_until(&self, _mc_seqno: u32) -> Self::CleanupFut<'_> {
        futures_util::future::ready(Ok(()))
    }
}

/// run: `RUST_BACKTRACE=1 cargo test -p tycho-collator --features test --test dump_state_collation_tests -- --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_collation_process_on_dump() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);
    tycho_util::test::init_logger("test_collation_process_on_dump", "debug");

    let dump_path = Path::new("../test/data/dump/"); // TODO: insert real dump

    let (storage, mq_adapter, _temp_dir, mc_block_id) =
        load_storage_from_dump(dump_path).await.unwrap();

    let zerostate_id = mc_block_id;

    let block_strider = BlockStrider::builder()
        .with_provider(EmptyBlockProvider)
        .with_state(PersistentBlockStriderState::new(
            zerostate_id,
            storage.clone(),
        ))
        .with_state_subscriber(storage.clone(), PrintSubscriber)
        .build();

    block_strider.run().await.unwrap();

    let node_keys_path = Path::new("../test/data/dump_1_8000000000000000_373/keys.json");
    let node_keys = NodeKeys::from_file(node_keys_path).unwrap_or(NodeKeys::generate());

    let keypair = Arc::new(ed25519::KeyPair::from(&node_keys.as_secret()));

    let config = CollatorConfig {
        supported_block_version: 50,
        supported_capabilities: supported_capabilities(),
        min_mc_block_delta_from_bc_to_sync: 3,
        check_value_flow: false,
        validate_config: true,
        fast_sync: false,
        accounts_split_depth: 4,
        merkle_split_depth: 5,
        merkle_chain_limit: 5,
    };

    tracing::info!("Trying to start CollationManager");

    let mc_state = storage
        .shard_state_storage()
        .load_state(mc_block_id.seqno, &mc_block_id)
        .await
        .unwrap();

    let mc_data = McData::load_from_state(&mc_state, Default::default()).unwrap();

    let (top_processed_to_anchor_mc, _) = mc_data
        .processed_upto
        .get_min_externals_processed_to()
        .unwrap_or_default();

    let top_processed_to_anchor_shards = mc_data.top_processed_to_anchor;

    let manager = CollationManager::start(
        keypair,
        config,
        mq_adapter,
        |listener| {
            StateNodeAdapterStdImpl::new(listener, storage.clone(), CollatorSyncContext::Historical)
        },
        |listener| {
            MempoolAdapterStubImpl::with_anchors_from_dump(
                listener,
                Some(mc_data.gen_chain_time),
                top_processed_to_anchor_mc,
                top_processed_to_anchor_shards,
                dump_path.join("mempool"),
            )
            .unwrap()
        },
        ValidatorStub {},
        CollatorStdImplFactory {
            wu_tuner_event_sender: None,
        },
        None,
    );

    let collator = CollatorStateSubscriber {
        adapter: manager.state_node_adapter().clone(),
    };
    collator.adapter.handle_state(&mc_state).await.unwrap();

    // NOTE: Make sure to drop the state after handling it
    drop(mc_state);

    let collator_block_provider = CollatorBlockProvider {
        adapter: manager.state_node_adapter().clone(),
    };

    let block_strider = BlockStrider::builder()
        .with_provider(
            collator
                .new_sync_point(CollatorSyncContext::Historical)
                .chain(collator.new_sync_point(CollatorSyncContext::Recent))
                .chain(collator_block_provider),
        )
        .with_state(PersistentBlockStriderState::new(
            zerostate_id,
            storage.clone(),
        ))
        .with_block_subscriber(ShardStateApplier::new(storage.clone(), collator))
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
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
            println!();
            println!("Test timeout elapsed");
        }
    }
}
