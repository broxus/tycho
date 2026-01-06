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
use tycho_collator::test_utils::{LoadedInfoFromDump, load_info_from_dump, try_init_test_tracing};
use tycho_collator::types::{CollatorConfig, McData, supported_capabilities};
use tycho_collator::validator::{
    AddSession, ValidationComplete, ValidationSessionId, ValidationStatus, Validator,
};
use tycho_core::block_strider::{
    BlockProvider, BlockProviderExt, BlockStrider, OptionalBlockStuff, PersistentBlockStriderState,
    ShardStateApplier, StateSubscriber, StateSubscriberContext,
};
use tycho_core::global_config::ZerostateId;
use tycho_core::node::NodeKeys;
use tycho_crypto::ed25519;
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent};

mod common;

#[derive(Clone)]
struct ValidatorStub {}

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
        block_id: &BlockId,
    ) -> Result<ValidationStatus> {
        tracing::info!("Got block on validation {}", block_id);

        Ok(ValidationStatus::Complete(ValidationComplete {
            signatures: Default::default(),
            total_weight: 0,
        }))
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
    engine_stop_tx: tokio::sync::mpsc::Sender<()>,
    block_id_to_handle: BlockIdShort,
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
        Box::pin(async move {
            if cx.block.id().as_short_id() == self.block_id_to_handle {
                let _ = self.engine_stop_tx.send(()).await;
            }

            self.adapter.handle_state(&cx.mc_block_id, &cx.state).await
        })
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

/// run: `RUST_BACKTRACE=1 cargo test -p tycho-collator --features test --test collation_tests -- --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_collation_process_on_dump() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);
    tycho_util::test::init_logger("test_collation_process_on_dump", "debug");

    let dump_path = Path::new("../test/data/dump/"); // TODO: insert real dump
    let block_id_to_handle = BlockIdShort {
        // TODO: Fill with correct block seqno
        seqno: 58,
        shard: ShardIdent::MASTERCHAIN,
    };
    let (ctx, _temp_dir) = tycho_storage::StorageContext::new_temp().await.unwrap();

    let LoadedInfoFromDump {
        storage,
        mq_adapter,
        mc_block_id,
        dumped_anchors,
    } = load_info_from_dump(dump_path, ctx).await.unwrap();

    let zerostate_id = ZerostateId {
        seqno: mc_block_id.seqno,
        root_hash: mc_block_id.root_hash,
        file_hash: mc_block_id.file_hash,
    };

    let mc_state = storage
        .shard_state_storage()
        .load_state(mc_block_id.seqno, &mc_block_id)
        .await
        .unwrap();

    let node_keys_path = dump_path.join("keys.json");
    let node_keys = NodeKeys::from_file(node_keys_path).unwrap_or(NodeKeys::generate());

    let keypair = Arc::new(ed25519::KeyPair::from(&node_keys.as_secret()));

    let config = CollatorConfig {
        supported_block_version: 100,
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

    let mc_data = McData::load_from_state(&mc_state, Default::default()).unwrap();

    let (engine_stop_tx, mut engine_stop_rx) = tokio::sync::mpsc::channel(1);
    let validator = ValidatorStub {};

    let manager = CollationManager::start(
        keypair,
        config,
        mq_adapter,
        |listener| {
            StateNodeAdapterStdImpl::new(
                listener,
                storage.clone(),
                CollatorSyncContext::Historical,
                zerostate_id,
            )
        },
        |listener| {
            MempoolAdapterStubImpl::with_anchors_from_dump(
                listener,
                Some(mc_data.gen_chain_time),
                dumped_anchors,
            )
            .unwrap()
        },
        validator,
        CollatorStdImplFactory {
            wu_tuner_event_sender: None,
        },
        None,
    );

    let collator = CollatorStateSubscriber {
        adapter: manager.state_node_adapter().clone(),
        engine_stop_tx,
        block_id_to_handle,
    };
    collator
        .adapter
        .handle_state(mc_state.block_id(), &mc_state)
        .await
        .unwrap();

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
            zerostate_id.as_block_id(),
            storage.clone(),
        ))
        .with_block_subscriber(ShardStateApplier::new(storage.clone(), collator))
        .build();

    let strider_handle = block_strider.run();

    tokio::select! {
        _ = strider_handle => {
            tracing::info!("block_strider finished");
        },
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received, shutting down the test");
        },
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
            tracing::info!("Test timeout elapsed");
        }
        _ = engine_stop_rx.recv() => {
            tracing::info!("Stopped, found block: {}", block_id_to_handle);
        }
    }
}
