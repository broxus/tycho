use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures_util::future::{self, BoxFuture};
use futures_util::lock::Mutex;
use tycho_block_util::block::BlockIdRelation;
use tycho_block_util::state::ShardStateStuff;
use tycho_collator::collator::CollatorStdImplFactory;
use tycho_collator::internal_queue::types::message::EnqueuedMessage;
use tycho_collator::manager::CollationManager;
use tycho_collator::mempool::{DumpedAnchor, MempoolAdapterStubImpl};
use tycho_collator::queue_adapter::MessageQueueAdapter;
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
use tycho_core::storage::CoreStorage;
use tycho_crypto::ed25519;
use tycho_storage::StorageContext;
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
    engine_stop_tx: tokio::sync::mpsc::Sender<Vec<BlockIdShort>>,
    block_id_to_handle: BlockIdShort,
    observed: Mutex<Vec<BlockIdShort>>,
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

impl StateSubscriber for CollatorStateSubscriber {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        Box::pin(async move {
            let mut observed = self.observed.lock().await;
            observed.push(cx.block.id().as_short_id());

            if cx.block.id().as_short_id() == self.block_id_to_handle {
                let _ = self.engine_stop_tx.send(observed.clone()).await;
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

type CollationManagerHandle =
    tycho_collator::manager::RunningCollationManager<CollatorStdImplFactory, ValidatorStub>;

#[allow(unused)]
struct DumpCollationContext {
    storage: CoreStorage,
    mq_adapter: Arc<dyn MessageQueueAdapter<EnqueuedMessage>>,
    mc_block_id: BlockId,
    mc_state: ShardStateStuff,
    zerostate_id: ZerostateId,
    keypair: Arc<ed25519::KeyPair>,
    config: CollatorConfig,
    mc_data: Arc<McData>,
    temp_dir: tempfile::TempDir,
}

fn default_collator_config() -> CollatorConfig {
    CollatorConfig {
        supported_block_version: 100,
        supported_capabilities: supported_capabilities(),
        min_mc_block_delta_from_bc_to_sync: 3,
        check_value_flow: false,
        validate_config: true,
        fast_sync: false,
        accounts_split_depth: 4,
        merkle_split_depth: 5,
        merkle_chain_limit: 5,
    }
}

async fn load_dump_collation_context(
    dump_path: &Path,
) -> Result<(DumpCollationContext, Vec<DumpedAnchor>)> {
    let (ctx, temp_dir) = StorageContext::new_temp().await?;

    let LoadedInfoFromDump {
        storage,
        mq_adapter,
        mc_block_id,
        dumped_anchors,
    } = load_info_from_dump::<EnqueuedMessage>(dump_path, ctx).await?;

    let zerostate_id = ZerostateId {
        seqno: mc_block_id.seqno,
        root_hash: mc_block_id.root_hash,
        file_hash: mc_block_id.file_hash,
    };

    let mc_state = storage
        .shard_state_storage()
        .load_state(mc_block_id.seqno, &mc_block_id)
        .await?;

    let node_keys_path = dump_path.join("keys.json");
    let node_keys = NodeKeys::from_file(node_keys_path).unwrap_or(NodeKeys::generate());

    let keypair = Arc::new(ed25519::KeyPair::from(&node_keys.as_secret()));
    let config = default_collator_config();
    let mc_data = McData::load_from_state(&mc_state, Default::default())?;

    Ok((
        DumpCollationContext {
            storage,
            mq_adapter,
            mc_block_id,
            mc_state,
            zerostate_id,
            keypair,
            config,
            mc_data,
            temp_dir,
        },
        dumped_anchors,
    ))
}

fn start_collation_manager(
    ctx: &DumpCollationContext,
    dumped_anchors: Vec<DumpedAnchor>,
) -> CollationManagerHandle {
    let validator = ValidatorStub {};

    CollationManager::start(
        ctx.keypair.clone(),
        ctx.config.clone(),
        ctx.mq_adapter.clone(),
        |listener| {
            StateNodeAdapterStdImpl::new(
                listener,
                ctx.storage.clone(),
                CollatorSyncContext::Historical,
                ctx.zerostate_id,
            )
        },
        |listener| {
            MempoolAdapterStubImpl::with_anchors_from_dump(
                listener,
                Some(ctx.mc_data.gen_chain_time),
                dumped_anchors,
            )
            .unwrap()
        },
        validator,
        CollatorStdImplFactory {
            wu_tuner_event_sender: None,
        },
        None,
    )
}

fn build_block_strider<S>(
    ctx: &DumpCollationContext,
    adapter: Arc<dyn StateNodeAdapter>,
    subscriber: S,
) -> impl std::future::Future<Output = Result<()>>
where
    S: StateSubscriber + Send + Sync + 'static,
{
    let collator_block_provider = CollatorBlockProvider {
        adapter: adapter.clone(),
    };

    let block_strider = BlockStrider::builder()
        .with_provider(
            SetSyncContext {
                adapter: adapter.clone(),
                ctx: CollatorSyncContext::Historical,
            }
            .chain(SetSyncContext {
                adapter: adapter.clone(),
                ctx: CollatorSyncContext::Recent,
            })
            .chain(collator_block_provider),
        )
        .with_state(PersistentBlockStriderState::new(
            ctx.zerostate_id.as_block_id(),
            ctx.storage.clone(),
        ))
        .with_block_subscriber(ShardStateApplier::new(ctx.storage.clone(), subscriber))
        .build();

    block_strider.run()
}

fn get_top_shard_block_id_seqno(mc_state: &ShardStateStuff, shard_id: ShardIdent) -> Result<u32> {
    for item in mc_state.shards()?.latest_blocks() {
        let block_id = item?;
        if block_id.shard == shard_id {
            return Ok(block_id.seqno);
        }
    }

    anyhow::bail!("No shard blocks found in master state")
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
    let (ctx, dumped_anchors) = load_dump_collation_context(dump_path).await.unwrap();

    tracing::info!("Trying to start CollationManager");

    let (engine_stop_tx, mut engine_stop_rx) = tokio::sync::mpsc::channel(1);
    let manager = start_collation_manager(&ctx, dumped_anchors);

    let collator = CollatorStateSubscriber {
        adapter: manager.state_node_adapter().clone(),
        engine_stop_tx,
        block_id_to_handle,
        observed: Default::default(),
    };
    let mc_state = ctx.mc_state.clone();
    collator
        .adapter
        .handle_state(mc_state.block_id(), &mc_state)
        .await
        .unwrap();

    // NOTE: Make sure to drop the state after handling it
    drop(mc_state);

    let block_strider = build_block_strider(&ctx, manager.state_node_adapter().clone(), collator);

    tokio::select! {
        _ = block_strider => {
            tracing::info!("Block strider finished");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_one_shard_block_per_master_block() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);
    tycho_util::test::init_logger("test_one_shard_block_per_master_block", "debug");

    let dump_path = Path::new("../test/data/dump_one_shard/");

    let (ctx, dumped_anchors) = load_dump_collation_context(dump_path).await.unwrap();

    let top_shard_block_id_seqno =
        get_top_shard_block_id_seqno(&ctx.mc_state, ShardIdent::BASECHAIN).unwrap();
    let expected_shard_block_1 = BlockIdShort {
        shard: ShardIdent::BASECHAIN,
        seqno: top_shard_block_id_seqno + 1,
    };
    let expected_master_block_1 = BlockIdShort {
        shard: ShardIdent::MASTERCHAIN,
        seqno: ctx.mc_block_id.seqno + 1,
    };
    let expected_shard_block_2 = BlockIdShort {
        shard: ShardIdent::BASECHAIN,
        seqno: top_shard_block_id_seqno + 2,
    };

    let (engine_stop_tx, mut engine_stop_rx) = tokio::sync::mpsc::channel(1);

    let manager = start_collation_manager(&ctx, dumped_anchors);

    let expected_sequence = vec![
        expected_shard_block_1,
        expected_master_block_1,
        expected_shard_block_2,
    ];

    let collator = CollatorStateSubscriber {
        adapter: manager.state_node_adapter().clone(),
        engine_stop_tx,
        block_id_to_handle: expected_shard_block_2,
        observed: Mutex::new(Vec::new()),
    };
    let mc_state = ctx.mc_state.clone();
    collator
        .adapter
        .handle_state(mc_state.block_id(), &mc_state)
        .await
        .unwrap();

    // NOTE: Make sure to drop the state after handling it
    drop(mc_state);

    let block_strider = build_block_strider(&ctx, manager.state_node_adapter().clone(), collator);

    tokio::select! {
        _ = block_strider => {
            tracing::info!("Block strider finished");
        },
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received, shutting down the test");
        },
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
            tracing::info!("Test timeout elapsed");
        },
        observed = engine_stop_rx.recv() => {
            tracing::info!("Stopped, found enough blocks to check");
            tracing::info!("Expected sequence: {:?}", expected_sequence);
            let observed = observed.unwrap();
            tracing::info!("Observed sequence: {:?}", observed);
            assert_eq!(
                observed, expected_sequence,
                "Expected shard/master ordering to be one shard block per master block",
            );
            tracing::info!("Expected and observed blocks match");

        }
    };
}
