use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use everscale_crypto::ed25519;
use everscale_types::boc::{Boc, BocRepr};
use everscale_types::cell::HashBytes;
use everscale_types::models::{
    Block, BlockId, BlockIdShort, CollationConfig, GenesisInfo, ShardIdent, ShardStateUnsplit,
};
use futures_util::future::BoxFuture;
use serde::Deserialize;
use tokio::sync::Notify;
use tycho_block_util::archive::ArchiveData;
use tycho_block_util::block::{BlockIdRelation, BlockStuff, ValidatorSubsetInfo};
use tycho_block_util::queue::{QueueDiffStuff, QueueDiffStuffAug};
use tycho_block_util::state::ShardStateStuff;
use tycho_collator::collator::{
    get_anchors_processing_info, CollationCancelReason, CollatorContext, CollatorEventListener,
    CollatorStdImpl, CollatorStdImplFactory, ForceMasterCollation,
};
use tycho_collator::internal_queue::queue::{QueueFactory, QueueFactoryStdImpl};
use tycho_collator::internal_queue::state::storage::QueueStateImplFactory;
use tycho_collator::internal_queue::types::{
    DiffStatistics, EnqueuedMessage, QueueDiffWithMessages,
};
use tycho_collator::manager::utils::get_all_shards_processed_to_by_partitions_for_mc_block_without_cache;
use tycho_collator::manager::CollationManager;
use tycho_collator::mempool::{MempoolAdapterStubImpl, MempoolAnchor, MempoolEventListener};
use tycho_collator::queue_adapter::{MessageQueueAdapter, MessageQueueAdapterStdImpl};
use tycho_collator::state_node::{
    CollatorSyncContext, StateNodeAdapter, StateNodeAdapterStdImpl, StateNodeEventListener,
};
use tycho_collator::test_utils::{prepare_test_storage, try_init_test_tracing};
use tycho_collator::types::processed_upto::{ProcessedUptoInfoExtension, ProcessedUptoInfoStuff};
use tycho_collator::types::{
    supported_capabilities, BlockCollationResult, CollationSessionId, CollationSessionInfo,
    CollatorConfig, McData,
};
use tycho_collator::validator::ValidatorStdImpl;
use tycho_core::block_strider::{
    BlockProvider, BlockStrider, EmptyBlockProvider, OptionalBlockStuff,
    PersistentBlockStriderState, PrintSubscriber, StateSubscriber, StateSubscriberContext,
};
use tycho_core::global_config::MempoolGlobalConfig;
use tycho_storage::{NewBlockMeta, Storage, StorageConfig};
use tycho_util::serde_helpers::{load_json_from_file, write_json_to_file};

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

    let current_dir = std::env::current_dir().unwrap();
    let node_keys_path = current_dir.join("../keys.json");
    let node_keys: NodeKeys =
        tycho_util::serde_helpers::load_json_from_file(node_keys_path).unwrap();
    tracing::trace!("node_keys: {:?}", node_keys);

    let node_1_secret = ed25519::SecretKey::from_bytes(node_keys.secret.0);
    let node_1_keypair = Arc::new(ed25519::KeyPair::from(&node_1_secret));

    let validator_network = common::make_validator_network(&node_1_secret, &zerostate_id);

    let config = CollatorConfig {
        supported_block_version: 50,
        supported_capabilities: supported_capabilities(),
        min_mc_block_delta_from_bc_to_sync: 3,
        check_value_flow: true,
        validate_config: true,
        fast_sync: false,
    };

    tracing::info!("Trying to start CollationManager");

    let queue_state_factory = QueueStateImplFactory::new(storage.clone());

    let queue_factory = QueueFactoryStdImpl {
        state: queue_state_factory,
        config: Default::default(),
    };
    let queue = queue_factory.create();
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
        CollatorStdImplFactory,
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

#[tokio::test]
async fn test_collate_from_dumped_state_and_anchors() -> Result<()> {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    let current_dir = std::env::current_dir()?;
    tracing::trace!("current_dir: {:?}", current_dir);

    let dump_root_path = current_dir.join("../test/data/dump/local_0519_125_01");
    tracing::trace!("dump_root_path: {:?}", dump_root_path);

    if !dump_root_path.exists() {
        tracing::trace!("dump_root_path not exists, exit test");
        return Ok(());
    }

    // init test storage from the dump
    let (storage, _tmp_dir, mc_top_blocks_info) = init_test_storage(&dump_root_path).await?;

    let mut top_mc_block_id = BlockId::default();
    let mut top_sc_blocks_ids = vec![];
    for (top_block_id, _) in &mc_top_blocks_info {
        if top_block_id.is_masterchain() {
            top_mc_block_id = *top_block_id;
        } else {
            top_sc_blocks_ids.push(*top_block_id);
        }
    }
    let top_sc_block_id = top_sc_blocks_ids[0];

    // init state node adapter
    let state_node_adapter = Arc::new(StateNodeAdapterStdImpl::new(
        Arc::new(StubStateNodeEventListener::default()),
        storage.clone(),
        CollatorSyncContext::Recent,
    ));

    // init internal queue
    let queue_state_factory = QueueStateImplFactory::new(storage.clone());
    let queue_factory = QueueFactoryStdImpl {
        state: queue_state_factory,
        config: Default::default(),
    };
    let queue = queue_factory.create();
    let mq_adapter = Arc::new(MessageQueueAdapterStdImpl::<EnqueuedMessage>::new(queue));

    // restore queue for top blocks
    for (top_block_id, _) in &mc_top_blocks_info {
        let queue_diff_stuff = state_node_adapter.load_diff(top_block_id).await?.unwrap();
        let block_stuff = state_node_adapter.load_block(top_block_id).await?.unwrap();
        let out_msgs = block_stuff.load_extra()?.out_msg_description.load()?;
        let queue_diff_with_messages =
            QueueDiffWithMessages::from_queue_diff(&queue_diff_stuff, &out_msgs)?;
        let statistics = DiffStatistics::from_diff(
            &queue_diff_with_messages,
            top_block_id.shard,
            queue_diff_stuff.as_ref().min_message,
            queue_diff_stuff.as_ref().max_message,
        );
        mq_adapter.apply_diff(
            queue_diff_with_messages,
            top_block_id.as_short_id(),
            queue_diff_stuff.diff_hash(),
            statistics,
            None,
        )?;
    }
    mq_adapter.commit_diff(&mc_top_blocks_info, &[0, 1].into_iter().collect())?;

    // init stub mempool adapter
    let anchors_dump_path = current_dir.join("../test/data/dump/testnet_0517_2685990_01/anchors");
    let mpool_adapter = MempoolAdapterStubImpl::with_dumped_anchors_externals(
        Arc::new(StubMempoolEventListener::default()),
        &anchors_dump_path,
    )?;

    // create shard collator
    let config = CollatorConfig {
        supported_block_version: 50,
        supported_capabilities: supported_capabilities(),
        min_mc_block_delta_from_bc_to_sync: 3,
        check_value_flow: true,
        validate_config: true,
        fast_sync: true,
    };
    let config = Arc::new(config);

    let shard_id = ShardIdent::BASECHAIN;

    let collation_session = CollationSessionInfo::new(
        shard_id,
        0,
        ValidatorSubsetInfo {
            validators: vec![],
            short_hash: 0,
        },
        None,
    );
    let collation_session = Arc::new(collation_session);

    let mc_state = state_node_adapter.load_state(&top_mc_block_id).await?;
    let all_shards_processed_to_by_partitions =
        get_all_shards_processed_to_by_partitions_for_mc_block_without_cache(
            &top_mc_block_id,
            state_node_adapter.clone(),
        )
        .await?;
    let mc_data = McData::load_from_state(&mc_state, all_shards_processed_to_by_partitions)?;

    // we should set genesis round from last processed_to anchor and millis from the first dumped anchor
    let top_sc_state = state_node_adapter.load_state(&top_sc_block_id).await?;
    let processed_upto = top_sc_state.state().processed_upto.load()?;
    let processed_upto = ProcessedUptoInfoStuff::try_from(processed_upto)?;
    let anchors_processing_info = get_anchors_processing_info(
        &shard_id,
        &mc_data,
        &top_sc_block_id,
        top_sc_state.get_gen_chain_time(),
        processed_upto.get_min_externals_processed_to()?,
    )
    .unwrap();

    let first_anchor = mpool_adapter.get_first_anchor().await;

    let mempool_config_override = MempoolGlobalConfig {
        genesis_info: GenesisInfo {
            start_round: anchors_processing_info.processed_to_anchor_id,
            genesis_millis: first_anchor.chain_time,
        },
        consensus_config: None,
    };
    let mempool_config_override = Some(mempool_config_override);

    let cancel_collation = Arc::new(Notify::new());

    let collator_cx = CollatorContext {
        mq_adapter,
        mpool_adapter,
        state_node_adapter,
        config,
        collation_session,
        listener: Arc::new(StubCollatorEventListener::default()),
        shard_id,
        prev_blocks_ids: vec![top_sc_block_id],
        mc_data,
        mempool_config_override,
        cancel_collation,
    };

    let (mut collator, working_state_tx) = CollatorStdImpl::create(&collator_cx);

    // init collator and try collate next block
    collator
        .init_collator(
            collator_cx.prev_blocks_ids.clone(),
            collator_cx.mc_data.clone(),
            working_state_tx,
        )
        .await?;

    Ok(())
}

pub async fn init_test_storage(
    dump_root_path: &Path,
) -> Result<(Storage, tempfile::TempDir, Vec<(BlockId, bool)>)> {
    // read top block ids
    let dump_top_blocks_ids_path = dump_root_path.join("top_blocks_ids.json");
    let top_blocks_ids: Vec<String> = load_json_from_file(&dump_top_blocks_ids_path).unwrap();
    let top_block_ids: Vec<_> = top_blocks_ids
        .iter()
        .map(|id_str| BlockId::from_str(id_str).unwrap())
        .collect();

    let (storage, tmp_dir) = Storage::new_temp().await?;

    let mut top_mc_block_id = None;
    let mut mc_top_blocks = vec![];

    // read each and store to storage
    for top_block_id in &top_block_ids {
        let prefix = if top_block_id.is_masterchain() {
            top_mc_block_id = Some(*top_block_id);
            "top_mc".to_owned()
        } else {
            format!("top_sc_{}", top_block_id.as_short_id()).replace(":", "_")
        };

        // load state
        let state_dump_path = dump_root_path.join(format!("{}_state.boc", prefix));
        tracing::trace!(
            top_block_id = %top_block_id.as_short_id(),
            "loading state from {:?}",
            state_dump_path,
        );
        let state_data: Bytes = std::fs::read(state_dump_path).unwrap().into();
        let state_root = Boc::decode(state_data).unwrap();
        let state: Box<ShardStateUnsplit> = state_root.parse().unwrap();

        // load block
        let block_dump_path = dump_root_path.join(format!("{}_block.boc", prefix));
        tracing::trace!(
            top_block_id = %top_block_id.as_short_id(),
            "loading block from {:?}",
            block_dump_path,
        );
        let block_data: Bytes = std::fs::read(block_dump_path).unwrap().into();
        let block_root = Boc::decode(&block_data).unwrap();
        let block: Block = block_root.parse().unwrap();

        // make block meta
        let state_extra = state.load_custom().unwrap();
        let block_meta_data = NewBlockMeta {
            is_key_block: state_extra
                .map(|extra| extra.after_key_block)
                .unwrap_or_default(),
            gen_utime: state.gen_utime,
            ref_by_mc_seqno: top_mc_block_id.unwrap().seqno,
        };

        // store block
        let block_stuff =
            BlockStuff::from_block_and_root(top_block_id, block, block_root, block_data.len());
        let handle = storage
            .block_storage()
            .store_block_data(&block_stuff, &ArchiveData::New(block_data), block_meta_data)
            .await
            .unwrap()
            .handle;
        tracing::trace!(
            top_block_id = %top_block_id.as_short_id(),
            "block stored",
        );

        // store state
        let state_stuff = ShardStateStuff::from_state_and_root(
            top_block_id,
            state,
            state_root,
            storage.shard_state_storage().min_ref_mc_state(),
        )
        .unwrap();

        storage
            .shard_state_storage()
            .store_state(&handle, &state_stuff, Default::default())
            .await
            .unwrap();
        tracing::trace!(
            top_block_id = %top_block_id.as_short_id(),
            "state stored",
        );

        // load and store diff
        let block_diff_dump_path = dump_root_path.join(format!("{}_block_diff.boc", prefix));
        tracing::trace!(
            top_block_id = %top_block_id.as_short_id(),
            "loading block diff from {:?}",
            block_diff_dump_path,
        );
        let block_diff_data: Bytes = std::fs::read(block_diff_dump_path).unwrap().into();
        let block_diff_stuff = QueueDiffStuff::deserialize(top_block_id, &block_diff_data)?;
        let block_diff_stuff_aug =
            QueueDiffStuffAug::new(block_diff_stuff, block_diff_data.to_vec());

        storage
            .block_storage()
            .store_queue_diff(&block_diff_stuff_aug, handle.into())
            .await
            .unwrap();
        tracing::trace!(
            top_block_id = %top_block_id.as_short_id(),
            "block diff stored",
        );

        if top_block_id.is_masterchain() {
            // last mc block id
            storage.node_state().store_last_mc_block_id(top_block_id);
            tracing::trace!(
                top_block_id = %top_block_id.as_short_id(),
                "last_mc_block_id stored",
            );

            // top blocks
            mc_top_blocks.push((*top_block_id, true));
            let mut top_shard_blocks_info = state_stuff.get_top_shard_blocks_info()?;
            mc_top_blocks.append(&mut top_shard_blocks_info);
        }
    }

    Ok((storage, tmp_dir, mc_top_blocks))
}

async fn _dump_init_state() -> Result<()> {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::TRACE);

    // open storage
    let current_dir = std::env::current_dir()?;
    tracing::trace!("current_dir: {:?}", current_dir);

    let db_path = current_dir.join("../.temp/db1/");
    tracing::trace!("db_path: {:?}", db_path);

    let db_path_canonicalized = std::fs::canonicalize(db_path.clone());
    tracing::trace!("db_path_canonicalized: {:?}", db_path_canonicalized);

    let storage_config = StorageConfig::new_potato(&db_path);
    let storage = Storage::builder()
        .with_config(storage_config)
        .build()
        .await
        .context("failed to create storage")?;

    tracing::trace!(
        root_dir = %storage.root().path().display(),
        "initialized storage"
    );

    // get last mc_block_id
    let node_state = storage.node_state();

    let last_mc_block_id = node_state.load_last_mc_block_id();
    tracing::trace!("last_mc_block_id: {:?}", last_mc_block_id);

    let init_mc_block_id = node_state.load_init_mc_block_id();
    tracing::trace!("init_mc_block_id: {:?}", init_mc_block_id);

    // load and dump top mc block and shard blocks, states, diffs
    let dump_root_path = current_dir.join("../test/data/dump/local_0519_125_01");
    tracing::trace!("dump_root_path: {:?}", dump_root_path);
    std::fs::create_dir_all(&dump_root_path)?;

    let mut top_blocks_ids = vec![];

    // top mc block
    let top_mc_block_id = last_mc_block_id.unwrap();
    top_blocks_ids.push(top_mc_block_id);

    let mc_state =
        _dump_block_diff_state(&storage, &top_mc_block_id, &dump_root_path, "top_mc").await?;

    // top shard blocks, diffs, and states
    for item in mc_state.shards()?.latest_blocks() {
        let top_shard_block_id = item?;
        top_blocks_ids.push(top_shard_block_id);

        _dump_block_diff_state(
            &storage,
            &top_shard_block_id,
            &dump_root_path,
            format!("top_sc_{}", top_shard_block_id.as_short_id())
                .replace(":", "_")
                .as_str(),
        )
        .await?;
    }

    // store top blocks ids
    let dump_top_blocks_ids_path = dump_root_path.join("top_blocks_ids.json");
    write_json_to_file(
        &top_blocks_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>(),
        dump_top_blocks_ids_path,
    )?;

    Ok(())
}

async fn _dump_block_diff_state(
    storage: &Storage,
    block_id: &BlockId,
    dump_root_path: &Path,
    prefix: &str,
) -> Result<ShardStateStuff> {
    let handle = storage
        .block_handle_storage()
        .load_handle(block_id)
        .unwrap();

    let blocks = storage.block_storage();

    // block
    let block_data = {
        let data = blocks.load_block_data_raw_ref(&handle).await?;
        Bytes::copy_from_slice(data.as_ref())
    };
    let block_dump_path = dump_root_path.join(format!("{}_block.boc", prefix));
    std::fs::write(block_dump_path, block_data)?;

    // block diff
    let block_diff_data = {
        let data = storage.block_storage().load_queue_diff_raw(&handle).await?;
        Bytes::copy_from_slice(data.as_ref())
    };
    let block_diff_dump_path = dump_root_path.join(format!("{}_block_diff.boc", prefix));
    std::fs::write(block_diff_dump_path, block_diff_data)?;

    // state
    let state = storage.shard_state_storage().load_state(block_id).await?;
    let state_data: Bytes = BocRepr::encode_rayon(state.state())?.into();
    let state_dump_path = dump_root_path.join(format!("{}_state.boc", prefix));
    std::fs::write(state_dump_path, state_data)?;

    Ok(state)
}

#[derive(Debug, Deserialize)]
pub struct NodeKeys {
    pub secret: HashBytes,
}

#[derive(Default)]
struct StubMempoolEventListener {}

#[async_trait]
impl MempoolEventListener for StubMempoolEventListener {
    async fn on_new_anchor(&self, _anchor: Arc<MempoolAnchor>) -> Result<()> {
        // do nothing
        Ok(())
    }
}

#[derive(Default)]
struct StubStateNodeEventListener {}

#[async_trait]
impl StateNodeEventListener for StubStateNodeEventListener {
    #[tracing::instrument(skip_all, fields(block_id = %_state.block_id().as_short_id()))]
    async fn on_block_accepted(&self, _state: &ShardStateStuff) -> Result<()> {
        // do nothign
        Ok(())
    }
    #[tracing::instrument(skip_all, fields(block_id = %_state.block_id().as_short_id()))]
    async fn on_block_accepted_external(&self, _state: &ShardStateStuff) -> Result<()> {
        // do nothign
        Ok(())
    }
}

#[derive(Default)]
struct StubCollatorEventListener {}

#[async_trait]
impl CollatorEventListener for StubCollatorEventListener {
    #[tracing::instrument(skip_all, fields(next_block_id = %_next_block_id_short, ct = _anchor_chain_time, ?_force_mc_block))]
    async fn on_skipped(
        &self,
        _prev_mc_block_id: BlockId,
        _next_block_id_short: BlockIdShort,
        _anchor_chain_time: u64,
        _force_mc_block: ForceMasterCollation,
        _collation_config: Arc<CollationConfig>,
    ) -> Result<()> {
        // do nothing
        Ok(())
    }
    #[tracing::instrument(skip_all, fields(next_block_id = %_next_block_id_short, ?_cancel_reason))]
    async fn on_cancelled(
        &self,
        _prev_mc_block_id: BlockId,
        _next_block_id_short: BlockIdShort,
        _cancel_reason: CollationCancelReason,
    ) -> Result<()> {
        // do nothing
        Ok(())
    }
    #[tracing::instrument(
        skip_all,
        fields(block_id = %_collation_result.candidate.block.id().as_short_id()),
    )]
    async fn on_block_candidate(&self, _collation_result: BlockCollationResult) -> Result<()> {
        // do nothing
        Ok(())
    }
    #[tracing::instrument(skip_all, fields(?_collation_session_id))]
    async fn on_collator_stopped(&self, _collation_session_id: CollationSessionId) -> Result<()> {
        // do nothing
        Ok(())
    }
}
