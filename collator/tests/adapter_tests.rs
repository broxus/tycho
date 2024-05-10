use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_types::cell::Cell;
use everscale_types::models::{BlockId, ShardIdent, ShardStateUnsplit};
use tycho_block_util::block::{BlockStuff, BlockStuffAug};
use tycho_block_util::state::{MinRefMcStateTracker, ShardStateStuff};
use tycho_collator::state_node::{
    StateNodeAdapter, StateNodeAdapterStdImpl, StateNodeEventListener,
};
use tycho_collator::test_utils::{prepare_test_storage, try_init_test_tracing};
use tycho_collator::types::BlockStuffForSync;
use tycho_core::block_strider::{
    BlockStrider, EmptyBlockProvider, PersistentBlockStriderState, PrintSubscriber,
};
use tycho_storage::Storage;

struct MockEventListener {
    accepted_count: Arc<AtomicUsize>,
}

#[async_trait]
impl StateNodeEventListener for MockEventListener {
    async fn on_block_accepted(&self, _block_id: &BlockId) -> Result<()> {
        self.accepted_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn on_block_accepted_external(&self, _state: &ShardStateStuff) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_add_and_get_block() {
    let (mock_storage, _tmp_dir) = Storage::new_temp().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = Arc::new(MockEventListener {
        accepted_count: counter.clone(),
    });
    let adapter = StateNodeAdapterStdImpl::new(listener, mock_storage);

    // Test adding a block

    let empty_block = BlockStuff::new_empty(ShardIdent::BASECHAIN, 1);
    let block_id = *empty_block.id();
    let block_stuff_aug = BlockStuffAug::loaded(empty_block);

    let block = BlockStuffForSync {
        block_id,
        block_stuff_aug,
        signatures: Default::default(),
        prev_blocks_ids: Vec::new(),
        top_shard_blocks_ids: Vec::new(),
    };
    adapter.accept_block(block).await.unwrap();

    // Test getting the next block (which should be the one just added)
    let next_block = adapter.wait_for_block(&block_id).await;
    assert!(
        next_block.is_some(),
        "Block should be retrieved after being added"
    );
}

#[tokio::test]
async fn test_storage_accessors() {
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

    let counter = Arc::new(AtomicUsize::new(0));
    let listener = Arc::new(MockEventListener {
        accepted_count: counter.clone(),
    });
    let adapter = StateNodeAdapterStdImpl::new(listener, storage.clone());

    let last_mc_block_id = adapter.load_last_applied_mc_block_id().await.unwrap();

    storage
        .shard_state_storage()
        .load_state(&last_mc_block_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_add_and_get_next_block() {
    let (mock_storage, _tmp_dir) = Storage::new_temp().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = Arc::new(MockEventListener {
        accepted_count: counter.clone(),
    });
    let adapter = StateNodeAdapterStdImpl::new(listener, mock_storage);

    // Test adding a block
    let prev_block = BlockStuff::new_empty(ShardIdent::MASTERCHAIN, 1);
    let prev_block_id = prev_block.id();

    let empty_block = BlockStuff::new_empty(ShardIdent::MASTERCHAIN, 2);
    let block_stuff_aug = BlockStuffAug::loaded(empty_block);

    let block = BlockStuffForSync {
        block_id: *block_stuff_aug.data.id(),
        block_stuff_aug,
        signatures: Default::default(),
        prev_blocks_ids: vec![*prev_block_id],
        top_shard_blocks_ids: Vec::new(),
    };
    adapter.accept_block(block).await.unwrap();

    let next_block = adapter.wait_for_block_next(prev_block_id).await;
    assert!(
        next_block.is_some(),
        "Block should be retrieved after being added"
    );
}

#[tokio::test]
async fn test_add_read_handle_1000_blocks_parallel() {
    try_init_test_tracing(tracing_subscriber::filter::LevelFilter::DEBUG);
    tycho_util::test::init_logger("test_add_read_handle_100000_blocks_parallel");

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

    let counter = Arc::new(AtomicUsize::new(0));
    let listener = Arc::new(MockEventListener {
        accepted_count: counter.clone(),
    });
    let adapter = Arc::new(StateNodeAdapterStdImpl::new(
        listener.clone(),
        storage.clone(),
    ));

    let empty_block = get_empty_block();
    let cloned_block = empty_block.block().clone();
    // Task 1: Adding 1000 blocks
    let add_blocks = {
        let adapter = adapter.clone();
        tokio::spawn(async move {
            for i in 1..=1000 {
                let block_id = BlockId {
                    shard: ShardIdent::new_full(0),
                    seqno: i,
                    root_hash: Default::default(),
                    file_hash: Default::default(),
                };
                let block_stuff_aug = BlockStuffAug::loaded(BlockStuff::with_block(
                    block_id.clone(),
                    cloned_block.clone(),
                ));

                let block = BlockStuffForSync {
                    block_id,
                    block_stuff_aug,
                    signatures: Default::default(),
                    prev_blocks_ids: Vec::new(),
                    top_shard_blocks_ids: Vec::new(),
                };
                let accept_result = adapter.accept_block(block).await;
                assert!(accept_result.is_ok(), "Block {} should be accepted", i);
            }
        })
    };

    // Task 2: Retrieving and handling 1000 blocks
    let handle_blocks = {
        let adapter = adapter.clone();
        tokio::spawn(async move {
            for i in 1..=1000 {
                let block_id = BlockId {
                    shard: ShardIdent::new_full(0),
                    seqno: i,
                    root_hash: Default::default(),
                    file_hash: Default::default(),
                };
                let next_block = adapter.wait_for_block(&block_id).await;
                assert!(
                    next_block.is_some(),
                    "Block {i} should be retrieved after being added",
                );

                let mcstate_tracker = MinRefMcStateTracker::new();
                let mut shard_state = ShardStateUnsplit::default();
                shard_state.shard_ident = block_id.shard;
                shard_state.seqno = block_id.seqno;

                let state = ShardStateStuff::from_state_and_root(
                    &block_id,
                    Box::new(shard_state),
                    Cell::default(),
                    &mcstate_tracker,
                )
                .unwrap();

                let handle_block = adapter.handle_state(&state).await;
                assert!(
                    handle_block.is_ok(),
                    "Block {i} should be handled after being added",
                );
            }
        })
    };

    // Await both tasks to complete
    let _ = tokio::join!(handle_blocks, add_blocks);

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1000,
        "1000 blocks should be accepted"
    );
}

pub fn get_empty_block() -> BlockStuffAug {
    let block_data = include_bytes!("../../core/tests/data/empty_block.bin");
    let block = everscale_types::boc::BocRepr::decode(block_data).unwrap();
    BlockStuffAug::new(
        BlockStuff::with_block(BlockId::default(), block),
        block_data.as_slice(),
    )
}
