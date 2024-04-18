use anyhow::Result;
use async_trait::async_trait;
use everscale_types::models::{BlockId, ShardIdent};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tycho_block_util::block::block_stuff::get_empty_block;
use tycho_block_util::block::BlockStuff;
use tycho_collator::state_node::{
    StateNodeAdapter, StateNodeAdapterStdImpl, StateNodeEventListener,
};
use tycho_collator::types::BlockStuffForSync;
use tycho_core::block_strider::provider::BlockProvider;
use tycho_core::block_strider::subscriber::BlockSubscriber;
use tycho_storage::build_tmp_storage;

struct MockEventListener {
    accepted_count: Arc<AtomicUsize>,
}

#[async_trait]
impl StateNodeEventListener for MockEventListener {
    async fn on_block_accepted(&self, _block_id: &BlockId) -> Result<()> {
        self.accepted_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn on_block_accepted_external(&self, _block_id: &BlockId) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_add_and_get_block() {
    let mock_storage = build_tmp_storage().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = Arc::new(MockEventListener {
        accepted_count: counter.clone(),
    });
    let adapter = StateNodeAdapterStdImpl::create(listener, mock_storage);

    // Test adding a block
    let block_id = BlockId {
        shard: ShardIdent::new_full(0),
        seqno: 1,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };
    let block = BlockStuffForSync {
        block_id,
        block_stuff: None,
        signatures: Default::default(),
        prev_blocks_ids: Vec::new(),
        top_shard_blocks_ids: Vec::new(),
    };
    adapter.accept_block(block).await.unwrap();

    // Test getting the next block (which should be the one just added)
    let next_block = adapter.get_block(&block_id).await;
    assert!(
        next_block.is_some(),
        "Block should be retrieved after being added"
    );
}

#[tokio::test]
async fn test_add_and_get_next_block() {
    let mock_storage = build_tmp_storage().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = Arc::new(MockEventListener {
        accepted_count: counter.clone(),
    });
    let adapter = StateNodeAdapterStdImpl::create(listener, mock_storage);

    // Test adding a block
    let previous_block_id = BlockId {
        shard: ShardIdent::new_full(ShardIdent::MASTERCHAIN.workchain()),
        seqno: 1,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };
    let block_id = BlockId {
        shard: ShardIdent::new_full(ShardIdent::MASTERCHAIN.workchain()),
        seqno: 2,
        root_hash: Default::default(),
        file_hash: Default::default(),
    };
    let block = BlockStuffForSync {
        block_id,
        block_stuff: None,
        signatures: Default::default(),
        prev_blocks_ids: vec![previous_block_id],
        top_shard_blocks_ids: Vec::new(),
    };
    adapter.accept_block(block).await.unwrap();

    let next_block = adapter.get_next_block(&previous_block_id).await;
    assert!(
        next_block.is_some(),
        "Block should be retrieved after being added"
    );
}

#[tokio::test]
async fn test_add_read_handle_100000_blocks_parallel() {
    let mock_storage = build_tmp_storage().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = Arc::new(MockEventListener {
        accepted_count: counter.clone(),
    });
    let adapter = Arc::new(StateNodeAdapterStdImpl::create(
        listener.clone(),
        mock_storage.clone(),
    ));

    let empty_block = get_empty_block();
    let cloned_block = empty_block.clone();
    // Task 1: Adding 100000 blocks
    let add_blocks = {
        let adapter = adapter.clone();
        tokio::spawn(async move {
            for i in 1..=100000 {
                let block_id = BlockId {
                    shard: ShardIdent::new_full(0),
                    seqno: i,
                    root_hash: Default::default(),
                    file_hash: Default::default(),
                };
                let block_stuff = BlockStuff::with_block(block_id.clone(), cloned_block.clone());

                let block = BlockStuffForSync {
                    block_id,
                    block_stuff: Some(block_stuff.clone()),
                    signatures: Default::default(),
                    prev_blocks_ids: Vec::new(),
                    top_shard_blocks_ids: Vec::new(),
                };
                let accept_result = adapter.accept_block(block).await;
                assert!(accept_result.is_ok(), "Block {} should be accepted", i);
            }
        })
    };

    // Task 2: Retrieving and handling 100000 blocks
    let handle_blocks = {
        let adapter = adapter.clone();
        tokio::spawn(async move {
            for i in 1..=100000 {
                let block_id = BlockId {
                    shard: ShardIdent::new_full(0),
                    seqno: i,
                    root_hash: Default::default(),
                    file_hash: Default::default(),
                };
                let next_block = adapter.get_block(&block_id).await;
                assert!(
                    next_block.is_some(),
                    "Block {} should be retrieved after being added",
                    i
                );

                let block_stuff = BlockStuff::with_block(block_id.clone(), empty_block.clone());

                let handle_block = adapter.handle_block(&block_stuff).await;
                assert!(
                    handle_block.is_ok(),
                    "Block {} should be handled after being added",
                    i
                );
            }
        })
    };

    // Await both tasks to complete
    let _ = tokio::join!(handle_blocks, add_blocks);

    assert_eq!(
        counter.load(Ordering::SeqCst),
        100000,
        "100000 blocks should be accepted"
    );
}
