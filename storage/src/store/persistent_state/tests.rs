use anyhow::Result;
use bytesize::ByteSize;
use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, CellBuilder, CellSlice, HashBytes};
use everscale_types::models::{
    CurrencyCollection, IntMsgInfo, IntermediateAddr, Lazy, Message, MsgEnvelope, MsgInfo, OutMsg,
    OutMsgDescr, OutMsgNew, OutMsgQueueUpdates, ShardIdent,
};
use everscale_types::num::Tokens;
use tycho_block_util::queue::{QueueDiffStuff, QueueKey};
use tycho_block_util::state::ShardStateStuff;
use tycho_util::compression::zstd_decompress;

use super::*;
use crate::{NewBlockMeta, Storage, StorageConfig};

#[tokio::test]
async fn persistent_shard_state() -> Result<()> {
    tycho_util::test::init_logger("persistent_shard_state", "debug");

    let (storage, tmp_dir) = Storage::new_temp().await?;
    assert!(storage.node_state().load_init_mc_block_id().is_none());

    let shard_states = storage.shard_state_storage();
    let persistent_states = storage.persistent_state_storage();

    // Read zerostate
    static ZEROSTATE_BOC: &[u8] = include_bytes!("../../../../core/tests/data/zerostate.boc");
    let zerostate_root = Boc::decode(ZEROSTATE_BOC)?;
    let zerostate_id = BlockId {
        shard: ShardIdent::MASTERCHAIN,
        seqno: 0,
        root_hash: *zerostate_root.repr_hash(),
        file_hash: Boc::file_hash_blake(ZEROSTATE_BOC),
    };

    let zerostate = ShardStateStuff::from_root(
        &zerostate_id,
        zerostate_root,
        shard_states.min_ref_mc_state(),
    )?;

    // Write zerostate to db
    let (handle, _) = storage.block_handle_storage().create_or_load_handle(
        &zerostate_id,
        NewBlockMeta::zero_state(zerostate.as_ref().gen_utime, true),
    );

    shard_states
        .store_state(&handle, &zerostate, Default::default())
        .await?;

    // Check seqno
    let min_ref_mc_state = shard_states.min_ref_mc_state();
    assert_eq!(
        min_ref_mc_state.seqno(),
        Some(zerostate.as_ref().min_ref_mc_seqno)
    );

    // Load zerostate from db
    {
        let loaded_state = shard_states.load_state(zerostate.block_id()).await?;
        assert_eq!(zerostate.state(), loaded_state.state());
        assert_eq!(zerostate.block_id(), loaded_state.block_id());
        assert_eq!(zerostate.root_cell(), loaded_state.root_cell());
        // NOTE: `loaded_state` must be dropped here since cells are preventing storage from drop
    }

    // Write persistent state to file
    assert!(persistent_states.load_oldest_known_handle().is_none());

    persistent_states
        .store_shard_state(0, &handle, zerostate.ref_mc_state_handle().clone())
        .await?;

    // Check if state exists
    let exist = persistent_states.state_exists(zerostate.block_id(), PersistentStateKind::Shard);
    assert!(exist);

    let read_verify_state = || async {
        let persistent_state_data = persistent_states
            .read_state_part(zerostate.block_id(), 0, PersistentStateKind::Shard)
            .await
            .unwrap();

        let mut boc = Vec::new();
        zstd_decompress(&persistent_state_data, &mut boc)?;

        // Check state
        let cell = Boc::decode(&boc)?;
        assert_eq!(&cell, zerostate.root_cell());
        Ok::<_, anyhow::Error>(())
    };

    let verify_descriptor_cache = |expected_mc_seqno: u32| {
        let cached = persistent_states
            .inner
            .descriptor_cache
            .get(&CacheKey {
                block_id: zerostate_id,
                kind: PersistentStateKind::Shard,
            })
            .unwrap();
        assert_eq!(cached.mc_seqno, expected_mc_seqno);
    };

    verify_descriptor_cache(0);

    let expected_set = FastHashSet::from_iter([zerostate_id]);

    {
        let index = persistent_states.inner.mc_seqno_to_block_ids.lock();
        assert_eq!(index.get(&0), Some(&expected_set));
    }

    // Read persistent state a couple of times to check if it is stateless
    for _ in 0..2 {
        read_verify_state().await?;
    }

    // Reuse persistent state for a different block
    let new_mc_seqno = 123123;
    persistent_states
        .store_shard_state(
            new_mc_seqno,
            &handle,
            zerostate.ref_mc_state_handle().clone(),
        )
        .await?;

    // Check if state exists
    let exist = persistent_states.state_exists(zerostate.block_id(), PersistentStateKind::Shard);
    assert!(exist);
    for _ in 0..2 {
        read_verify_state().await?;
    }

    // Check if state file was reused
    let file_name = PersistentStateKind::Shard.make_file_name(&zerostate_id);
    let prev_file = persistent_states.inner.mc_states_dir(0).file(&file_name);
    let new_file = persistent_states
        .inner
        .mc_states_dir(new_mc_seqno)
        .file(file_name);

    let prev_file = std::fs::read(prev_file.path())?;
    let new_file = std::fs::read(new_file.path())?;
    assert_eq!(prev_file, new_file);

    verify_descriptor_cache(new_mc_seqno);

    {
        let index = persistent_states.inner.mc_seqno_to_block_ids.lock();
        assert!(!index.contains_key(&0));
        assert_eq!(index.get(&new_mc_seqno), Some(&expected_set));
    }

    // Close the previous storage instance
    drop(storage);

    // And reload it
    let new_storage = Storage::builder()
        .with_config(StorageConfig::new_potato(tmp_dir.path()))
        .build()
        .await?;

    let new_persistent_states = new_storage.persistent_state_storage();

    {
        let index = new_persistent_states.inner.mc_seqno_to_block_ids.lock();
        assert!(!index.contains_key(&0));
        assert_eq!(index.get(&new_mc_seqno), Some(&expected_set));
    }

    let new_cached = new_persistent_states
        .inner
        .descriptor_cache
        .get(&CacheKey {
            block_id: zerostate_id,
            kind: PersistentStateKind::Shard,
        })
        .unwrap()
        .clone();
    assert_eq!(new_cached.mc_seqno, new_mc_seqno);
    assert_eq!(new_cached.file.as_slice(), new_file);

    Ok(())
}

#[tokio::test]
async fn persistent_queue_state_read_write() -> Result<()> {
    tycho_util::test::init_logger("persistent_queue_state_read_write", "debug");

    struct SimpleBlock {
        queue_diff: QueueDiffStuff,
        out_msgs: OutMsgDescr,
    }

    let (storage, _temp_dir) = Storage::new_temp().await?;

    let target_mc_seqno = 10;

    // Prepare blocks with queue diffs
    let shard = ShardIdent::MASTERCHAIN;
    let mut blocks = Vec::new();

    let mut prev_hash = HashBytes::ZERO;
    let mut target_message_count = 0;
    for seqno in 1..=target_mc_seqno {
        let start_lt = seqno as u64 * 10000;

        let mut messages = Vec::new();
        for i in 0..5000 {
            let message = Message {
                info: MsgInfo::Int(IntMsgInfo {
                    created_lt: start_lt + i,
                    ..Default::default()
                }),
                init: None,
                body: CellSlice::default(),
                layout: None,
            };
            let cell = CellBuilder::build_from(message)?;
            messages.push((
                *cell.repr_hash(),
                CurrencyCollection::ZERO,
                OutMsg::New(OutMsgNew {
                    out_msg_envelope: Lazy::new(&MsgEnvelope {
                        cur_addr: IntermediateAddr::FULL_SRC_SAME_WORKCHAIN,
                        next_addr: IntermediateAddr::FULL_DEST_SAME_WORKCHAIN,
                        fwd_fee_remaining: Tokens::ZERO,
                        message: Lazy::from_raw(cell),
                    })?,
                    transaction: Lazy::from_raw(Cell::default()),
                }),
            ));
        }
        target_message_count += messages.len();

        messages.sort_unstable_by(|(a, _, _), (b, _, _)| a.cmp(b));
        let out_msgs = OutMsgDescr::try_from_sorted_slice(&messages)?;

        let queue_diff = QueueDiffStuff::builder(shard, seqno, &prev_hash)
            .with_processed_to([(shard, QueueKey::min_for_lt(0))].into())
            .with_messages(
                &QueueKey::max_for_lt(0),
                &QueueKey::max_for_lt(0),
                messages.iter().map(|(hash, _, _)| hash),
            )
            .serialize();

        let block_id = BlockId {
            shard,
            seqno,
            root_hash: HashBytes::ZERO,
            file_hash: HashBytes::ZERO,
        };

        prev_hash = *queue_diff.hash();

        let queue_diff = queue_diff.build(&block_id).data;

        blocks.push(SimpleBlock {
            queue_diff,
            out_msgs,
        });
    }

    // Write blocks to file
    let persistent_states = storage.persistent_state_storage();
    let states_dir = persistent_states
        .inner
        .prepare_persistent_states_dir(target_mc_seqno)?;

    let target_header = QueueStateHeader {
        shard_ident: shard,
        seqno: target_mc_seqno,
        queue_diffs: blocks
            .iter()
            .rev()
            .map(|block| block.queue_diff.as_ref().clone())
            .collect(),
    };

    let target_block_id = BlockId {
        shard,
        seqno: target_mc_seqno,
        root_hash: HashBytes::ZERO,
        file_hash: HashBytes::ZERO,
    };
    QueueStateWriter::new(
        &states_dir,
        &target_block_id,
        target_header.clone(),
        blocks
            .iter()
            .rev()
            .map(|block| block.queue_diff.zip(&block.out_msgs))
            .collect(),
    )
    .write(None)?;

    persistent_states.inner.cache_state(
        target_mc_seqno,
        &target_block_id,
        PersistentStateKind::Queue,
    )?;

    // Check storage state
    let expected_set = FastHashSet::from_iter([target_block_id]);
    {
        let index = persistent_states.inner.mc_seqno_to_block_ids.lock();
        assert!(!index.contains_key(&0));
        assert_eq!(index.get(&target_mc_seqno), Some(&expected_set));
    }

    let decompressed = {
        let cached = persistent_states
            .inner
            .descriptor_cache
            .get(&CacheKey {
                block_id: target_block_id,
                kind: PersistentStateKind::Queue,
            })
            .unwrap()
            .clone();
        assert_eq!(cached.mc_seqno, target_mc_seqno);

        let mut written = tokio::fs::read(
            states_dir
                .file(PersistentStateKind::Queue.make_file_name(&target_block_id))
                .path(),
        )
        .await?;
        assert_eq!(written, cached.file.as_slice());

        let compressed_size = written.len() as u64;

        written.clear();
        zstd_decompress(&cached.file, &mut written)?;

        let decompressed_size = written.len() as u64;

        assert!(compressed_size <= decompressed_size);

        tracing::info!(
            compressed_size = %ByteSize(compressed_size),
            decompressed_size = %ByteSize(decompressed_size),
        );

        written
    };

    let tail_len = blocks
        .iter()
        .rev()
        .map(|block| block.queue_diff.as_ref().clone())
        .len() as u32;

    // Read queue queue state from file
    let top_update = OutMsgQueueUpdates {
        diff_hash: *blocks.last().unwrap().queue_diff.diff_hash(),
        tail_len,
    };
    let mut reader = QueueStateReader::begin_from_mapped(&decompressed, &top_update)?;
    assert_eq!(reader.state().header, target_header);
    assert!(reader.state().messages.len() > 1);

    for (i, chunk) in reader.state().messages.iter().enumerate() {
        tracing::info!(i, chunk_size = %ByteSize(chunk.len() as u64));
    }

    let mut read_messages = 0;
    while let Some(cell) = reader.read_next_message()? {
        let msg = cell.parse::<Message<'_>>()?;
        let MsgInfo::Int(_) = msg.info else {
            panic!("unexpected message type");
        };
        assert!(msg.init.is_none());
        assert!(msg.body.is_empty());
        read_messages += 1;
    }
    assert_eq!(read_messages, target_message_count);

    reader.finish()?;

    Ok(())
}
