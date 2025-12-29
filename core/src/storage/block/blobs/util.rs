use anyhow::Result;
use bytes::Buf;
use tycho_block_util::archive::ArchiveEntryType;
use tycho_block_util::block::ShardHeights;
use tycho_storage::kv::StoredValue;
use tycho_types::cell::HashBytes;
use tycho_types::models::*;
use tycho_util::sync::CancellationFlag;
use weedb::rocksdb;

use super::super::package_entry::{PackageEntryKey, PartialBlockId};
use super::BlobStorage;
use super::types::BlockGcStats;
use crate::storage::BlockFlags;

pub fn remove_blocks(
    blob_storage: &BlobStorage,
    max_blocks_per_batch: Option<usize>,
    mc_seqno: u32,
    shard_heights: ShardHeights,
    cancelled: Option<&CancellationFlag>,
) -> Result<BlockGcStats> {
    let db = &blob_storage.db;
    let blocks = blob_storage.blocks().clone();
    let mut stats = BlockGcStats::default();

    let raw = db.rocksdb().as_ref();
    let full_block_ids_cf = db.full_block_ids.cf();
    let block_connections_cf = db.block_connections.cf();
    let block_handles_cf = db.block_handles.cf();

    // Create batch
    let mut batch = rocksdb::WriteBatch::default();
    let mut batch_len = 0;

    // Iterate all entries and find expired items
    let mut blocks_iter =
        raw.raw_iterator_cf_opt(&full_block_ids_cf, db.full_block_ids.new_read_config());
    blocks_iter.seek_to_first();

    let block_handles_readopts = db.block_handles.new_read_config();
    let is_persistent = |root_hash: &[u8; 32]| -> Result<bool> {
        const FLAGS: u64 = ((BlockFlags::IS_KEY_BLOCK.bits()
            | BlockFlags::IS_PERSISTENT.bits()
            | BlockFlags::IS_ZEROSTATE.bits()) as u64)
            << 32;

        let Some(value) =
            raw.get_pinned_cf_opt(&block_handles_cf, root_hash, &block_handles_readopts)?
        else {
            return Ok(false);
        };
        Ok(value.as_ref().get_u64_le() & FLAGS != 0)
    };

    let mut key_buffer = [0u8; PackageEntryKey::SIZE_HINT];
    let mut delete_range = |batch: &mut rocksdb::WriteBatch,
                            from: &BlockIdShort,
                            to: &BlockIdShort|
     -> Result<()> {
        debug_assert_eq!(from.shard, to.shard);
        debug_assert!(from.seqno <= to.seqno);

        let range_from = &mut key_buffer;
        range_from[..4].copy_from_slice(&from.shard.workchain().to_be_bytes());
        range_from[4..12].copy_from_slice(&from.shard.prefix().to_be_bytes());
        range_from[12..16].copy_from_slice(&from.seqno.to_be_bytes());

        let mut range_to = *range_from;
        range_to[12..16].copy_from_slice(&to.seqno.saturating_add(1).to_be_bytes());

        // At this point we have two keys:
        // [workchain, shard, from_seqno, 0...]
        // [workchain, shard, to_seqno + 1, 0...]
        //
        // It will delete all entries in range [from_seqno, to_seqno) for this shard.
        // Keep only metadata cleanup in RocksDB
        batch.delete_range_cf(&full_block_ids_cf, &*range_from, &range_to);
        batch.delete_range_cf(&block_connections_cf, &*range_from, &range_to);

        for ty in [
            ArchiveEntryType::Block,
            ArchiveEntryType::Proof,
            ArchiveEntryType::QueueDiff,
        ] {
            let from_key = PackageEntryKey {
                block_id: PartialBlockId {
                    shard: from.shard,
                    seqno: from.seqno,
                    root_hash: HashBytes::ZERO,
                },
                ty,
            };

            let to_key = PackageEntryKey {
                block_id: PartialBlockId {
                    shard: to.shard,
                    seqno: to.seqno + 1,
                    root_hash: HashBytes::ZERO,
                },
                ty,
            };

            match blocks.remove_range(from_key..to_key) {
                Ok(deleted_count) => {
                    if deleted_count > 0 {
                        tracing::debug!(%from, %to, ?ty, deleted_count, "deleted from Cassadilia");
                    }
                }
                Err(e) => {
                    tracing::warn!(%from, %to, ?ty, error = ?e, "failed to delete from Cassadilia");
                }
            }
        }

        tracing::debug!(%from, %to, "delete_range");
        Ok(())
    };

    let mut cancelled = cancelled.map(|c| c.debounce(100));
    let mut current_range = None::<(BlockIdShort, BlockIdShort)>;
    loop {
        let key = match blocks_iter.key() {
            Some(key) => key,
            None => break blocks_iter.status()?,
        };

        if let Some(cancelled) = &mut cancelled
            && cancelled.check()
        {
            anyhow::bail!("blocks GC cancelled");
        }

        // Key structure:
        // [workchain id, 4 bytes]  |
        // [shard id, 8 bytes]      | BlockIdShort
        // [seqno, 4 bytes]         |
        // [root hash, 32 bytes] <-
        // ..
        let block_id = BlockIdShort::from_slice(key);
        let root_hash: &[u8; 32] = key[16..48].try_into().unwrap();
        let is_masterchain = block_id.shard.is_masterchain();

        // Don't gc latest blocks, key blocks or persistent blocks
        if block_id.seqno == 0
            || is_masterchain && block_id.seqno >= mc_seqno
            || !is_masterchain
                && shard_heights.contains_shard_seqno(&block_id.shard, block_id.seqno)
            || is_persistent(root_hash)?
        {
            // Remove the current range
            if let Some((from, to)) = current_range.take() {
                delete_range(&mut batch, &from, &to)?;
                batch_len += 1; // Ensure that we flush the batch
            }
            blocks_iter.next();
            continue;
        }

        match &mut current_range {
            // Delete the previous range and start a new one
            Some((from, to)) if from.shard != block_id.shard => {
                delete_range(&mut batch, from, to)?;
                *from = block_id;
                *to = block_id;
            }
            // Update the current range
            Some((_, to)) => *to = block_id,
            // Start a new range
            None => current_range = Some((block_id, block_id)),
        }

        // Count entry
        stats.total_blocks_removed += 1;
        if is_masterchain {
            stats.mc_blocks_removed += 1;
        }

        batch.delete_cf(&block_handles_cf, root_hash);

        batch_len += 1;
        if matches!(
            max_blocks_per_batch,
            Some(max_blocks_per_batch) if batch_len >= max_blocks_per_batch
        ) {
            tracing::info!(
                total_blocks_removed = stats.total_blocks_removed,
                "applying intermediate batch",
            );
            let batch = std::mem::take(&mut batch);
            raw.write(batch)?;
            batch_len = 0;
        }

        blocks_iter.next();
    }

    if let Some((from, to)) = current_range.take() {
        delete_range(&mut batch, &from, &to)?;
        batch_len += 1; // Ensure that we flush the batch
    }

    if batch_len > 0 {
        tracing::info!("applying final batch");
        raw.write(batch)?;
    }

    // Done
    Ok(stats)
}
