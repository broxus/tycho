use bytesize::ByteSize;
use tycho_storage::kv::{
    DEFAULT_MIN_BLOB_SIZE, TableContext, default_block_based_table_factory,
    optimize_for_level_compaction, optimize_for_point_lookup, refcount, with_blob_db,
};
use weedb::rocksdb::{
    self, BlockBasedIndexType, BlockBasedOptions, BlockBasedTablePinningTier, CompactionPri,
    DBCompressionType, DataBlockIndexType, MergeOperands, Options, ReadOptions,
};
use weedb::{ColumnFamily, ColumnFamilyOptions};

/// Stores generic node parameters
/// - Key: `...`
/// - Value: `...`
pub struct State;

impl ColumnFamily for State {
    const NAME: &'static str = "state";
}

impl ColumnFamilyOptions<TableContext> for State {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        default_block_based_table_factory(opts, ctx);

        opts.set_optimize_filters_for_hits(true);
        optimize_for_point_lookup(opts, ctx);
    }
}

/// Stores prepared archives
/// - Key: `u32 (BE)` (archive id)
/// - Value: `Vec<u8>` (archive block ids)
pub struct ArchiveBlockIds;

impl ColumnFamily for ArchiveBlockIds {
    const NAME: &'static str = "archive_block_ids";
}

impl ColumnFamilyOptions<TableContext> for ArchiveBlockIds {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        default_block_based_table_factory(opts, ctx);

        // Uses 128MB * 6 = 768 GB
        optimize_for_level_compaction(opts, ctx, ByteSize::mib(128), 6);

        opts.set_merge_operator_associative("archive_data_merge", archive_data_merge);
        // data is hardly compressible and dataset is small
        opts.set_compression_type(DBCompressionType::None);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE, DBCompressionType::None);
    }
}

/// Stores archive lifetime events.
/// - Key: `u32 (BE)` (archive id) + `32 (BE)` (event id)
/// - Value: event data
pub struct ArchiveEvents;

impl ArchiveEvents {
    pub const KEY_LEN: usize = 4 + 4;
}

impl ColumnFamily for ArchiveEvents {
    const NAME: &'static str = "archive_events";

    fn read_options(opts: &mut rocksdb::ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

impl ColumnFamilyOptions<TableContext> for ArchiveEvents {
    fn options(opts: &mut rocksdb::Options, ctx: &mut TableContext) {
        default_block_based_table_factory(opts, ctx);
    }
}

/// Maps block root hash to block meta
/// - Key: `[u8; 32]`
/// - Value: `BlockMeta`
pub struct BlockHandles;

impl ColumnFamily for BlockHandles {
    const NAME: &'static str = "block_handles";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

impl ColumnFamilyOptions<TableContext> for BlockHandles {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        // Uses 128MB * 6 = 768 GB
        optimize_for_level_compaction(opts, ctx, ByteSize::mib(128), 6);

        opts.set_merge_operator_associative("block_handle_merge", block_handle_merge);
        optimize_for_point_lookup(opts, ctx);
    }
}

/// Maps seqno to key block id
/// - Key: `u32 (BE)`
/// - Value: `BlockIdExt`
pub struct KeyBlocks;

impl ColumnFamily for KeyBlocks {
    const NAME: &'static str = "key_blocks";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

impl ColumnFamilyOptions<TableContext> for KeyBlocks {}

/// Maps block id (partial) to file hash
pub struct FullBlockIds;

impl ColumnFamily for FullBlockIds {
    const NAME: &'static str = "full_block_ids";

    fn read_options(opts: &mut rocksdb::ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

impl ColumnFamilyOptions<TableContext> for FullBlockIds {
    fn options(opts: &mut rocksdb::Options, ctx: &mut TableContext) {
        default_block_based_table_factory(opts, ctx);
    }
}

/// Maps `BlockId` to root cell hash
/// - Key: `BlockId`
/// - Value: `[u8; 32] (state root hash)`
pub struct ShardStates;

impl ShardStates {
    pub const KEY_LEN: usize = 4 + 8 + 4 + 32 + 32;
}

impl ColumnFamily for ShardStates {
    const NAME: &'static str = "shard_states";
}

impl ColumnFamilyOptions<TableContext> for ShardStates {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        default_block_based_table_factory(opts, ctx);
        opts.set_compression_type(DBCompressionType::Zstd);
    }
}

/// Stores cells data
/// - Key: `[u8; 32]` (cell repr hash)
/// - Value: `StorageCell`
pub struct Cells;

impl ColumnFamily for Cells {
    const NAME: &'static str = "cells";

    fn read_options(opts: &mut ReadOptions) {
        opts.fill_cache(false);
        opts.set_readahead_size(0);
        opts.set_verify_checksums(false);
    }
}

impl ColumnFamilyOptions<TableContext> for Cells {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        opts.set_level_compaction_dynamic_level_bytes(true);
        // Kept for legacy/unresolved merge operands while old cells DBs migrate
        // to indexed values. New storage writes canonical indexed rows directly.
        opts.set_merge_operator_associative("cell_merge", refcount::merge_operator);

        let write_buffer_size = ByteSize::mib(512);
        let buffers_to_merge = 4;
        let buffer_count = 12;

        // Nursery absorbs short-lived cell churn, so flush survived cells before
        // accumulating multi-GiB WALs and immutable memtables.
        opts.set_write_buffer_size(write_buffer_size.as_u64() as usize);
        opts.set_min_write_buffer_number_to_merge(buffers_to_merge);
        opts.set_max_write_buffer_number(buffer_count);
        opts.set_max_write_buffer_size_to_maintain(0);

        ctx.track_buffer_usage(
            ByteSize(write_buffer_size.as_u64() * buffers_to_merge as u64),
            ByteSize(write_buffer_size.as_u64() * buffer_count as u64),
        );

        opts.set_memtable_prefix_bloom_ratio(0.005);
        opts.set_memtable_whole_key_filtering(true);

        let mut block_factory = BlockBasedOptions::default();

        // todo: some how make block cache separate for cells,
        block_factory.set_block_cache(&ctx.caches().block_cache);

        block_factory.set_cache_index_and_filter_blocks(true);
        block_factory.set_pin_top_level_index_and_filter(true);
        block_factory.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_factory.set_top_level_index_pinning_tier(BlockBasedTablePinningTier::All);
        block_factory.set_partition_pinning_tier(BlockBasedTablePinningTier::All);
        block_factory.set_unpartitioned_pinning_tier(BlockBasedTablePinningTier::All);

        block_factory.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
        block_factory.set_partition_filters(true);
        block_factory.set_metadata_block_size(ByteSize::kib(4).as_u64() as usize);

        block_factory.set_bloom_filter(12.0, false);
        block_factory.set_optimize_filters_for_memory(true);
        block_factory.set_whole_key_filtering(true);

        // to match fs block size
        block_factory.set_block_size(ByteSize::kib(4).as_u64() as usize);
        block_factory.set_format_version(6);

        // we have 4096 / 256 = 16 keys per block, so binary search is enough
        block_factory.set_data_block_index_type(DataBlockIndexType::BinarySearch);

        opts.set_block_based_table_factory(&block_factory);

        opts.set_compression_type(DBCompressionType::None);

        opts.set_compaction_pri(CompactionPri::MinOverlappingRatio);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_level_zero_slowdown_writes_trigger(8);
        opts.set_level_zero_stop_writes_trigger(12);

        opts.set_max_bytes_for_level_base(ByteSize::gib(16).as_u64());
        opts.set_max_bytes_for_level_multiplier(10.0);
        opts.set_target_file_size_base(ByteSize::gib(2).as_u64());
        opts.set_target_file_size_multiplier(1);
        opts.set_num_levels(6);
        opts.set_max_compaction_bytes(ByteSize::gib(32).as_u64());

        opts.set_optimize_filters_for_hits(false);

        opts.set_inplace_update_support(false);
    }
}

/// Stores temp cells data
/// - Key: `ton_types::UInt256` (cell repr hash)
/// - Value: `StorageCell`
pub struct TempCells;

impl ColumnFamily for TempCells {
    const NAME: &'static str = "temp_cells";
}

impl ColumnFamilyOptions<TableContext> for TempCells {
    fn options(opts: &mut rocksdb::Options, ctx: &mut TableContext) {
        let mut block_factory = BlockBasedOptions::default();
        block_factory.set_block_cache(&ctx.caches().block_cache);
        block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
        block_factory.set_whole_key_filtering(true);
        block_factory.set_checksum_type(rocksdb::ChecksumType::NoChecksum);

        block_factory.set_bloom_filter(10.0, false);
        block_factory.set_block_size(16 * 1024);
        block_factory.set_format_version(5);

        opts.set_optimize_filters_for_hits(true);
    }
}

/// Stores connections data
/// - Key: `BlockIdShort (16 bytes), [u8; 32] (block root hash), connection type (1 byte)`
/// - Value: `BlockId (LE)`
pub struct BlockConnections;

impl BlockConnections {
    pub const KEY_LEN: usize = 4 + 8 + 4 + 32 + 1;
}

impl ColumnFamily for BlockConnections {
    const NAME: &'static str = "block_connections";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

impl ColumnFamilyOptions<TableContext> for BlockConnections {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        default_block_based_table_factory(opts, ctx);
        optimize_for_point_lookup(opts, ctx);
    }
}

// === Old collator storage ===

// TODO should be deleted
pub struct ShardInternalMessagesOld;
impl ColumnFamily for ShardInternalMessagesOld {
    const NAME: &'static str = "shard_int_msgs";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<TableContext> for ShardInternalMessagesOld {
    fn options(_opts: &mut Options, _ctx: &mut TableContext) {}
}

// TODO should be deleted
pub struct ShardInternalMessagesUncommitedOld;
impl ColumnFamily for ShardInternalMessagesUncommitedOld {
    const NAME: &'static str = "shard_int_msgs_uncommited";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}
impl ColumnFamilyOptions<TableContext> for ShardInternalMessagesUncommitedOld {
    fn options(_opts: &mut Options, _ctx: &mut TableContext) {}
}

// TODO should be deleted
pub struct InternalMessageStatsOld;
impl ColumnFamily for InternalMessageStatsOld {
    const NAME: &'static str = "int_msg_stats";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<TableContext> for InternalMessageStatsOld {
    fn options(_opts: &mut Options, _ctx: &mut TableContext) {}
}

// TODO should be deleted
pub struct InternalMessageStatsUncommitedOld;
impl ColumnFamily for InternalMessageStatsUncommitedOld {
    const NAME: &'static str = "int_msg_stats_uncommited";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<TableContext> for InternalMessageStatsUncommitedOld {
    fn options(_opts: &mut Options, _ctx: &mut TableContext) {}
}

fn archive_data_merge(
    _: &[u8],
    current_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let total_len = current_value.map(|data| data.len()).unwrap_or_default()
        + operands.iter().map(|data| data.len()).sum::<usize>();

    let mut result = Vec::with_capacity(total_len);

    if let Some(current_value) = current_value {
        result.extend_from_slice(current_value);
    }

    for data in operands {
        result.extend_from_slice(data);
    }

    Some(result)
}

fn block_handle_merge(
    _: &[u8],
    current_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut value = [0u8; 12];
    if let Some(current_value) = current_value {
        value.copy_from_slice(current_value);
    }

    for operand in operands {
        assert_eq!(operand.len(), 12);
        for (a, b) in std::iter::zip(&mut value, operand) {
            *a |= *b;
        }
    }

    Some(value.to_vec())
}
