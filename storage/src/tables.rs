use bytesize::ByteSize;
use weedb::rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, DBCompressionType, DataBlockIndexType, MergeOperands,
    Options, ReadOptions,
};
use weedb::{rocksdb, Caches, ColumnFamily};

use super::refcount;

/// Stores prepared archives
/// - Key: `u32 (BE)` (archive id)
/// - Value: `Vec<u8>` (archive data)
pub struct Archives;
impl ColumnFamily for Archives {
    const NAME: &'static str = "archives";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
        optimize_for_level_compaction(opts, ByteSize::mib(512u64));

        opts.set_merge_operator_associative("archive_data_merge", archive_data_merge);
        opts.set_compression_type(DBCompressionType::Zstd);
    }
}

/// Maps block root hash to block meta
/// - Key: `[u8; 32]`
/// - Value: `BlockMeta`
pub struct BlockHandles;
impl ColumnFamily for BlockHandles {
    const NAME: &'static str = "block_handles";

    fn options(opts: &mut Options, caches: &Caches) {
        optimize_for_level_compaction(opts, ByteSize::mib(512u64));

        let mut block_factory = BlockBasedOptions::default();
        block_factory.set_block_cache(&caches.block_cache);

        block_factory.set_index_type(BlockBasedIndexType::HashSearch);
        block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
        block_factory.set_format_version(5);

        opts.set_block_based_table_factory(&block_factory);
        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Maps seqno to key block id
/// - Key: `u32 (BE)`
/// - Value: `ton_block::BlockIdExt`
pub struct KeyBlocks;
impl ColumnFamily for KeyBlocks {
    const NAME: &'static str = "key_blocks";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Maps package entry id to entry data
/// - Key: `BlockIdShort (16 bytes), [u8; 32], package type (1 byte)`
/// - Value: `Vec<u8>`
pub struct PackageEntries;
impl ColumnFamily for PackageEntries {
    const NAME: &'static str = "package_entries";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
        opts.set_compression_type(DBCompressionType::Zstd);

        // This flag specifies that the implementation should optimize the filters
        // mainly for cases where keys are found rather than also optimize for keys
        // missed. This would be used in cases where the application knows that
        // there are very few misses or the performance in the case of misses is not
        // important.
        //
        // For now, this flag allows us to not store filters for the last level i.e
        // the largest level which contains data of the LSM store. For keys which
        // are hits, the filters in this level are not useful because we will search
        // for the data anyway. NOTE: the filters in other levels are still useful
        // even for key hit because they tell us whether to look in that level or go
        // to the higher level.
        // https://github.com/facebook/rocksdb/blob/81aeb15988e43c49952c795e32e5c8b224793589/include/rocksdb/advanced_options.h#L846
        opts.set_optimize_filters_for_hits(true);
    }
}

/// Maps BlockId to root cell hash
/// - Key: `BlockId`
/// - Value: `[u8; 32]`
pub struct ShardStates;
impl ColumnFamily for ShardStates {
    const NAME: &'static str = "shard_states";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
        opts.set_compression_type(DBCompressionType::Zstd);
    }
}

/// Stores cells data
/// - Key: `[u8; 32]` (cell repr hash)
/// - Value: `StorageCell`
pub struct Cells;
impl ColumnFamily for Cells {
    const NAME: &'static str = "cells";

    fn options(opts: &mut Options, caches: &Caches) {
        opts.set_level_compaction_dynamic_level_bytes(true);

        opts.set_merge_operator_associative("cell_merge", refcount::merge_operator);
        opts.set_compaction_filter("cell_compaction", refcount::compaction_filter);

        optimize_for_level_compaction(opts, ByteSize::gib(1u64));

        let mut block_factory = BlockBasedOptions::default();
        block_factory.set_block_cache(&caches.block_cache);
        block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
        block_factory.set_whole_key_filtering(true);
        block_factory.set_checksum_type(rocksdb::ChecksumType::NoChecksum);

        block_factory.set_bloom_filter(10.0, false);
        block_factory.set_block_size(16 * 1024);
        block_factory.set_format_version(5);

        opts.set_block_based_table_factory(&block_factory);
        opts.set_optimize_filters_for_hits(true);
        // option is set for cf
        opts.set_compression_type(DBCompressionType::Lz4);
    }
}

/// Stores generic node parameters
/// - Key: `...`
/// - Value: `...`
pub struct NodeStates;
impl ColumnFamily for NodeStates {
    const NAME: &'static str = "node_states";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        opts.set_optimize_filters_for_hits(true);
        optimize_for_point_lookup(opts, caches);
    }
}

/// Stores connections data
/// - Key: `[u8; 32]` (block root hash)
/// - Value: `BlockId (LE)`
pub struct Prev1;
impl ColumnFamily for Prev1 {
    const NAME: &'static str = "prev1";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Stores connections data
/// - Key: `[u8; 32]` (block root hash)
/// - Value: `BlockId (LE)`
pub struct Prev2;
impl ColumnFamily for Prev2 {
    const NAME: &'static str = "prev2";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Stores connections data
/// - Key: `[u8; 32]` (block root hash)
/// - Value: `BlockId (LE)`
pub struct Next1;
impl ColumnFamily for Next1 {
    const NAME: &'static str = "next1";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Stores connections data
/// - Key: `[u8; 32]` (block root hash)
/// - Value: `BlockId (LE)`
pub struct Next2;
impl ColumnFamily for Next2 {
    const NAME: &'static str = "next2";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

fn archive_data_merge(
    _: &[u8],
    current_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    use tycho_constansts::archive::ARCHIVE_PREFIX;

    let total_len: usize = operands.iter().map(|data| data.len()).sum();
    let mut result = Vec::with_capacity(ARCHIVE_PREFIX.len() + total_len);

    result.extend_from_slice(current_value.unwrap_or(&ARCHIVE_PREFIX));

    for data in operands {
        let data = data.strip_prefix(&ARCHIVE_PREFIX).unwrap_or(data);
        result.extend_from_slice(data);
    }

    Some(result)
}

fn default_block_based_table_factory(opts: &mut Options, caches: &Caches) {
    opts.set_level_compaction_dynamic_level_bytes(true);
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    block_factory.set_format_version(5);
    opts.set_block_based_table_factory(&block_factory);
}

// setting our shared cache instead of individual caches for each cf
fn optimize_for_point_lookup(opts: &mut Options, caches: &Caches) {
    //     https://github.com/facebook/rocksdb/blob/81aeb15988e43c49952c795e32e5c8b224793589/options/options.cc
    //     BlockBasedTableOptions block_based_options;
    //     block_based_options.data_block_index_type =
    //         BlockBasedTableOptions::kDataBlockBinaryAndHash;
    //     block_based_options.data_block_hash_table_util_ratio = 0.75;
    //     block_based_options.filter_policy.reset(NewBloomFilterPolicy(10));
    //     block_based_options.block_cache =
    //         NewLRUCache(static_cast<size_t>(block_cache_size_mb * 1024 * 1024));
    //     table_factory.reset(new BlockBasedTableFactory(block_based_options));
    //     memtable_prefix_bloom_size_ratio = 0.02;
    //     memtable_whole_key_filtering = true;
    //
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
    block_factory.set_data_block_hash_ratio(0.75);
    block_factory.set_bloom_filter(10.0, false);
    block_factory.set_block_cache(&caches.block_cache);
    opts.set_block_based_table_factory(&block_factory);

    opts.set_memtable_prefix_bloom_ratio(0.02);
    opts.set_memtable_whole_key_filtering(true);
}

fn optimize_for_level_compaction(opts: &mut Options, budget: ByteSize) {
    opts.set_write_buffer_size(budget.as_u64() as usize / 4);
    // this means we'll use 50% extra memory in the worst case, but will reduce
    //  write stalls.
    opts.set_min_write_buffer_number_to_merge(2);
    // this means we'll use 50% extra memory in the worst case, but will reduce
    // write stalls.
    opts.set_max_write_buffer_number(6);
    // start flushing L0->L1 as soon as possible. each file on level0 is
    // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
    // memtable_memory_budget.
    opts.set_level_zero_file_num_compaction_trigger(2);
    // doesn't really matter much, but we don't want to create too many files
    opts.set_target_file_size_base(budget.as_u64() / 8);
    // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
    opts.set_max_bytes_for_level_base(budget.as_u64());
}
