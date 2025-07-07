use bytesize::ByteSize;
use weedb::Caches;
use weedb::rocksdb::{BlockBasedOptions, DBCompressionType, DataBlockIndexType, Options};

use super::TableContext;

/// took from
/// <https://github.com/tikv/tikv/blob/d60c7fb6f3657dc5f3c83b0e3fc6ac75636e1a48/src/config/mod.rs#L170>
/// todo: need to benchmark and update if it's not optimal
pub const DEFAULT_MIN_BLOB_SIZE: u64 = bytesize::KIB * 32;

pub fn default_block_based_table_factory<C: AsRef<Caches>>(opts: &mut Options, ctx: &C) {
    let caches = ctx.as_ref();

    opts.set_level_compaction_dynamic_level_bytes(true);
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    block_factory.set_format_version(6);
    opts.set_block_based_table_factory(&block_factory);
}

/// Set our shared cache instead of individual caches for each cf.
pub fn optimize_for_point_lookup<C: AsRef<Caches>>(opts: &mut Options, ctx: &C) {
    let caches = ctx.as_ref();

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

pub fn optimize_for_level_compaction(
    opts: &mut Options,
    ctx: &TableContext,
    buffer_size: ByteSize,
    buffer_count: usize,
) {
    const MIN_BUFFERS_TO_MERGE: u64 = 2;

    let min_budget = buffer_size.0.saturating_mul(MIN_BUFFERS_TO_MERGE);
    let max_budget = buffer_size.0.saturating_mul(buffer_count as _);

    opts.set_write_buffer_size(buffer_size.0 as _);
    // allow early flush
    opts.set_min_write_buffer_number_to_merge(MIN_BUFFERS_TO_MERGE as _);
    // this means we'll use 50% extra memory in the worst case, but will reduce
    // write stalls.
    opts.set_max_write_buffer_number(buffer_count as _);
    // start flushing L0->L1 as soon as possible. each file on level0 is
    // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
    // memtable_memory_budget.
    opts.set_level_zero_file_num_compaction_trigger(2);
    // doesn't really matter much, but we don't want to create too many files
    opts.set_target_file_size_base(max_budget / 8);
    // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
    opts.set_max_bytes_for_level_base(max_budget);

    ctx.track_buffer_usage(ByteSize(min_budget), ByteSize(max_budget));
}

pub fn zstd_block_based_table_factory<C: AsRef<Caches>>(opts: &mut Options, ctx: &C) {
    let caches = ctx.as_ref();
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    opts.set_block_based_table_factory(&block_factory);
    opts.set_compression_type(DBCompressionType::Zstd);
}

pub fn with_blob_db(opts: &mut Options, min_value_size: u64, compression_type: DBCompressionType) {
    opts.set_enable_blob_files(true);
    opts.set_enable_blob_gc(true);

    opts.set_min_blob_size(min_value_size);
    opts.set_blob_compression_type(compression_type);
}
