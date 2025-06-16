use bytesize::ByteSize;
use weedb::rocksdb::{BlockBasedOptions, DBCompressionType, DataBlockIndexType, Options};
use weedb::{Caches, ColumnFamily, ColumnFamilyOptions};

// took from
// https://github.com/tikv/tikv/blob/d60c7fb6f3657dc5f3c83b0e3fc6ac75636e1a48/src/config/mod.rs#L170
// todo: need to benchmark and update if it's not optimal
pub const DEFAULT_MIN_BLOB_SIZE: u64 = bytesize::KIB * 32;

/// Stores generic node parameters
/// - Key: `...`
/// - Value: `...`
pub struct State;

impl ColumnFamily for State {
    const NAME: &'static str = "state";
}

impl ColumnFamilyOptions<Caches> for State {
    fn options(opts: &mut Options, caches: &mut Caches) {
        default_block_based_table_factory(opts, caches);

        opts.set_optimize_filters_for_hits(true);
        optimize_for_point_lookup(opts, caches);
    }
}

// === Base tables ===

// === Helpers ===

pub fn default_block_based_table_factory(opts: &mut Options, caches: &Caches) {
    opts.set_level_compaction_dynamic_level_bytes(true);
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    block_factory.set_format_version(6);
    opts.set_block_based_table_factory(&block_factory);
}

// setting our shared cache instead of individual caches for each cf
pub fn optimize_for_point_lookup(opts: &mut Options, caches: &Caches) {
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

pub fn optimize_for_level_compaction(opts: &mut Options, budget: ByteSize) {
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

pub fn zstd_block_based_table_factory(opts: &mut Options, caches: &Caches) {
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
