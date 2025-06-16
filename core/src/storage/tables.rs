use bytesize::ByteSize;
use tycho_storage::refcount;
use tycho_storage::tables::{
    default_block_based_table_factory, optimize_for_level_compaction, optimize_for_point_lookup,
    with_blob_db, DEFAULT_MIN_BLOB_SIZE,
};
use weedb::rocksdb::{
    self, BlockBasedIndexType, BlockBasedOptions, CompactionPri, DBCompressionType,
    DataBlockIndexType, MemtableFactory, MergeOperands, Options, ReadOptions, SliceTransform,
};
use weedb::{Caches, ColumnFamily, ColumnFamilyOptions};

/// Stores prepared archives
/// - Key: `u32 (BE)` (archive id)
/// - Value: `Vec<u8>` (archive block ids)
pub struct ArchiveBlockIds;

impl ColumnFamily for ArchiveBlockIds {
    const NAME: &'static str = "archive_block_ids";
}

impl ColumnFamilyOptions<Caches> for ArchiveBlockIds {
    fn options(opts: &mut Options, caches: &mut Caches) {
        default_block_based_table_factory(opts, caches);
        optimize_for_level_compaction(opts, ByteSize::mib(512u64));

        opts.set_merge_operator_associative("archive_data_merge", archive_data_merge);
        // data is hardly compressible and dataset is small
        opts.set_compression_type(DBCompressionType::None);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE, DBCompressionType::None);
    }
}

/// Stores split archives
/// - Key: `u32 (BE)` (archive id) + `u64 (BE)` (chunk index)
/// - Value: `Vec<u8>` (archive data chunk)
pub struct Archives;

impl Archives {
    pub const KEY_LEN: usize = 4 + 8;
}

impl ColumnFamily for Archives {
    const NAME: &'static str = "archives";
}

impl ColumnFamilyOptions<Caches> for Archives {
    fn options(opts: &mut Options, caches: &mut Caches) {
        default_block_based_table_factory(opts, caches);
        optimize_for_level_compaction(opts, ByteSize::mib(512u64));

        // data is already compressed
        opts.set_compression_type(DBCompressionType::None);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE, DBCompressionType::None);

        opts.set_max_write_buffer_number(8); // 8 * 512MB = 4GB;
        opts.set_write_buffer_size(512 * 1024 * 1024); // 512 per memtable
        opts.set_min_write_buffer_number_to_merge(2); // allow early flush
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

impl ColumnFamilyOptions<Caches> for BlockHandles {
    fn options(opts: &mut Options, caches: &mut Caches) {
        optimize_for_level_compaction(opts, ByteSize::mib(512u64));

        let mut block_factory = BlockBasedOptions::default();
        block_factory.set_block_cache(&caches.block_cache);

        block_factory.set_index_type(BlockBasedIndexType::HashSearch);
        block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
        block_factory.set_format_version(5);

        opts.set_merge_operator_associative("block_handle_merge", block_handle_merge);
        opts.set_block_based_table_factory(&block_factory);
        optimize_for_point_lookup(opts, caches);
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

impl ColumnFamilyOptions<Caches> for KeyBlocks {}

/// Maps block id (partial) to file hash
pub struct FullBlockIds;

impl ColumnFamily for FullBlockIds {
    const NAME: &'static str = "full_block_ids";

    fn read_options(opts: &mut rocksdb::ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

impl ColumnFamilyOptions<Caches> for FullBlockIds {
    fn options(opts: &mut rocksdb::Options, caches: &mut Caches) {
        default_block_based_table_factory(opts, caches);
    }
}

/// Maps package entry id to entry data
/// - Key: `BlockIdShort (16 bytes), [u8; 32], package type (1 byte)` <=> (`PackageEntryKey`)
/// - Value: `Vec<u8>` (block/proof/queue diff data)
pub struct PackageEntries;

impl PackageEntries {
    pub const KEY_LEN: usize = 4 + 8 + 4 + 32 + 1;
}

impl ColumnFamily for PackageEntries {
    const NAME: &'static str = "package_entries";
}

impl ColumnFamilyOptions<Caches> for PackageEntries {
    fn options(opts: &mut Options, caches: &mut Caches) {
        default_block_based_table_factory(opts, caches);
        opts.set_compression_type(DBCompressionType::Zstd);

        opts.set_max_write_buffer_number(8); // 8 * 512MB = 4GB;
        opts.set_write_buffer_size(512 * 1024 * 1024); // 512 per memtable
        opts.set_min_write_buffer_number_to_merge(2); // allow early flush

        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE, DBCompressionType::Zstd);

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

/// Maps block id to compressed block data
/// - Key: `BlockIdShort (16 bytes), [u8; 32], chunk index (4 byte)`
/// - Value: `Vec<u8>`
pub struct BlockDataEntries;

impl BlockDataEntries {
    pub const KEY_LEN: usize = 4 + 8 + 4 + 32 + 4;
}

impl ColumnFamily for BlockDataEntries {
    const NAME: &'static str = "block_data_entries";
}

impl ColumnFamilyOptions<Caches> for BlockDataEntries {
    fn options(opts: &mut Options, caches: &mut Caches) {
        default_block_based_table_factory(opts, caches);
        optimize_for_level_compaction(opts, ByteSize::mib(512u64));

        // data is already compressed
        opts.set_compression_type(DBCompressionType::None);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE, DBCompressionType::None);
    }
}

/// Maps `BlockId` to root cell hash
/// - Key: `BlockId`
/// - Value: `[u8; 32]`
pub struct ShardStates;

impl ColumnFamily for ShardStates {
    const NAME: &'static str = "shard_states";
}

impl ColumnFamilyOptions<Caches> for ShardStates {
    fn options(opts: &mut Options, caches: &mut Caches) {
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
}

impl ColumnFamilyOptions<Caches> for Cells {
    fn options(opts: &mut Options, block_cache: &mut Caches) {
        opts.set_level_compaction_dynamic_level_bytes(true);

        opts.set_merge_operator_associative("cell_merge", refcount::merge_operator);
        opts.set_compaction_filter("cell_compaction", refcount::compaction_filter);

        // optimize for bulk inserts and single writer
        opts.set_max_write_buffer_number(8); // 8 * 512MB = 4GB
        opts.set_min_write_buffer_number_to_merge(2); // allow early flush
        opts.set_write_buffer_size(512 * 1024 * 1024); // 512 per memtable

        opts.set_max_successive_merges(0); // it will eat cpu, we are doing first merge in hashmap anyway.

        // - Write batch size: 500K entries
        // - Entry size: ~244 bytes (32 SHA + 8 seq + 192 value + 12 overhead)
        // - Memtable size: 512MB

        // 1. Entries per memtable = 512MB / 244B ≈ 2.2M entries
        // 2. Target bucket load factor = 10-12 entries per bucket (RocksDB recommendation)
        // 3. Bucket count = entries / target_load = 2.2M / 11 ≈ 200K
        opts.set_memtable_factory(MemtableFactory::HashLinkList {
            bucket_count: 200_000,
        });

        opts.set_memtable_prefix_bloom_ratio(0.1); // we use hash-based memtable so bloom filter is not that useful
        opts.set_bloom_locality(1); // Optimize bloom filter locality

        let mut block_factory = BlockBasedOptions::default();

        // todo: some how make block cache separate for cells,
        // using 3/4 of all available cache space
        block_factory.set_block_cache(&block_cache.block_cache);

        // 10 bits per key, stored at the end of the sst
        block_factory.set_bloom_filter(10.0, false);
        block_factory.set_optimize_filters_for_memory(true);
        block_factory.set_whole_key_filtering(true);

        // to match fs block size
        block_factory.set_block_size(4096);
        block_factory.set_format_version(6);

        // we have 4096 / 256 = 16 keys per block, so binary search is enough
        block_factory.set_data_block_index_type(DataBlockIndexType::BinarySearch);

        block_factory.set_index_type(BlockBasedIndexType::HashSearch);
        block_factory.set_pin_l0_filter_and_index_blocks_in_cache(true);

        opts.set_block_based_table_factory(&block_factory);
        opts.set_prefix_extractor(SliceTransform::create_noop());

        opts.set_memtable_whole_key_filtering(true);
        opts.set_memtable_prefix_bloom_ratio(0.25);

        opts.set_compression_type(DBCompressionType::None);

        opts.set_compaction_pri(CompactionPri::OldestSmallestSeqFirst);
        opts.set_level_zero_file_num_compaction_trigger(8);

        opts.set_target_file_size_base(512 * 1024 * 1024); // smaller files for more efficient GC

        opts.set_max_bytes_for_level_base(4 * 1024 * 1024 * 1024); // 4GB per level
        opts.set_max_bytes_for_level_multiplier(8.0);

        // 512MB per file; less files - less compactions
        opts.set_target_file_size_base(512 * 1024 * 1024);
        // L1: 4GB
        // L2: ~32GB
        // L3: ~256GB
        // L4: ~2TB
        opts.set_num_levels(5);

        opts.set_optimize_filters_for_hits(true);

        // we have our own cache and don't want `kcompactd` goes brrr scenario
        opts.set_use_direct_reads(true);
        opts.set_use_direct_io_for_flush_and_compaction(true);

        opts.add_compact_on_deletion_collector_factory(
            100, // N: examine 100 consecutive entries
            // Small enough window to detect local delete patterns
            // Large enough to avoid spurious compactions
            45, // D: trigger on 45 deletions in window
            // Balance between the space reclaim and compaction frequency
            // ~45% deletion density trigger
            0.5, /* deletion_ratio: trigger if 50% of a total file is deleted
                  * Backup trigger for overall file health
                  * Higher than window trigger to prefer local optimization */
        );

        // single writer optimizations
        opts.set_enable_write_thread_adaptive_yield(false);
        opts.set_allow_concurrent_memtable_write(false);
        opts.set_enable_pipelined_write(true);
        opts.set_inplace_update_support(false);
        opts.set_unordered_write(true); // we don't use snapshots
        opts.set_avoid_unnecessary_blocking_io(true); // schedule unnecessary IO in background;

        opts.set_auto_tuned_ratelimiter(
            256 * 1024 * 1024, // 256MB/s base rate
            100_000,           // 100ms refill (standard value)
            10,                // fairness (standard value)
        );

        opts.set_periodic_compaction_seconds(3600 * 24); // force compaction once a day
    }
}

/// Stores temp cells data
/// - Key: `ton_types::UInt256` (cell repr hash)
/// - Value: `StorageCell`
pub struct TempCells;

impl ColumnFamily for TempCells {
    const NAME: &'static str = "temp_cells";
}

impl ColumnFamilyOptions<Caches> for TempCells {
    fn options(opts: &mut rocksdb::Options, caches: &mut Caches) {
        let mut block_factory = BlockBasedOptions::default();
        block_factory.set_block_cache(&caches.block_cache);
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

impl ColumnFamilyOptions<Caches> for BlockConnections {
    fn options(opts: &mut Options, caches: &mut Caches) {
        default_block_based_table_factory(opts, caches);

        optimize_for_point_lookup(opts, caches);
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

impl ColumnFamilyOptions<Caches> for ShardInternalMessagesOld {
    fn options(_opts: &mut Options, _caches: &mut Caches) {}
}

// TODO should be deleted
pub struct ShardInternalMessagesUncommitedOld;
impl ColumnFamily for ShardInternalMessagesUncommitedOld {
    const NAME: &'static str = "shard_int_msgs_uncommited";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}
impl ColumnFamilyOptions<Caches> for ShardInternalMessagesUncommitedOld {
    fn options(_opts: &mut Options, _caches: &mut Caches) {}
}

// TODO should be deleted
pub struct InternalMessageStatsOld;
impl ColumnFamily for InternalMessageStatsOld {
    const NAME: &'static str = "int_msg_stats";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for InternalMessageStatsOld {
    fn options(_opts: &mut Options, _caches: &mut Caches) {}
}

// TODO should be deleted
pub struct InternalMessageStatsUncommitedOld;
impl ColumnFamily for InternalMessageStatsUncommitedOld {
    const NAME: &'static str = "int_msg_stats_uncommited";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for InternalMessageStatsUncommitedOld {
    fn options(_opts: &mut Options, _caches: &mut Caches) {}
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
