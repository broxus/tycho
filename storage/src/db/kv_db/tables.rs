use bytesize::ByteSize;
use weedb::rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, DBCompressionType, DataBlockIndexType, MergeOperands,
    Options, ReadOptions,
};
use weedb::{rocksdb, Caches, ColumnFamily, ColumnFamilyOptions};

use super::refcount;

// took from
// https://github.com/tikv/tikv/blob/d60c7fb6f3657dc5f3c83b0e3fc6ac75636e1a48/src/config/mod.rs#L170
// todo: need to benchmark and update if it's not optimal
const DEFAULT_MIN_BLOB_SIZE: u64 = bytesize::KIB * 32;

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

/// Stores prepared archives
/// - Key: `u32 (BE)` (archive id)
/// - Value: `Vec<u8>` (archive data)
pub struct IntermediateArchives;

impl ColumnFamily for IntermediateArchives {
    const NAME: &'static str = "intermediate_archives";
}

impl ColumnFamilyOptions<Caches> for IntermediateArchives {
    fn options(opts: &mut Options, caches: &mut Caches) {
        default_block_based_table_factory(opts, caches);
        optimize_for_level_compaction(opts, ByteSize::mib(512u64));

        opts.set_merge_operator_associative("archive_data_merge", archive_data_merge);
        opts.set_compression_type(DBCompressionType::Zstd);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE);
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

        opts.set_compression_type(DBCompressionType::Zstd);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE);
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

/// Maps package entry id to entry data
/// - Key: `BlockIdShort (16 bytes), [u8; 32], package type (1 byte)`
/// - Value: `Vec<u8>`
pub struct PackageEntries;

impl ColumnFamily for PackageEntries {
    const NAME: &'static str = "package_entries";
}

impl ColumnFamilyOptions<Caches> for PackageEntries {
    fn options(opts: &mut Options, caches: &mut Caches) {
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
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE);
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
    fn options(opts: &mut Options, caches: &mut Caches) {
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

/// Stores connections data
/// - Key: `[u8; 32]` (block root hash)
/// - Value: `BlockId (LE)`
pub struct InternalMessages;
impl ColumnFamily for InternalMessages {
    const NAME: &'static str = "internal_messages";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for InternalMessages {
    fn options(opts: &mut Options, caches: &mut Caches) {
        zstd_block_based_table_factory(opts, caches);
    }
}

/// Stores persistent internal messages
pub struct ShardsInternalMessages;
impl ColumnFamily for ShardsInternalMessages {
    const NAME: &'static str = "shards_internal_messages";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for ShardsInternalMessages {
    fn options(opts: &mut Options, caches: &mut Caches) {
        zstd_block_based_table_factory(opts, caches);
    }
}

/// Stores connections data
pub struct ShardsInternalMessagesSession;
impl ColumnFamily for ShardsInternalMessagesSession {
    const NAME: &'static str = "shards_internal_messages_session";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for ShardsInternalMessagesSession {
    fn options(opts: &mut Options, caches: &mut Caches) {
        zstd_block_based_table_factory(opts, caches);
    }
}

fn archive_data_merge(
    _: &[u8],
    current_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    use tycho_block_util::archive::ARCHIVE_PREFIX;

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

// === JRPC tables ===

/// Stores raw transactions
/// - Key: `workchain: i8, account: [u8; 32], lt: u64`
/// - Value: `transaction BOC`
pub struct Transactions;

impl Transactions {
    pub const KEY_LEN: usize = 1 + 32 + 8;
}

impl ColumnFamily for Transactions {
    const NAME: &'static str = "transactions";
}

impl ColumnFamilyOptions<Caches> for Transactions {
    fn options(opts: &mut Options, caches: &mut Caches) {
        zstd_block_based_table_factory(opts, caches);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE);
    }
}

/// Transaction hash to full key
/// - Key: `tx_hash: [u8; 32]`
/// - Value: `workchain: i8, account: [u8; 32], lt: u64`
pub struct TransactionsByHash;

impl ColumnFamily for TransactionsByHash {
    const NAME: &'static str = "transactions_by_hash";
}

impl ColumnFamilyOptions<Caches> for TransactionsByHash {
    fn options(opts: &mut Options, caches: &mut Caches) {
        zstd_block_based_table_factory(opts, caches);
    }
}

/// Inbound message hash to full transaction key
/// - Key: `msg_hash: [u8; 32]`
/// - Value: `workchain: i8, account: [u8; 32], lt: u64`
pub struct TransactionsByInMsg;

impl ColumnFamily for TransactionsByInMsg {
    const NAME: &'static str = "transactions_by_in_msg";
}

impl ColumnFamilyOptions<Caches> for TransactionsByInMsg {
    fn options(opts: &mut Options, caches: &mut Caches) {
        zstd_block_based_table_factory(opts, caches);
    }
}

/// Code hash with account address
/// - Key: `code_hash: [u8; 32], workchain: i8, account: [u8; 32]`
/// - Value: empty
pub struct CodeHashes;

impl CodeHashes {
    pub const KEY_LEN: usize = 32 + 1 + 32;
}

impl ColumnFamily for CodeHashes {
    const NAME: &'static str = "code_hashes";
}

impl ColumnFamilyOptions<Caches> for CodeHashes {
    fn options(opts: &mut Options, caches: &mut Caches) {
        zstd_block_based_table_factory(opts, caches);
    }
}

/// Account address to code hash
/// - Key: `workchain: i8, account: [u8; 32]`
/// - Value: `code_hash: [u8; 32]`
pub struct CodeHashesByAddress;

impl CodeHashesByAddress {
    pub const KEY_LEN: usize = 1 + 32;
}

impl ColumnFamily for CodeHashesByAddress {
    const NAME: &'static str = "code_hashes_by_address";
}

impl ColumnFamilyOptions<Caches> for CodeHashesByAddress {
    fn options(opts: &mut Options, caches: &mut Caches) {
        zstd_block_based_table_factory(opts, caches);
    }
}

/// Stores mempool point data
/// - Key: `round: u32, digest: [u8; 32]`
/// - Value: `Point`
pub struct Points;

impl ColumnFamily for Points {
    const NAME: &'static str = "points";
}

impl ColumnFamilyOptions<Caches> for Points {
    fn options(opts: &mut Options, caches: &mut Caches) {
        optimize_for_point_lookup(opts, caches);
        opts.set_disable_auto_compactions(true);

        opts.set_enable_blob_files(true);
        opts.set_enable_blob_gc(false); // manual
        opts.set_min_blob_size(DEFAULT_MIN_BLOB_SIZE);
        opts.set_blob_compression_type(DBCompressionType::None);
    }
}

/// Stores truncated mempool point data
/// - Key: `round: u32, digest: [u8; 32]`
/// - Value: `PointInfo`
pub struct PointsInfo;

impl ColumnFamily for PointsInfo {
    const NAME: &'static str = "points_info";
}

impl ColumnFamilyOptions<Caches> for PointsInfo {
    fn options(opts: &mut Options, caches: &mut Caches) {
        optimize_for_point_lookup(opts, caches);
        opts.set_disable_auto_compactions(true);
    }
}

/// Stores mempool point flags
/// - Key: `round: u32, digest: [u8; 32]` as in [`Points`]
/// - Value: [`crate::models::PointFlags`]
pub struct PointsFlags;

impl PointsFlags {}

impl ColumnFamily for PointsFlags {
    const NAME: &'static str = "points_flags";
}

impl ColumnFamilyOptions<Caches> for PointsFlags {
    fn options(opts: &mut Options, caches: &mut Caches) {
        optimize_for_point_lookup(opts, caches);
        opts.set_disable_auto_compactions(true);

        opts.set_merge_operator_associative("points_flags_merge", crate::models::PointFlags::merge);
    }
}

fn zstd_block_based_table_factory(opts: &mut Options, caches: &Caches) {
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    opts.set_block_based_table_factory(&block_factory);
    opts.set_compression_type(DBCompressionType::Zstd);
}

fn with_blob_db(opts: &mut Options, min_value_size: u64) {
    opts.set_enable_blob_files(true);
    opts.set_enable_blob_gc(true);

    opts.set_min_blob_size(min_value_size);
    opts.set_blob_compression_type(DBCompressionType::Zstd);
}
