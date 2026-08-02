use tycho_storage::kv::{
    DEFAULT_MIN_BLOB_SIZE, TableContext, default_block_based_table_factory,
    optimize_for_point_lookup, with_blob_db, zstd_block_based_table_factory,
};
use weedb::rocksdb::{DBCompressionType, Options};
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

/// Partition lifecycle manifest keyed by a big-endian eight-byte partition id.
pub struct PartitionManifests;

impl ColumnFamily for PartitionManifests {
    const NAME: &'static str = "partition_manifests";
}

impl ColumnFamilyOptions<TableContext> for PartitionManifests {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Immutable filter bundle descriptors keyed by a big-endian eight-byte partition id.
pub struct FilterCatalog;

impl ColumnFamily for FilterCatalog {
    const NAME: &'static str = "filter_catalog";
}

impl ColumnFamilyOptions<TableContext> for FilterCatalog {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
        optimize_for_point_lookup(opts, ctx);
    }
}

/// Block write statistics keyed by `codec::partition_commit_key`.
pub struct PartitionCommits;

impl ColumnFamily for PartitionCommits {
    const NAME: &'static str = "partition_commits";
}

impl ColumnFamilyOptions<TableContext> for PartitionCommits {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Stores raw transactions
/// - Key: `workchain: i8, account: [u8; 32], lt: u64`
/// - Value: `related mc_seqno: u32 (BE), transaction mask: u8, transaction hash: [u8; 32],
///   optional message hash: [u8; 32], transaction BOC`; use `codec::TransactionValueRef`.
pub struct Transactions;

impl Transactions {
    pub const KEY_LEN: usize = 1 + 32 + 8;
}

impl ColumnFamily for Transactions {
    const NAME: &'static str = "transactions";
}

impl ColumnFamilyOptions<TableContext> for Transactions {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
        opts.set_compression_type(DBCompressionType::Zstd);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE, DBCompressionType::Zstd);
    }
}

/// Accounts with at least one indexed transaction in this partition.
/// - Key: `workchain: i8, account: [u8; 32]`
/// - Value: empty
pub struct Accounts;

impl Accounts {
    pub const KEY_LEN: usize = 1 + 32;
}

impl ColumnFamily for Accounts {
    const NAME: &'static str = "accounts";
}

impl ColumnFamilyOptions<TableContext> for Accounts {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Transaction hash to full key
/// - Key: `tx_hash: [u8; 32]`
/// - Value: `workchain: i8, account: [u8; 32], lt: u64 (BE), shard_depth: u8, seqno: u32 (LE), root_hash: [u8; 32], file_hash: [u8; 32], mc_seqno: u32 (LE)`
pub struct TransactionsByHash;

impl TransactionsByHash {
    pub const VALUE_FULL_LEN: usize = Transactions::KEY_LEN + 1 + 4 + 32 + 32 + 4;
}

impl ColumnFamily for TransactionsByHash {
    const NAME: &'static str = "transactions_by_hash";
}

impl ColumnFamilyOptions<TableContext> for TransactionsByHash {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Inbound message hash to full transaction key
/// - Key: `msg_hash: [u8; 32]`
/// - Value: `workchain: i8, account: [u8; 32], lt: u64`
pub struct TransactionsByInMsg;

impl ColumnFamily for TransactionsByInMsg {
    const NAME: &'static str = "transactions_by_in_msg";
}

impl ColumnFamilyOptions<TableContext> for TransactionsByInMsg {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Processed blocks (to distinguish "no block" from "no block transactions")
/// - Key: `workchain: i8, shard: u64 (BE), seqno: u32 (BE)`
/// - Value: `root_hash: [u8; 32], file_hash: [u8; 32], mc_seqno: u32 (LE), info_version: u8, info...`
pub struct KnownBlocks;

impl KnownBlocks {
    pub const KEY_LEN: usize = 1 + 8 + 4;
}

impl ColumnFamily for KnownBlocks {
    const NAME: &'static str = "known_blocks";
}

impl ColumnFamilyOptions<TableContext> for KnownBlocks {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Transactions grouped by short block id.
/// - Key: `workchain: i8, shard: u64 (BE), seqno: u32 (BE), account: [u8; 32], lt: u64 (BE)`.
/// - Value: `tx_hash: [u8; 32]`.
pub struct BlockTransactions;

impl BlockTransactions {
    pub const KEY_LEN: usize = 1 + 8 + 4 + 32 + 8;
}

impl ColumnFamily for BlockTransactions {
    const NAME: &'static str = "block_transactions";
}

impl ColumnFamilyOptions<TableContext> for BlockTransactions {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Blocks by logical time.
///
/// - Key: `mc_seqno: u32 (BE), workchain: i8, shard: u64 (BE), seqno: u32 (BE)`
/// - Value: `root_hash: [u8; 32], file_hash: [u8; 32], start_lt: u64 (LE), end_lt: (LE)`
///
/// # Additional value for masterchain
/// - `shard_count: u32 (LE), shard_count * (workchain: i8, shard: u64 (LE), seqno: u32 (LE)),
///   root_hash: [u8; 32], file_hash: [u8; 32], start_lt: u64 (LE), end_lt: (LE))`
pub struct BlocksByMcSeqno;

impl BlocksByMcSeqno {
    pub const KEY_LEN: usize = 4 + 1 + 8 + 4;
    pub const VALUE_LEN: usize = 32 + 32 + 8 + 8;

    pub const DESCR_OFFSET: usize = Self::VALUE_LEN + 4;
    pub const DESCR_LEN: usize = 1 + 8 + 4 + Self::VALUE_LEN;
}

impl ColumnFamily for BlocksByMcSeqno {
    const NAME: &'static str = "blocks_by_mc_seqno";
}

impl ColumnFamilyOptions<TableContext> for BlocksByMcSeqno {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Monolithic tail transaction payloads keyed by `account || lt`.
pub struct TailTransactions;

impl ColumnFamily for TailTransactions {
    const NAME: &'static str = "transactions";
}

impl ColumnFamilyOptions<TableContext> for TailTransactions {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
        opts.set_compression_type(DBCompressionType::Zstd);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE, DBCompressionType::Zstd);
    }
}

/// Monolithic tail account directory keyed by `account || lt`.
pub struct TailTransactionsByAccount;

impl ColumnFamily for TailTransactionsByAccount {
    const NAME: &'static str = "transactions_by_account";
}

impl ColumnFamilyOptions<TableContext> for TailTransactionsByAccount {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Monolithic tail hash directory keyed by transaction hash.
pub struct TailTransactionsByHash;

impl ColumnFamily for TailTransactionsByHash {
    const NAME: &'static str = "transactions_by_hash";
}

impl ColumnFamilyOptions<TableContext> for TailTransactionsByHash {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
        optimize_for_point_lookup(opts, ctx);
    }
}

/// Monolithic tail inbound-message directory keyed by message hash.
pub struct TailTransactionsByInMsg;

impl ColumnFamily for TailTransactionsByInMsg {
    const NAME: &'static str = "transactions_by_in_msg";
}

impl ColumnFamilyOptions<TableContext> for TailTransactionsByInMsg {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
        optimize_for_point_lookup(opts, ctx);
    }
}

/// Logical retirement queue keyed by `dead_generation || payload_key`.
pub struct TailRetiredByGeneration;

impl ColumnFamily for TailRetiredByGeneration {
    const NAME: &'static str = "retired_by_generation";
}

impl ColumnFamilyOptions<TableContext> for TailRetiredByGeneration {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}

/// Authoritative evacuation progress keyed by target generation.
pub struct TailGenerationProgress;

impl ColumnFamily for TailGenerationProgress {
    const NAME: &'static str = "generation_progress";
}

impl ColumnFamilyOptions<TableContext> for TailGenerationProgress {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
        optimize_for_point_lookup(opts, ctx);
    }
}

/// Terminal generation certificates and the fixed tail identity record.
pub struct TailGenerationCommits;

impl ColumnFamily for TailGenerationCommits {
    const NAME: &'static str = "generation_commits";
}

impl ColumnFamilyOptions<TableContext> for TailGenerationCommits {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
        optimize_for_point_lookup(opts, ctx);
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

impl ColumnFamilyOptions<TableContext> for CodeHashes {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
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

impl ColumnFamilyOptions<TableContext> for CodeHashesByAddress {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
    }
}
