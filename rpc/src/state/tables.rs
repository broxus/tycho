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

/// Transaction hash router keyed by the 32-byte transaction hash.
///
/// Values use `codec::RouterLocation` and contain the partition id and related masterchain seqno.
pub struct TransactionRouter;

impl ColumnFamily for TransactionRouter {
    const NAME: &'static str = "transaction_router";
}

impl ColumnFamilyOptions<TableContext> for TransactionRouter {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
        optimize_for_point_lookup(opts, ctx);
    }
}

/// Inbound message hash router keyed by the 32-byte inbound-message hash.
///
/// Values use `codec::RouterLocation` and contain the partition id and related masterchain seqno.
pub struct InboundMessageRouter;

impl ColumnFamily for InboundMessageRouter {
    const NAME: &'static str = "inbound_message_router";
}

impl ColumnFamilyOptions<TableContext> for InboundMessageRouter {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        zstd_block_based_table_factory(opts, ctx);
        optimize_for_point_lookup(opts, ctx);
    }
}

/// Short block id router keyed by `codec::encode_short_block_id`, including every indexed masterchain block.
///
/// Values use `codec::RouterLocation` and contain the partition id and related masterchain seqno.
pub struct BlockRouter;

impl ColumnFamily for BlockRouter {
    const NAME: &'static str = "block_router";
}

impl ColumnFamilyOptions<TableContext> for BlockRouter {
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
