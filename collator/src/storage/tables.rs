use bytesize::ByteSize;
use tycho_storage::kv::{DEFAULT_MIN_BLOB_SIZE, TableContext, with_blob_db};
use weedb::rocksdb::{BlockBasedOptions, DBCompressionType, Options, ReadOptions};
use weedb::{ColumnFamily, ColumnFamilyOptions};

/// Stores persistent internal messages
pub struct ShardInternalMessages;

impl ColumnFamily for ShardInternalMessages {
    const NAME: &'static str = "shard_int_messages";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<TableContext> for ShardInternalMessages {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        internal_queue_options(opts, ctx);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE, DBCompressionType::None);
    }
}

//

pub struct InternalMessageStatistics;

impl ColumnFamily for InternalMessageStatistics {
    const NAME: &'static str = "int_msg_statistics";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<TableContext> for InternalMessageStatistics {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        internal_queue_options(opts, ctx);
    }
}

//

pub struct InternalMessageVar;

impl ColumnFamily for InternalMessageVar {
    const NAME: &'static str = "int_msg_var";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<TableContext> for InternalMessageVar {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        internal_queue_options(opts, ctx);
    }
}

//

pub struct InternalMessageDiffsTail;

impl ColumnFamily for InternalMessageDiffsTail {
    const NAME: &'static str = "int_msg_diffs_tail";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<TableContext> for InternalMessageDiffsTail {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        internal_queue_options(opts, ctx);
    }
}

//

pub struct InternalMessageDiffInfo;

impl ColumnFamily for InternalMessageDiffInfo {
    const NAME: &'static str = "int_msg_diff_info";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<TableContext> for InternalMessageDiffInfo {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        internal_queue_options(opts, ctx);
    }
}

//

pub struct InternalMessageCommitPointer;

impl ColumnFamily for InternalMessageCommitPointer {
    const NAME: &'static str = "int_msg_commit_pointer";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<TableContext> for InternalMessageCommitPointer {
    fn options(opts: &mut Options, ctx: &mut TableContext) {
        internal_queue_options(opts, ctx);
    }
}

// === Helpers ===

fn internal_queue_options(opts: &mut Options, ctx: &mut TableContext) {
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&ctx.caches().block_cache);
    block_factory.set_format_version(6);

    opts.set_block_based_table_factory(&block_factory);
    opts.set_disable_auto_compactions(false);
    opts.set_compression_type(DBCompressionType::None);
    opts.set_level_compaction_dynamic_level_bytes(true);

    opts.set_max_total_wal_size(300 * 1024 * 1024);

    let default_buffer = ByteSize::mib(64);
    let default_count = 3;
    let default_merge = 1;

    ctx.track_buffer_usage(
        ByteSize(default_buffer.as_u64() * default_merge as u64),
        ByteSize(default_buffer.as_u64() * default_count as u64),
    );
}
