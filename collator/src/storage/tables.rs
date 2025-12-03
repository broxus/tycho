use bytesize::ByteSize;
use tycho_storage::kv::{default_block_based_table_factory, DEFAULT_MIN_BLOB_SIZE, optimize_for_point_lookup, TableContext, with_blob_db};
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
        default_block_based_table_factory(opts, ctx);
        opts.set_optimize_filters_for_hits(true);
        optimize_for_point_lookup(opts, ctx);
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
        default_block_based_table_factory(opts, ctx);
        opts.set_optimize_filters_for_hits(true);
        optimize_for_point_lookup(opts, ctx);
    }
}


// === Helpers ===

fn internal_queue_options(opts: &mut Options, ctx: &mut TableContext) {
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&ctx.caches().block_cache);
    block_factory.set_format_version(6);

    opts.set_block_based_table_factory(&block_factory);
    opts.set_disable_auto_compactions(true);
    opts.set_compression_type(DBCompressionType::None);

    opts.set_level_compaction_dynamic_level_bytes(true);

    // optimize for bulk inserts and single writer
    let buffer_size = ByteSize::mib(256);
    let buffers_to_merge = 2;
    let buffer_count = 2;
    opts.set_write_buffer_size(buffer_size.as_u64() as _);
    opts.set_max_write_buffer_number(buffer_count);
    opts.set_min_write_buffer_number_to_merge(buffers_to_merge); // allow early flush
    ctx.track_buffer_usage(
        ByteSize(buffer_size.as_u64() * buffers_to_merge as u64),
        ByteSize(buffer_size.as_u64() * buffer_count as u64),
    );
}
