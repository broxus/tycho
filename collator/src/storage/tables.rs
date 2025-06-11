use tycho_storage::tables::{with_blob_db, DEFAULT_MIN_BLOB_SIZE};
use weedb::rocksdb::{BlockBasedOptions, DBCompressionType, Options, ReadOptions};
use weedb::{Caches, ColumnFamily, ColumnFamilyOptions};

/// Stores persistent internal messages
pub struct ShardInternalMessages;
impl ColumnFamily for ShardInternalMessages {
    const NAME: &'static str = "shard_int_messages";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for ShardInternalMessages {
    fn options(opts: &mut Options, caches: &mut Caches) {
        internal_queue_options(opts, caches);
        with_blob_db(opts, DEFAULT_MIN_BLOB_SIZE, DBCompressionType::None);
    }
}

pub struct InternalMessageStatistics;
impl ColumnFamily for InternalMessageStatistics {
    const NAME: &'static str = "int_msg_statistics";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for InternalMessageStatistics {
    fn options(opts: &mut Options, caches: &mut Caches) {
        internal_queue_options(opts, caches);
    }
}

fn internal_queue_options(opts: &mut Options, caches: &mut Caches) {
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    block_factory.set_format_version(6);

    opts.set_block_based_table_factory(&block_factory);
    opts.set_disable_auto_compactions(true);
    opts.set_compression_type(DBCompressionType::None);

    opts.set_level_compaction_dynamic_level_bytes(true);

    // optimize for bulk inserts and single writer
    opts.set_max_write_buffer_number(2); // 2 * 256MB = 512MB
    opts.set_min_write_buffer_number_to_merge(2); // allow early flush
    opts.set_write_buffer_size(256 * 1024 * 1024); // 256 per memtable
}

pub struct InternalMessageVar;
impl ColumnFamily for InternalMessageVar {
    const NAME: &'static str = "int_msg_var";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for InternalMessageVar {
    fn options(opts: &mut Options, caches: &mut Caches) {
        internal_queue_options(opts, caches);
    }
}
pub struct InternalMessageDiffsTail;
impl ColumnFamily for InternalMessageDiffsTail {
    const NAME: &'static str = "int_msg_diffs_tail";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for InternalMessageDiffsTail {
    fn options(opts: &mut Options, caches: &mut Caches) {
        internal_queue_options(opts, caches);
    }
}

pub struct InternalMessageDiffInfo;
impl ColumnFamily for InternalMessageDiffInfo {
    const NAME: &'static str = "int_msg_diff_info";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for InternalMessageDiffInfo {
    fn options(opts: &mut Options, caches: &mut Caches) {
        internal_queue_options(opts, caches);
    }
}

pub struct InternalMessageCommitPointer;
impl ColumnFamily for InternalMessageCommitPointer {
    const NAME: &'static str = "int_msg_commit_pointer";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(true);
    }
}

impl ColumnFamilyOptions<Caches> for InternalMessageCommitPointer {
    fn options(opts: &mut Options, caches: &mut Caches) {
        internal_queue_options(opts, caches);
    }
}
