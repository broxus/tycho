use tycho_storage::kv::NamedTables;
use weedb::{Caches, WeeDb};

use super::tables;

pub type InternalQueueDB = WeeDb<InternalQueueTables>;

impl NamedTables for InternalQueueTables {
    const NAME: &'static str = "int_queue";
}

// TODO: Add migrations.

weedb::tables! {
    pub struct InternalQueueTables<Caches> {
        pub internal_message_var: tables::InternalMessageVar,
        pub internal_message_diffs_tail: tables::InternalMessageDiffsTail,
        pub internal_message_diff_info: tables::InternalMessageDiffInfo,
        pub internal_message_commit_pointer: tables::InternalMessageCommitPointer,
        pub internal_message_stats: tables::InternalMessageStatistics,
        pub shard_internal_messages: tables::ShardInternalMessages,
    }
}
