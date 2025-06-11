use tycho_storage::{Migrations, WithMigrations};
use tycho_util::sync::CancellationFlag;
use weedb::{Caches, MigrationError, Semver, WeeDb};

use super::tables;

pub type InternalQueueDB = WeeDb<InternalQueueTables>;

impl WithMigrations for InternalQueueTables {
    const NAME: &'static str = "int_queue";
    const VERSION: Semver = [0, 0, 1];

    fn register_migrations(
        _migrations: &mut Migrations<Self>,
        _cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        // TODO: register migrations here
        Ok(())
    }
}

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
