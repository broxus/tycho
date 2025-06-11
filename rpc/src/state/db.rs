use tycho_storage::{Migrations, WithMigrations};
use tycho_util::sync::CancellationFlag;
use weedb::{Caches, MigrationError, Semver, WeeDb};

use super::tables;

pub type RpcDb = WeeDb<RpcTables>;

impl WithMigrations for RpcTables {
    const NAME: &'static str = "rpc";
    const VERSION: Semver = [0, 1, 0];

    fn register_migrations(
        _migrations: &mut Migrations<Self>,
        _cancelled: CancellationFlag,
    ) -> Result<(), MigrationError> {
        // migrations.register([0, 0, 1], [0, 0, 2], move |db| Ok(()))?;

        Ok(())
    }
}

weedb::tables! {
    pub struct RpcTables<Caches> {
        pub state: tycho_storage::tables::State,
        pub transactions: tables::Transactions,
        pub transactions_by_hash: tables::TransactionsByHash,
        pub transactions_by_in_msg: tables::TransactionsByInMsg,
        pub known_blocks: tables::KnownBlocks,
        pub block_transactions: tables::BlockTransactions,
        pub blocks_by_mc_seqno: tables::BlocksByMcSeqno,
        pub code_hashes: tables::CodeHashes,
        pub code_hashes_by_address: tables::CodeHashesByAddress,
    }
}
