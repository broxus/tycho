use tycho_storage::kv::{NamedTables, TableContext};
use weedb::WeeDb;

use super::tables;

pub type RpcControlDb = WeeDb<RpcControlTables>;
pub type RpcRouterDb = WeeDb<RpcRouterTables>;
pub type RpcCurrentStateDb = WeeDb<RpcCurrentStateTables>;
pub type RpcTransactionsDb = WeeDb<RpcTransactionsTables>;

impl NamedTables for RpcControlTables {
    const NAME: &'static str = "rpc-control";
}

impl NamedTables for RpcRouterTables {
    const NAME: &'static str = "rpc-router";
}

impl NamedTables for RpcCurrentStateTables {
    const NAME: &'static str = "rpc-current-state";
}

impl NamedTables for RpcTransactionsTables {
    const NAME: &'static str = "rpc-transactions";
}

weedb::tables! {
    pub struct RpcControlTables<TableContext> {
        pub state: tables::State,
        pub manifests: tables::PartitionManifests,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writable_table_groups_have_distinct_metric_names() {
        assert_eq!(RpcControlTables::NAME, "rpc-control");
        assert_eq!(RpcRouterTables::NAME, "rpc-router");
        assert_eq!(RpcCurrentStateTables::NAME, "rpc-current-state");
        assert_eq!(RpcTransactionsTables::NAME, "rpc-transactions");
    }
}

weedb::tables! {
    pub struct RpcRouterTables<TableContext> {
        pub transactions: tables::TransactionRouter,
        pub inbound_messages: tables::InboundMessageRouter,
        pub blocks: tables::BlockRouter,
    }
}

weedb::tables! {
    pub struct RpcCurrentStateTables<TableContext> {
        pub state: tables::State,
        pub code_hashes: tables::CodeHashes,
        pub code_hashes_by_address: tables::CodeHashesByAddress,
    }
}

weedb::tables! {
    pub struct RpcTransactionsTables<TableContext> {
        pub transactions: tables::Transactions,
        pub transactions_by_hash: tables::TransactionsByHash,
        pub transactions_by_in_msg: tables::TransactionsByInMsg,
        pub known_blocks: tables::KnownBlocks,
        pub block_transactions: tables::BlockTransactions,
        pub blocks_by_mc_seqno: tables::BlocksByMcSeqno,
        pub partition_commits: tables::PartitionCommits,
    }
}
