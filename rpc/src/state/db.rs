use tycho_storage::kv::{NamedTables, TableContext};
use weedb::WeeDb;

use super::tables;

pub type RpcControlDb = WeeDb<RpcControlTables>;
pub type RpcFilterCatalogDb = WeeDb<RpcFilterCatalogTables>;
pub type RpcCurrentStateDb = WeeDb<RpcCurrentStateTables>;
pub type RpcTransactionsDb = WeeDb<RpcTransactionsTables>;
pub type RpcTailDb = WeeDb<RpcTailTables>;

impl NamedTables for RpcControlTables {
    const NAME: &'static str = "rpc-control";
}

impl NamedTables for RpcFilterCatalogTables {
    const NAME: &'static str = "rpc-filter-catalog";
}

impl NamedTables for RpcCurrentStateTables {
    const NAME: &'static str = "rpc-current-state";
}

impl NamedTables for RpcTransactionsTables {
    const NAME: &'static str = "rpc-transactions";
}

impl NamedTables for RpcTailTables {
    const NAME: &'static str = "rpc-tail";
}

weedb::tables! {
    pub struct RpcControlTables<TableContext> {
        pub state: tables::State,
        pub manifests: tables::PartitionManifests,
    }
}

weedb::tables! {
    pub struct RpcFilterCatalogTables<TableContext> {
        pub descriptors: tables::FilterCatalog,
    }
}

weedb::tables! {
    pub struct RpcTailTables<TableContext> {
        pub transactions: tables::TailTransactions,
        pub transactions_by_account: tables::TailTransactionsByAccount,
        pub transactions_by_hash: tables::TailTransactionsByHash,
        pub transactions_by_in_msg: tables::TailTransactionsByInMsg,
        pub retired_by_generation: tables::TailRetiredByGeneration,
        pub generation_progress: tables::TailGenerationProgress,
        pub generation_commits: tables::TailGenerationCommits,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tycho_storage::StorageContext;

    #[test]
    fn writable_table_groups_have_distinct_metric_names() {
        assert_eq!(RpcControlTables::NAME, "rpc-control");
        assert_eq!(RpcFilterCatalogTables::NAME, "rpc-filter-catalog");
        assert_eq!(RpcCurrentStateTables::NAME, "rpc-current-state");
        assert_eq!(RpcTransactionsTables::NAME, "rpc-transactions");
        assert_eq!(RpcTailTables::NAME, "rpc-tail");
    }

    #[tokio::test]
    async fn filter_catalog_has_only_descriptor_column_family() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let db: RpcFilterCatalogDb = context.open_preconfigured("rpc/filter-catalog").unwrap();
        db.descriptors.insert(1u64.to_be_bytes(), [1]).unwrap();
        assert_eq!(db.descriptors.get(1u64.to_be_bytes()).unwrap().as_deref(), Some(&[1][..]));
        assert_eq!(RpcFilterCatalogTables::NAME, "rpc-filter-catalog");
    }

    #[tokio::test]
    async fn transaction_partition_has_authoritative_accounts_column_family() {
        let (context, _tmp) = StorageContext::new_temp().await.unwrap();
        let db: RpcTransactionsDb = context.open_preconfigured("rpc/transactions-test").unwrap();
        let account = [0x11; tables::Accounts::KEY_LEN];
        db.accounts.insert(account, []).unwrap();
        assert!(db.accounts.get(account).unwrap().unwrap().is_empty());
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
        pub accounts: tables::Accounts,
        pub transactions_by_hash: tables::TransactionsByHash,
        pub transactions_by_in_msg: tables::TransactionsByInMsg,
        pub known_blocks: tables::KnownBlocks,
        pub block_transactions: tables::BlockTransactions,
        pub blocks_by_mc_seqno: tables::BlocksByMcSeqno,
        pub partition_commits: tables::PartitionCommits,
    }
}
