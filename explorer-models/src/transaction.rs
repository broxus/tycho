use std::fmt::Debug;

use diesel::Insertable;
use serde::Serialize;
use uuid::Uuid;

use crate::schema::sql_types::{ParsedType, TransactionType};
use crate::utils::HashInsert;
#[cfg(feature = "csv")]
use crate::utils::*;
use crate::{Hash, NumBinds};

#[derive(Debug, Serialize, Clone, Insertable)]
#[diesel(table_name = crate::schema::transactions)]
pub struct Transaction {
    pub workchain: i8,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub account_id: Hash,
    pub lt: u64,
    pub time: u32,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub hash: Hash,
    pub block_shard: u64,
    pub block_seqno: u32,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub block_hash: Hash,
    pub tx_type: TransactionType,

    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bool"))]
    pub aborted: bool,
    pub balance_change: i64,
    /// Compute phase result
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_optional"))]
    pub exit_code: Option<i32>,
    /// Action phase result
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_optional"))]
    pub result_code: Option<i32>,
}

impl NumBinds for Transaction {
    const NUM_BINDS: usize = 13;
}

#[derive(Debug, Serialize, Clone, Insertable)]
#[diesel(table_name = crate::schema::raw_transactions)]
pub struct RawTransaction {
    pub wc: i8,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub account_id: Hash,
    pub lt: u64,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub data: Vec<u8>,
}

impl NumBinds for RawTransaction {
    const NUM_BINDS: usize = 4;
}

#[derive(Debug, Clone, Insertable)]
#[diesel(check_for_backend(Mysql))]
#[diesel(table_name = crate::schema::parsed_messages_new)]
pub struct ParsedRecord {
    #[diesel(serialize_as = HashInsert<32>)]
    pub message_hash: [u8; 32],
    pub method_name: String,
    #[diesel(serialize_as = HashInsert<16>)]
    pub contract_id: Uuid,
    pub parsed_id: u32,
    pub parsed_type: ParsedType,
}

impl NumBinds for ParsedRecord {
    const NUM_BINDS: usize = 5;
}
