use diesel::Insertable;
use serde::Serialize;

use crate::schema::sql_types::MessageType;
#[cfg(feature = "csv")]
use crate::utils::*;
use crate::{Hash, NumBinds};

#[derive(Debug, Serialize, Clone, Insertable)]
#[diesel(table_name = crate::schema::messages)]
pub struct Message {
    /// Primary key
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub message_hash: Hash,

    pub src_workchain: i8,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub src_address: Option<Hash>,

    pub dst_workchain: i8,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub dst_address: Option<Hash>,

    pub message_type: MessageType,

    pub message_value: u64,
    pub ihr_fee: u64,
    pub fwd_fee: u64,
    pub import_fee: u64,

    pub created_lt: u64,
    pub created_at: u32,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bool"))]
    pub bounced: bool,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bool"))]
    pub bounce: bool,
}

impl NumBinds for Message {
    const NUM_BINDS: usize = 14;
}

#[derive(Debug, Serialize, Clone, Insertable)]
#[diesel(table_name = crate::schema::transaction_messages)]
pub struct TransactionMessage {
    /// Primary key. Column 0
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub transaction_hash: Hash,
    /// Primary key. Column 1
    pub index_in_transaction: u16,
    /// Primary key. Column 2
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bool"))]
    pub is_out: bool,
    pub transaction_time: u32,
    pub transaction_lt: u64,
    pub transaction_account_workchain: i8,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub transaction_account_address: Hash,

    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub block_hash: Hash,

    pub dst_workchain: i8,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub dst_address: Option<Hash>,
    pub src_workchain: i8,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub src_address: Option<Hash>,

    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub message_hash: Hash,
    pub message_type: MessageType,
    pub message_value: u64,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bool"))]
    pub bounced: bool,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bool"))]
    pub bounce: bool,
}

impl NumBinds for TransactionMessage {
    const NUM_BINDS: usize = 17;
}

#[derive(Debug, Default)]
pub struct MessageInfo {
    pub value: u64,
    pub ihr_fee: u64,
    pub fwd_fee: u64,
    pub import_fee: u64,

    pub bounce: bool,
    pub bounced: bool,
    pub created_lt: u64,
    pub created_at: u32,
}
