use diesel::Insertable;
use serde::{Deserialize, Serialize};

use crate::utils::*;
use crate::{Hash, NumBinds};

#[cfg(not(feature = "csv"))]
#[derive(Debug, Serialize, Clone, Insertable)]
#[diesel(table_name = crate::schema::blocks)]
pub struct Block {
    // indexed
    pub workchain: i8,
    pub shard: u64,
    pub seqno: u32,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub root_hash: Hash,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub file_hash: Hash,

    // Brief meta
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bool"))]
    pub is_key_block: bool,
    pub transaction_count: u16,
    pub gen_utime: u32,
    pub gen_software_version: u32,

    // Prev block ref
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub prev1: Hash,
    pub prev1_seqno: u32,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub prev2: Option<Hash>,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_optional"))]
    pub prev2_seqno: Option<u32>,

    pub prev_key_block: u32,

    // Detailed info
    pub block_info: JsonValue,
    pub value_flow: JsonValue,
    pub account_blocks: JsonValue,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_optional"))]
    pub shards_info: Option<JsonValue>,

    #[cfg_attr(feature = "csv", serde(with = "serde_csv_optional"))]
    pub additional_info: Option<JsonValue>,
}

#[cfg(feature = "csv")]
#[derive(Debug, Serialize, Clone)]
pub struct Block {
    // indexed
    pub workchain: i8,
    pub shard: u64,
    pub seqno: u32,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub root_hash: Hash,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub file_hash: Hash,

    // Brief meta
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bool"))]
    pub is_key_block: bool,
    pub transaction_count: u16,
    pub gen_utime: u32,
    pub gen_software_version: u32,

    // Prev block ref
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub prev1: Hash,
    pub prev1_seqno: u32,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes_optional"))]
    pub prev2: Option<Hash>,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_optional"))]
    pub prev2_seqno: Option<u32>,

    pub prev_key_block: u32,

    // Detailed info
    pub block_info: JsonValue,
    pub value_flow: JsonValue,
    pub account_blocks: JsonValue,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_optional"))]
    pub shards_info: Option<JsonValue>,

    #[cfg_attr(feature = "csv", serde(with = "serde_csv_optional"))]
    pub additional_info: Option<JsonValue>,
}

impl NumBinds for Block {
    const NUM_BINDS: usize = 19;
}

#[derive(Debug, Serialize, Clone, Insertable)]
#[diesel(table_name = crate::schema::network_config)]
pub struct KeyBlockConfig {
    pub end_lt: u64,
    pub seq_no: u32,
    #[cfg_attr(feature = "csv", serde(with = "serde_csv_bytes"))]
    pub config_params_boc: Vec<u8>,
}

impl NumBinds for KeyBlockConfig {
    const NUM_BINDS: usize = 3;
}

#[cfg(not(feature = "csv"))]
pub type JsonValue = serde_json::Value;
#[cfg(feature = "csv")]
pub type JsonValue = String;

#[cfg(feature = "csv")]
pub fn to_json_value<T: Serialize>(value: &T) -> JsonValue {
    serde_json::to_string(value).unwrap()
}

#[cfg(not(feature = "csv"))]
pub fn to_json_value<T: Serialize>(value: &T) -> JsonValue {
    serde_json::to_value(value).unwrap()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockInfo {
    #[serde(default, skip_serializing_if = "is_default")]
    pub version: u32,
    #[serde(default, skip_serializing_if = "is_default")]
    pub after_merge: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub before_split: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub after_split: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub want_split: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub want_merge: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub key_block: bool,

    #[serde(default, skip_serializing_if = "is_default")]
    pub vert_seq_no: u32,
    #[serde(default, skip_serializing_if = "is_default")]
    pub vert_seq_no_incr: u32,

    pub flags: u8,

    #[serde(with = "serde_string")]
    pub start_lt: u64,
    #[serde(with = "serde_string")]
    pub end_lt: u64,
    pub gen_validator_list_hash_short: u32,
    pub gen_catchain_seqno: u32,
    pub min_ref_mc_seqno: u32,
    pub prev_key_block_seqno: u32,
    pub gen_software: Option<GlobalVersion>,

    pub master_ref: Option<ExtBlkRef>,
    pub prev_ref: BlkPrevInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_vert_ref: Option<BlkPrevInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum BlkPrevInfo {
    Block { prev: ExtBlkRef },
    Blocks { prev1: ExtBlkRef, prev2: ExtBlkRef },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtBlkRef {
    #[serde(with = "serde_string")]
    pub end_lt: u64,
    pub seq_no: u32,
    pub root_hash: HashInsert<32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalVersion {
    pub version: u32,
    #[serde(with = "serde_string")]
    pub capabilities: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValueFlow {
    #[serde(default, with = "serde_string", skip_serializing_if = "is_default")]
    pub from_prev_blk: u64,
    #[serde(default, with = "serde_string", skip_serializing_if = "is_default")]
    pub to_next_blk: u64,
    #[serde(default, with = "serde_string", skip_serializing_if = "is_default")]
    pub imported: u64,
    #[serde(default, with = "serde_string", skip_serializing_if = "is_default")]
    pub exported: u64,
    #[serde(default, with = "serde_string", skip_serializing_if = "is_default")]
    pub fees_collected: u64,
    #[serde(default, with = "serde_string", skip_serializing_if = "is_default")]
    pub fees_imported: u64,
    #[serde(default, with = "serde_string", skip_serializing_if = "is_default")]
    pub recovered: u64,
    #[serde(default, with = "serde_string", skip_serializing_if = "is_default")]
    pub created: u64,
    #[serde(default, with = "serde_string", skip_serializing_if = "is_default")]
    pub minted: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountBlockInfo {
    wc: i32,
    address: Hash,
    old_hash: Hash,
    new_hash: Hash,
    transaction_count: u16,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardDescrInfoItem {
    pub shard_ident: ShardId,
    pub info: ShardDescrInfo,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardId {
    pub wc: i32,
    #[serde(with = "serde_string")]
    pub shard_prefix: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardDescrInfo {
    pub seq_no: u32,
    pub reg_mc_seqno: u32,
    #[serde(with = "serde_string")]
    pub start_lt: u64,
    #[serde(with = "serde_string")]
    pub end_lt: u64,
    pub root_hash: Hash,
    pub file_hash: Hash,
    #[serde(default, skip_serializing_if = "is_default")]
    pub before_split: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub before_merge: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub want_split: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub want_merge: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub nx_cc_updated: bool,
    pub flags: u8,
    pub next_catchain_seqno: u32,
    #[serde(with = "serde_string")]
    pub next_validator_shard: u64,
    pub min_ref_mc_seqno: u32,
    pub gen_utime: u32,
    pub split_merge_at: FutureSplitMerge,
    #[serde(with = "serde_string")]
    pub fees_collected: u64,
    #[serde(with = "serde_string")]
    pub funds_created: u64,
    #[cfg(feature = "venom")]
    pub collators_info: Option<ShardCollators>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum FutureSplitMerge {
    None,
    #[serde(rename_all = "camelCase")]
    Split {
        split_utime: u32,
        interval: u32,
    },
    #[serde(rename_all = "camelCase")]
    Merge {
        merge_utime: u32,
        interval: u32,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardCollators {
    pub prev: CollatorRange,
    pub prev2: Option<CollatorRange>,
    pub current: CollatorRange,
    pub next: CollatorRange,
    pub next2: Option<CollatorRange>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CollatorRange {
    pub collator: u16,
    pub collator_info: Option<ValidatorDescr>,
    pub start: u32,
    pub finish: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatorDescr {
    pub public_key: String,
    pub weight: u64,
    pub adnl_addr: Option<String>,
    pub mc_seq_no_since: u32,
}
