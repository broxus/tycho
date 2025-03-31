use std::collections::HashMap;

use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, CellBuilder, CellFamily, HashBytes};
use everscale_types::models::{AccountStatus, MsgInfo, StorageInfo};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tycho_vm::{SafeRc, Stack, StackValue, StackValueType};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TonCenterResponse<T> {
    pub ok: bool,
    pub result: Option<T>,
    pub error: Option<String>,
    pub code: Option<u32>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddressInformation {
    #[serde(rename = "@type")]
    pub type_field: String,
    pub balance: u64,
    pub code: Option<String>,
    pub data: Option<String>,
    pub last_transaction_id: TransactionId,
    pub block_id: Option<BlockId>,
    pub frozen_hash: Option<String>,
    pub sync_utime: i64,
    #[serde(rename = "@extra")]
    pub extra: String,
    pub state: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockId {
    #[serde(rename = "@type")]
    pub type_field: String,
    pub workchain: i32,
    pub shard: String,
    pub seqno: u32,
    pub root_hash: String,
    pub file_hash: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetAccountResponse {
    pub address: String,
    pub balance: u64,
    pub extra_balance: Option<Vec<ValueExtraResponse>>,
    pub currencies_balance: Option<HashMap<u32, u64>>,
    pub last_activity: u64,
    pub status: String,
    pub interfaces: Option<Vec<String>>,
    pub name: Option<String>,
    pub is_scam: Option<bool>,
    pub icon: Option<String>,
    pub memo_required: Option<bool>,
    pub get_methods: Vec<String>,
    pub is_suspended: Option<bool>,
    pub is_wallet: bool,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountResponse {
    pub address: String,
    pub balance: u64,
    pub extra_balance: Option<HashMap<String, String>>,
    pub code: Option<String>,
    pub data: Option<String>,
    pub last_transaction_lt: u64,
    pub last_transaction_hash: Option<String>,
    pub frozen_hash: Option<String>,
    pub status: String,
    pub storage: StorageResponse,
    pub libraries: Option<Vec<LibraryResponse>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageResponse {
    pub used_cells: u64,
    pub used_bits: u64,
    pub used_public_cells: u64,
    pub last_paid: u32,
    pub due_payment: Option<u64>,
}

impl From<StorageInfo> for StorageResponse {
    fn from(info: StorageInfo) -> Self {
        Self {
            used_cells: info.used.cells.into(),
            used_bits: info.used.bits.into(),
            used_public_cells: info.used.public_cells.into(),
            last_paid: info.last_paid,
            due_payment: info.due_payment.map(|v| v.into_inner() as u64),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LibraryResponse {
    pub public: bool,
    pub root: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionsResponse {
    pub transactions: Vec<TransactionResponse>,
}

impl TransactionsResponse {
    pub const MAX_LIMIT: u8 = 100;
    pub const DEFAULT_LIMIT: u8 = 10;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionResponse {
    #[serde(rename = "@type")]
    pub type_field: String,
    pub address: AddressResponse,
    pub utime: u32,
    pub data: String,
    pub transaction_id: TransactionId,
    pub fee: String,
    pub storage_fee: String,
    pub other_fee: String,
    pub in_msg: Option<MsgResponse>,
    pub out_msgs: Vec<MsgResponse>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddressResponse {
    #[serde(rename = "@type")]
    pub type_field: String,
    pub account_address: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionId {
    #[serde(rename = "@type")]
    pub type_field: String,
    pub lt: String,
    pub hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsgResponse {
    #[serde(rename = "@type")]
    pub type_field: String,
    pub hash: String,
    pub source: Option<String>,
    pub destination: Option<String>,
    pub value: String,
    pub extra_currencies: Vec<Value>,
    pub fwd_fee: String,
    pub ihr_fee: String,
    pub created_lt: String,
    pub body_hash: String,
    pub msg_data: MsgDataResponse,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "@type")]
pub enum MsgDataResponse {
    #[serde(rename = "msg.dataText")]
    Text { text: String },
    #[serde(rename = "msg.dataRaw")]
    Body {
        body: Option<String>,
        init_state: String,
    },
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionAccountResponse {
    pub address: String,
    pub name: Option<String>,
    pub is_scam: bool,
    pub icon: Option<String>,
    pub is_wallet: bool,
}

impl From<(MsgInfo, Option<Cell>, HashBytes)> for MsgResponse {
    fn from(msg: (MsgInfo, Option<Cell>, HashBytes)) -> Self {
        let (msg, body, hash) = msg;
        match msg {
            MsgInfo::Int(int_msg_info) => {
                MsgResponse {
                    type_field: "raw.message".to_string(),
                    hash: hash.to_string(),
                    source: Some(int_msg_info.src.to_string()),
                    destination: Some(int_msg_info.dst.to_string()),
                    created_lt: int_msg_info.created_lt.to_string(),
                    value: (int_msg_info.value.tokens.into_inner() as u64).to_string(),
                    fwd_fee: (int_msg_info.fwd_fee.into_inner() as u64).to_string(),
                    ihr_fee: (int_msg_info.ihr_fee.into_inner() as u64).to_string(),
                    extra_currencies: vec![],
                    body_hash: body.clone().unwrap_or_default().repr_hash().to_string(),
                    msg_data: MsgDataResponse::Body {
                        body: body.clone().map(Boc::encode_hex),
                        init_state: "".to_string(),
                    },
                    message: body.map(Boc::encode_hex).unwrap_or_default(), /* TODO: fill with correct values */
                }
            }
            MsgInfo::ExtIn(ext_in_msg) => {
                MsgResponse {
                    type_field: "raw.message".to_string(),
                    hash: hash.to_string(),
                    source: None,
                    destination: Some(ext_in_msg.dst.to_string()),
                    created_lt: "0".to_string(),
                    value: "0".to_string(),
                    fwd_fee: "0".to_string(),
                    ihr_fee: "0".to_string(),
                    extra_currencies: vec![],
                    body_hash: body.clone().unwrap_or_default().repr_hash().to_string(),
                    msg_data: MsgDataResponse::Body {
                        body: body.clone().map(Boc::encode_hex),
                        init_state: "".to_string(),
                    },
                    message: body.map(Boc::encode_hex).unwrap_or_default(), /* TODO: fill with correct values */
                }
            }
            MsgInfo::ExtOut(ext_out_msg) => MsgResponse {
                type_field: "raw.message".to_string(),
                hash: hash.to_string(),
                source: Some(ext_out_msg.src.to_string()),
                destination: None,
                created_lt: "0".to_string(),
                value: "0".to_string(),
                fwd_fee: "0".to_string(),
                ihr_fee: "0".to_string(),
                extra_currencies: vec![],
                body_hash: body.clone().unwrap_or_default().repr_hash().to_string(),
                msg_data: MsgDataResponse::Body {
                    body: body.clone().map(Boc::encode_hex),
                    init_state: "".to_string(),
                },
                message: body.map(Boc::encode_hex).unwrap_or_default(), /* TODO: fill with correct values */
            },
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValueExtraResponse {
    pub amount: String,
    pub preview: PreviewResponse,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreviewResponse {
    pub id: u64,
    pub symbol: String,
    pub decimals: u64,
    pub image: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitResponse {
    pub boc: String,
    pub interfaces: Vec<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecGetMethodResponse {
    pub success: bool,
    pub exit_code: i32,
    pub stack: Vec<TvmStackRecord>,
    pub decoded: Option<String>,
}

pub fn status_to_string(account_status: AccountStatus) -> String {
    match account_status {
        AccountStatus::Active => "active".to_string(),
        AccountStatus::Frozen => "frozen".to_string(),
        AccountStatus::Uninit => "uninitialized".to_string(),
        AccountStatus::NotExists => "not_exists".to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum TvmStackRecord {
    Cell { cell: String },
    Slice { slice: String },
    Num { num: String },
    Null,
    Nan,
    Tuple { tuple: Vec<TvmStackRecord> },
}

pub fn parse_tvm_stack(stack: SafeRc<Stack>) -> anyhow::Result<Vec<TvmStackRecord>> {
    let mut stack_response = vec![];
    for arg in stack.items.iter() {
        stack_response.push(parse_tvm_stack_value(arg)?);
    }
    Ok(stack_response)
}

fn parse_tvm_stack_value(arg: &SafeRc<dyn StackValue>) -> anyhow::Result<TvmStackRecord> {
    if arg.ty() == StackValueType::Null {
        return Ok(TvmStackRecord::Nan);
    }

    if arg.ty() == StackValueType::Int {
        let v = arg.as_int().unwrap();
        return Ok(TvmStackRecord::Num { num: v.to_string() });
    }

    if arg.ty() == StackValueType::Cell {
        let cell = arg.as_cell().unwrap();
        return Ok(TvmStackRecord::Cell {
            cell: Boc::encode_hex(cell),
        });
    }

    if arg.ty() == StackValueType::Slice {
        let slice = arg.as_cell_slice().unwrap();
        let cell = CellBuilder::build_from(slice.apply())?;
        return Ok(TvmStackRecord::Slice {
            slice: Boc::encode_base64(cell),
        });
    }
    if arg.ty() == StackValueType::Cont {
        let mut builder = CellBuilder::new();
        let cx = Cell::empty_context();
        arg.store_as_stack_value(&mut builder, cx)?;
        let cell = builder.build()?;
        return Ok(TvmStackRecord::Slice {
            slice: Boc::encode_base64(cell),
        });
    }

    if arg.ty() == StackValueType::Tuple {
        let mut tuple = vec![];
        for arg in arg.as_tuple().unwrap() {
            tuple.push(parse_tvm_stack_value(arg)?);
        }
        return Ok(TvmStackRecord::Tuple { tuple });
    }
    Ok(TvmStackRecord::Null)
}
