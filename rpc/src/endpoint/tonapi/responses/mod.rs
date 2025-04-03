use std::collections::HashMap;

use everscale_types::boc::Boc;
use everscale_types::cell::{Cell, CellBuilder, CellFamily, HashBytes};
use everscale_types::models::{
    AccountStatus, AccountStatusChange, ActionPhase, BouncePhase, ComputePhaseSkipReason,
    CreditPhase, ExecutedComputePhase, MsgInfo, MsgType, SkippedComputePhase, StorageInfo,
    StoragePhase,
};
use serde::{Deserialize, Serialize};
use tycho_vm::{SafeRc, Stack, StackValue, StackValueType};

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
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionResponse {
    pub hash: String,
    pub lt: u64,
    pub account: TransactionAccountResponse,
    pub success: bool,
    pub utime: u32,
    pub orig_status: String,
    pub end_status: String,
    pub total_fees: u64,
    pub end_balance: u64,
    pub transaction_type: String,
    pub state_update_old: String,
    pub state_update_new: String,
    pub in_msg: Option<MsgResponse>,
    pub out_msgs: Vec<MsgResponse>,
    pub block: Option<String>,
    pub prev_trans_hash: Option<String>,
    pub prev_trans_lt: Option<u64>,
    pub compute_phase: Option<ComputePhaseResponse>,
    pub storage_phase: Option<StoragePhaseResponse>,
    pub credit_phase: Option<CreditPhaseResponse>,
    pub action_phase: Option<ActionPhaseResponse>,
    pub bounce_phase: Option<String>,
    pub aborted: bool,
    pub destroyed: bool,
    pub raw: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionAccountResponse {
    pub address: String,
    pub name: Option<String>,
    pub is_scam: bool,
    pub icon: Option<String>,
    pub is_wallet: bool,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsgResponse {
    pub msg_type: String,
    pub created_lt: u64,
    pub ihr_disabled: bool,
    pub bounce: bool,
    pub bounced: bool,
    pub value: u64,
    pub value_extra: Option<Vec<ValueExtraResponse>>,
    pub fwd_fee: u64,
    pub ihr_fee: u64,
    pub destination: Option<TransactionAccountResponse>,
    pub source: Option<TransactionAccountResponse>,
    pub import_fee: u64,
    pub created_at: u32,
    pub op_code: Option<String>,
    pub init: Option<InitResponse>,
    pub hash: String,
    pub raw_body: Option<String>,
    pub decoded_op_name: Option<String>,
    pub decoded_body: Option<String>,
}

impl From<(MsgInfo, Option<Cell>, HashBytes)> for MsgResponse {
    fn from(msg: (MsgInfo, Option<Cell>, HashBytes)) -> Self {
        let (msg, body, hash) = msg;
        match msg {
            MsgInfo::Int(int_msg_info) => {
                MsgResponse {
                    msg_type: msg_type_to_string(MsgType::Int),
                    created_lt: int_msg_info.created_lt,
                    ihr_disabled: int_msg_info.ihr_disabled,
                    bounce: int_msg_info.bounce,
                    bounced: int_msg_info.bounced,
                    value: int_msg_info.value.tokens.into_inner() as u64,
                    value_extra: None, // TODO: fill with correct value_extra
                    fwd_fee: int_msg_info.fwd_fee.into_inner() as u64,
                    ihr_fee: int_msg_info.ihr_fee.into_inner() as u64,
                    destination: Some(TransactionAccountResponse {
                        address: int_msg_info.dst.to_string(),
                        name: None,
                        is_scam: false,
                        icon: None,
                        is_wallet: true,
                    }),
                    source: Some(TransactionAccountResponse {
                        address: int_msg_info.src.to_string(),
                        name: None,
                        is_scam: false,
                        icon: None,
                        is_wallet: true,
                    }),
                    import_fee: 0u64, // TODO: fill with correct fees
                    created_at: int_msg_info.created_at,
                    op_code: None, // TODO: fill with correct values
                    init: None,    // TODO: fill with correct values
                    hash: hash.to_string(),
                    raw_body: body.map(Boc::encode_hex),
                    decoded_op_name: None, // TODO: fill with correct values
                    decoded_body: None,    // TODO: fill with correct values
                }
            }
            MsgInfo::ExtIn(ext_in_msg) => {
                MsgResponse {
                    msg_type: msg_type_to_string(MsgType::ExtIn),
                    created_lt: 0,
                    ihr_disabled: false,
                    bounce: false,
                    bounced: false,
                    value: 0u64,
                    value_extra: None, // TODO: fill with correct value_extra
                    fwd_fee: 0u64,
                    ihr_fee: 0u64,
                    destination: Some(TransactionAccountResponse {
                        address: ext_in_msg.dst.to_string(),
                        name: None,
                        is_scam: false,
                        icon: None,
                        is_wallet: true,
                    }),
                    source: None,
                    import_fee: ext_in_msg.import_fee.into_inner() as u64,
                    created_at: 0,
                    op_code: None, // TODO: fill with correct values
                    init: None,    // TODO: fill with correct values
                    hash: hash.to_string(),
                    raw_body: body.map(Boc::encode_hex),
                    decoded_op_name: None, // TODO: fill with correct values
                    decoded_body: None,    // TODO: fill with correct values
                }
            }
            MsgInfo::ExtOut(ext_out_msg) => MsgResponse {
                msg_type: msg_type_to_string(MsgType::ExtOut),
                created_lt: ext_out_msg.created_lt,
                ihr_disabled: false,
                bounce: false,
                bounced: false,
                value: 0u64,
                value_extra: None, // TODO: fill with correct value_extra
                fwd_fee: 0u64,
                ihr_fee: 0u64,
                destination: None,
                source: Some(TransactionAccountResponse {
                    address: ext_out_msg.src.to_string(),
                    name: None,
                    is_scam: false,
                    icon: None,
                    is_wallet: true,
                }),
                import_fee: 0,
                created_at: ext_out_msg.created_at,
                op_code: None, // TODO: fill with correct values
                init: None,    // TODO: fill with correct values
                hash: hash.to_string(),
                raw_body: body.map(Boc::encode_hex),
                decoded_op_name: None, // TODO: fill with correct values
                decoded_body: None,    // TODO: fill with correct values
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
pub struct ComputePhaseResponse {
    pub skipped: bool,
    pub skip_reason: Option<String>,
    pub success: Option<bool>,
    pub gas_fees: Option<u64>,
    pub gas_used: Option<u64>,
    pub vm_steps: Option<u32>,
    pub exit_code: Option<i32>,
    pub exit_code_description: Option<String>,
}

impl From<SkippedComputePhase> for ComputePhaseResponse {
    fn from(value: SkippedComputePhase) -> Self {
        ComputePhaseResponse {
            skipped: true,
            skip_reason: Some(reason_to_string(value.reason)),
            success: None,
            gas_fees: None,
            gas_used: None,
            vm_steps: None,
            exit_code: None,
            exit_code_description: None,
        }
    }
}

impl From<ExecutedComputePhase> for ComputePhaseResponse {
    fn from(value: ExecutedComputePhase) -> Self {
        ComputePhaseResponse {
            skipped: false,
            skip_reason: None,
            success: Some(value.success),
            gas_fees: Some(value.gas_fees.into_inner() as u64),
            gas_used: Some(value.gas_used.into_inner()),
            vm_steps: Some(value.vm_steps),
            exit_code: Some(value.exit_code),
            exit_code_description: None,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoragePhaseResponse {
    pub fees_collected: u64,
    pub fees_due: Option<u64>,
    pub status_change: String,
}

impl From<StoragePhase> for StoragePhaseResponse {
    fn from(storage_phase: StoragePhase) -> Self {
        StoragePhaseResponse {
            fees_collected: storage_phase.storage_fees_collected.into_inner() as u64,
            fees_due: storage_phase
                .storage_fees_due
                .map(|fees_due| fees_due.into_inner() as u64),
            status_change: status_change_to_string(storage_phase.status_change),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditPhaseResponse {
    pub fees_collected: Option<u64>,
    pub credit: u64,
}

impl From<CreditPhase> for CreditPhaseResponse {
    fn from(credit_phase: CreditPhase) -> Self {
        CreditPhaseResponse {
            fees_collected: credit_phase
                .due_fees_collected
                .map(|fees_due| fees_due.into_inner() as u64),
            credit: credit_phase.credit.tokens.into_inner() as u64,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionPhaseResponse {
    pub success: bool,
    pub result_code: i32,
    pub total_actions: u16,
    pub skipped_actions: u16,
    pub fwd_fees: u64,
    pub total_fees: u64,
    pub result_code_description: Option<String>,
}

impl From<ActionPhase> for ActionPhaseResponse {
    fn from(action_phase: ActionPhase) -> Self {
        ActionPhaseResponse {
            success: action_phase.success,
            result_code: action_phase.result_code,
            total_actions: action_phase.total_actions,
            skipped_actions: action_phase.skipped_actions,
            fwd_fees: action_phase
                .total_fwd_fees
                .map(|total_fwd_fees| total_fwd_fees.into_inner() as u64)
                .unwrap_or_default(),
            total_fees: action_phase
                .total_action_fees
                .map(|total_action_fees| total_action_fees.into_inner() as u64)
                .unwrap_or_default(),
            result_code_description: None,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecGetMethodResponse {
    pub success: bool,
    pub exit_code: i32,
    pub stack: Vec<TvmStackRecord>,
    pub decoded: Option<String>,
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

pub fn status_to_string(account_status: AccountStatus) -> String {
    match account_status {
        AccountStatus::Active => "active".to_string(),
        AccountStatus::Frozen => "frozen".to_string(),
        AccountStatus::Uninit => "uninit".to_string(),
        AccountStatus::NotExists => "not_exists".to_string(),
    }
}

pub fn msg_type_to_string(account_status: MsgType) -> String {
    match account_status {
        MsgType::Int => "int_msg".to_string(),
        MsgType::ExtIn => "ext_in_msg".to_string(),
        MsgType::ExtOut => "ext_out_msg".to_string(),
    }
}
pub fn reason_to_string(reason: ComputePhaseSkipReason) -> String {
    match reason {
        ComputePhaseSkipReason::NoState => "cskip_no_state".to_string(),
        ComputePhaseSkipReason::BadState => "cskip_bad_state".to_string(),
        ComputePhaseSkipReason::NoGas => "cskip_no_gas".to_string(),
        ComputePhaseSkipReason::Suspended => "cskip_suspended".to_string(), // No Suspended in docs
    }
}
pub fn status_change_to_string(status: AccountStatusChange) -> String {
    match status {
        AccountStatusChange::Unchanged => "acst_unchanged".to_string(),
        AccountStatusChange::Frozen => "acst_frozen".to_string(),
        AccountStatusChange::Deleted => "acst_deleted".to_string(),
    }
}
pub fn bounce_phase_to_string(bounce: BouncePhase) -> String {
    match bounce {
        BouncePhase::NegativeFunds => "TrPhaseBounceNegfunds".to_string(),
        BouncePhase::NoFunds(_) => "TrPhaseBounceNofunds".to_string(),
        BouncePhase::Executed(_) => "TrPhaseBounceOk".to_string(),
    }
}
