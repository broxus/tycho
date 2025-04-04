use std::borrow::Cow;
use std::cell::RefCell;
use std::str::FromStr;
use std::sync::OnceLock;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use base64::prelude::{Engine as _, BASE64_STANDARD};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use num_bigint::BigInt;
use rand::Rng;
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize};
use tycho_block_util::message::{validate_external_message, ExtMsgRepr};
use tycho_storage::TransactionsIterBuilder;
use tycho_util::metrics::HistogramGuard;
use tycho_util::serde_helpers::{self, Base64BytesWithLimit};
use tycho_util::sync::rayon_run;

use crate::state::{LoadedAccountState, RpcState, RpcStateError, RunGetMethodPermit};
use crate::util::error_codes::*;
use crate::util::jrpc_extractor::{declare_jrpc_method, Jrpc, JrpcErrorResponse, JrpcOkResponse};

declare_jrpc_method! {
    pub enum MethodParams: Method {
        GetAddressInformation(AccountParams),
        GetTransactions(TransactionsParams),
        SendBoc(SendBocParams),
        RunGetMethod(RunGetMethodParams),
    }
}

pub async fn route(State(state): State<RpcState>, req: Jrpc<String, Method>) -> Response {
    let label = [("method", req.method)];
    let _hist = HistogramGuard::begin_with_labels("tycho_jrpc_request_time", &label);
    match req.params {
        MethodParams::GetAddressInformation(p) => {
            handle_get_address_information(req.id, state, p).await
        }
        MethodParams::GetTransactions(p) => handle_get_transactions(req.id, state, p).await,
        MethodParams::SendBoc(p) => handle_send_boc(req.id, state, p).await,
        MethodParams::RunGetMethod(p) => handle_run_get_method(req.id, state, p).await,
    }
}

async fn handle_get_address_information(id: String, state: RpcState, p: AccountParams) -> Response {
    let item = match state.get_account_state(&p.address) {
        Ok(item) => item,
        Err(e) => return error_to_response(id, e),
    };

    let mut balance = Tokens::ZERO;
    let mut code = None::<Cell>;
    let mut data = None::<Cell>;
    let mut frozen_hash = None::<HashBytes>;
    let mut status = TonlibAccountStatus::Uninitialized;
    let last_transaction_id;
    let block_id;
    let sync_utime;

    let _mc_ref_handle;
    match item {
        LoadedAccountState::NotFound {
            timings,
            mc_block_id,
        } => {
            last_transaction_id = TonlibTransactionId::default();
            block_id = TonlibBlockId::from(*mc_block_id);
            sync_utime = timings.gen_utime;
        }
        LoadedAccountState::Found {
            timings,
            mc_block_id,
            state,
            mc_ref_handle,
        } => {
            last_transaction_id =
                TonlibTransactionId::new(state.last_trans_lt, state.last_trans_hash);
            block_id = TonlibBlockId::from(*mc_block_id);
            sync_utime = timings.gen_utime;

            match state.load_account() {
                Ok(Some(loaded)) => {
                    _mc_ref_handle = mc_ref_handle;

                    balance = loaded.balance.tokens;
                    match loaded.state {
                        AccountState::Active(account) => {
                            code = account.code;
                            data = account.data;
                            status = TonlibAccountStatus::Active;
                        }
                        AccountState::Frozen(hash) => {
                            frozen_hash = Some(hash);
                            status = TonlibAccountStatus::Frozen;
                        }
                        AccountState::Uninit => {}
                    }
                }
                Ok(None) => {}
                Err(e) => return error_to_response(id, RpcStateError::Internal(e.into())),
            }
        }
    }

    ok_to_response(id, AddressInformationResponse {
        ty: AddressInformationResponse::TY,
        balance,
        extra_currencies: [],
        code,
        data,
        last_transaction_id,
        block_id,
        frozen_hash,
        sync_utime,
        extra: TonlibExtra,
        state: status,
    })
}

async fn handle_get_transactions(id: String, state: RpcState, p: TransactionsParams) -> Response {
    if p.limit == 0 || p.lt.unwrap_or(u64::MAX) < p.to_lt {
        return JrpcOkResponse::new(id, [(); 0]).into_response();
    } else if p.limit > GetTransactionsResponse::MAX_LIMIT {
        return too_large_limit_response(id);
    }

    match state.get_transactions(&p.address, p.lt, p.to_lt) {
        Ok(list) => ok_to_response(id, GetTransactionsResponse {
            address: &p.address,
            list: RefCell::new(Some(list)),
            limit: p.limit,
            to_lt: p.to_lt,
        }),
        Err(e) => error_to_response(id, e),
    }
}

async fn handle_send_boc(id: String, state: RpcState, p: SendBocParams) -> Response {
    if let Err(e) = validate_external_message(&p.boc).await {
        return JrpcErrorResponse {
            id: Some(id),
            code: INVALID_BOC_CODE,
            message: e.to_string().into(),
        }
        .into_response();
    }

    state.broadcast_external_message(&p.boc).await;
    ok_to_response(id, EmptyOk)
}

async fn handle_run_get_method(id: String, state: RpcState, p: RunGetMethodParams) -> Response {
    enum RunMethodError {
        RpcError(RpcStateError),
        InvalidParams(everscale_types::error::Error),
        Internal(everscale_types::error::Error),
        TooBigStack(usize),
    }

    let config = &state.config().run_get_method;
    let permit = match state.acquire_run_get_method_permit().await {
        RunGetMethodPermit::Acquired(permit) => permit,
        RunGetMethodPermit::Disabled => {
            return JrpcErrorResponse {
                id: Some(id),
                code: NOT_SUPPORTED_CODE,
                message: "method disabled".into(),
            }
            .into_response()
        }
        RunGetMethodPermit::Timeout => {
            return JrpcErrorResponse {
                id: Some(id),
                code: TIMEOUT_CODE,
                message: "timeout while waiting for VM slot".into(),
            }
            .into_response()
        }
    };
    let gas_limit = config.vm_getter_gas;
    let max_response_stack_items = config.max_response_stack_items;

    let f = move || {
        // Prepare stack.
        let mut items = Vec::with_capacity(p.stack.len() + 1);
        for item in p.stack {
            let item =
                tycho_vm::RcStackValue::try_from(item).map_err(RunMethodError::InvalidParams)?;
            items.push(item);
        }
        items.push(tycho_vm::RcStackValue::new_dyn_value(BigInt::from(
            p.method,
        )));
        let stack = tycho_vm::Stack::with_items(items);

        // Load account state.
        let (block_id, shard_state, _mc_ref_handle, timings) = match state
            .get_account_state(&p.address)
            .map_err(RunMethodError::RpcError)?
        {
            LoadedAccountState::Found {
                mc_block_id,
                state,
                mc_ref_handle,
                timings,
            } => (mc_block_id, state, mc_ref_handle, timings),
            LoadedAccountState::NotFound { mc_block_id, .. } => {
                return Ok(TonlibRunResult {
                    ty: TonlibRunResult::TY,
                    exit_code: tycho_vm::VmException::Fatal.as_exit_code(),
                    gas_used: 0,
                    stack: None,
                    last_transaction_id: TonlibTransactionId::default(),
                    block_id: TonlibBlockId::from(*mc_block_id),
                    extra: TonlibExtra,
                });
            }
        };

        // Parse account state.
        let mut balance = CurrencyCollection::ZERO;
        let mut code = None::<Cell>;
        let mut data = None::<Cell>;
        let mut account_libs = Dict::new();
        let mut state_libs = Dict::new();
        if let Some(account) = shard_state
            .load_account()
            .map_err(RunMethodError::Internal)?
        {
            balance = account.balance;
            if let AccountState::Active(state_init) = account.state {
                code = state_init.code;
                data = state_init.data;
                account_libs = state_init.libraries;
                state_libs = state.get_libraries();
            }
        }

        // Prepare VM state.
        let config = state.get_unpacked_blockchain_config();

        let smc_info = tycho_vm::SmcInfoBase::new()
            .with_now(timings.gen_utime)
            .with_block_lt(timings.gen_lt)
            .with_tx_lt(timings.gen_lt)
            .with_account_balance(balance)
            .with_account_addr(p.address.into())
            .with_config(config.raw.clone())
            .require_ton_v4()
            .with_code(code.clone().unwrap_or_default())
            .with_message_balance(CurrencyCollection::ZERO)
            .with_storage_fees(Tokens::ZERO)
            .require_ton_v6()
            .with_unpacked_config(config.unpacked.as_tuple())
            .require_ton_v9();

        let libraries = (account_libs, state_libs);
        let mut vm = tycho_vm::VmState::builder()
            .with_smc_info(smc_info)
            .with_code(code)
            .with_data(data.unwrap_or_default())
            .with_libraries(&libraries)
            .with_init_selector(false)
            .with_raw_stack(tycho_vm::SafeRc::new(stack))
            .with_gas(tycho_vm::GasParams {
                max: gas_limit,
                limit: gas_limit,
                ..tycho_vm::GasParams::getter()
            })
            .with_modifiers(config.modifiers)
            .build();

        // Run VM.
        let exit_code = !vm.run();
        if vm.stack.depth() > max_response_stack_items {
            return Err(RunMethodError::TooBigStack(vm.stack.depth()));
        }

        // Prepare response.
        let gas_used = vm.gas.consumed();

        Ok::<_, RunMethodError>(TonlibRunResult {
            ty: TonlibRunResult::TY,
            exit_code,
            gas_used,
            stack: Some(vm.stack),
            last_transaction_id: TonlibTransactionId::new(
                shard_state.last_trans_lt,
                shard_state.last_trans_hash,
            ),
            block_id: TonlibBlockId::from(*block_id),
            extra: TonlibExtra,
        })
    };

    rayon_run(move || {
        let res = match f() {
            Ok(res) => {
                STACK_ITEMS_LIMIT.set(max_response_stack_items);
                ok_to_response(id, res)
            }
            Err(RunMethodError::RpcError(e)) => error_to_response(id, e),
            Err(RunMethodError::InvalidParams(e)) => JrpcErrorResponse {
                id: Some(id),
                code: INVALID_PARAMS_CODE,
                message: format!("invalid stack item: {e}").into(),
            }
            .into_response(),
            Err(RunMethodError::Internal(e)) => {
                error_to_response(id, RpcStateError::Internal(e.into()))
            }
            Err(RunMethodError::TooBigStack(n)) => error_to_response(
                id,
                RpcStateError::Internal(anyhow::anyhow!("response stack is too big to send: {n}")),
            ),
        };

        // NOTE: Make sure that permit lives as long as the execution.
        drop(permit);

        res
    })
    .await
}

// === Requests ===

#[derive(Debug, Deserialize)]
pub struct AccountParams {
    #[serde(with = "serde_tonlib_address")]
    pub address: StdAddr,
}

#[derive(Debug, Deserialize)]
pub struct TransactionsParams {
    #[serde(with = "serde_tonlib_address")]
    pub address: StdAddr,
    #[serde(default = "default_tx_limit")]
    pub limit: u8,
    #[serde(default, with = "serde_helpers::option_string")]
    pub lt: Option<u64>,
    #[expect(unused)]
    #[serde(default, with = "serde_option_tonlib_hash")]
    pub hash: Option<HashBytes>,
    #[serde(default, with = "serde_helpers::string")]
    pub to_lt: u64,
}

const fn default_tx_limit() -> u8 {
    10
}

#[derive(Debug, Deserialize)]
pub struct SendBocParams {
    #[serde(with = "Base64BytesWithLimit::<{ ExtMsgRepr::MAX_BOC_SIZE }>")]
    pub boc: bytes::Bytes,
}

#[derive(Debug, Deserialize)]
pub struct RunGetMethodParams {
    #[serde(with = "serde_tonlib_address")]
    pub address: StdAddr,
    #[serde(with = "serde_method_id")]
    pub method: i64,
    pub stack: Vec<SerdeInputStackItem>,
}

// === Responses ===

#[derive(Serialize)]
struct AddressInformationResponse {
    #[serde(rename = "@type")]
    ty: &'static str,
    #[serde(with = "serde_helpers::string")]
    balance: Tokens,
    extra_currencies: [(); 0],
    #[serde(with = "serde_boc_or_empty")]
    code: Option<Cell>,
    #[serde(with = "serde_boc_or_empty")]
    data: Option<Cell>,
    last_transaction_id: TonlibTransactionId,
    block_id: TonlibBlockId,
    #[serde(serialize_with = "serde_tonlib_hash::serialize_or_empty")]
    frozen_hash: Option<HashBytes>,
    sync_utime: u32,
    #[serde(rename = "@extra")]
    extra: TonlibExtra,
    state: TonlibAccountStatus,
}

impl AddressInformationResponse {
    const TY: &str = "raw.fullAccountState";
}

#[derive(Serialize)]
struct TonlibTransactionId {
    #[serde(rename = "@type")]
    ty: &'static str,
    #[serde(with = "serde_helpers::string")]
    lt: u64,
    #[serde(with = "serde_tonlib_hash")]
    hash: HashBytes,
}

impl TonlibTransactionId {
    const TY: &str = "internal.transactionId";

    fn new(lt: u64, hash: HashBytes) -> Self {
        Self {
            ty: Self::TY,
            lt,
            hash,
        }
    }
}

impl Default for TonlibTransactionId {
    fn default() -> Self {
        Self::new(0, HashBytes::ZERO)
    }
}

#[derive(Serialize)]
struct TonlibBlockId {
    #[serde(rename = "@type")]
    ty: &'static str,
    workchain: i32,
    #[serde(with = "serde_helpers::string")]
    shard: i64,
    seqno: u32,
    #[serde(with = "serde_tonlib_hash")]
    root_hash: HashBytes,
    #[serde(with = "serde_tonlib_hash")]
    file_hash: HashBytes,
}

impl TonlibBlockId {
    const TY: &str = "ton.blockIdExt";
}

impl From<BlockId> for TonlibBlockId {
    fn from(value: BlockId) -> Self {
        Self {
            ty: Self::TY,
            workchain: value.shard.workchain(),
            shard: value.shard.prefix() as i64,
            seqno: value.seqno,
            root_hash: value.root_hash,
            file_hash: value.file_hash,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum TonlibAccountStatus {
    Uninitialized,
    Frozen,
    Active,
}

#[derive(Debug, Clone, Copy)]
struct TonlibExtra;

impl Serialize for TonlibExtra {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl std::fmt::Display for TonlibExtra {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let rand: f32 = rand::thread_rng().gen();

        write!(f, "{now}:0:{rand}")
    }
}

struct GetTransactionsResponse<'a> {
    address: &'a StdAddr,
    list: RefCell<Option<TransactionsIterBuilder<'a>>>,
    limit: u8,
    to_lt: u64,
}

impl GetTransactionsResponse<'_> {
    const MAX_LIMIT: u8 = 100;
}

impl Serialize for GetTransactionsResponse<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::{Error, SerializeSeq};

        let list = self.list.borrow_mut().take().unwrap();

        let mut seq = serializer.serialize_seq(None)?;

        let mut buffer = String::new();

        // NOTE: We use a `.map` from a separate impl thus we cannot use `.try_for_each`.
        #[allow(clippy::map_collect_result_unit)]
        list.map(|item| {
            let cell = match Boc::decode(item) {
                Ok(cell) => cell,
                Err(e) => return Some(Err(S::Error::custom(e))),
            };
            let tx = match cell.parse::<Transaction>() {
                Ok(tx) => tx,
                Err(e) => return Some(Err(S::Error::custom(e))),
            };
            if tx.lt <= self.to_lt {
                return None;
            }

            let hash = *cell.repr_hash();
            drop(cell);

            Some((|| {
                let mut fee = tx.total_fees.tokens;

                let mut out_msgs = Vec::with_capacity(tx.out_msg_count.into_inner() as _);
                for item in tx.out_msgs.values() {
                    let msg = item
                        .and_then(|cell| TonlibMsg::parse(&cell))
                        .map_err(Error::custom)?;

                    fee = fee.saturating_add(msg.fwd_fee);
                    fee = fee.saturating_add(msg.ihr_fee);
                    out_msgs.push(msg);
                }

                let storage_fee = match tx.load_info().map_err(Error::custom)? {
                    TxInfo::Ordinary(info) => match info.storage_phase {
                        Some(phase) => phase.storage_fees_collected,
                        None => Tokens::ZERO,
                    },
                    TxInfo::TickTock(info) => info.storage_phase.storage_fees_collected,
                };

                BASE64_STANDARD.encode_string(item, &mut buffer);
                let res = seq.serialize_element(&TonlibTransaction {
                    ty: TonlibTransaction::TY,
                    address: TonlibAddress::new(self.address),
                    utime: tx.now,
                    data: &buffer,
                    transaction_id: TonlibTransactionId::new(tx.lt, hash),
                    fee,
                    storage_fee,
                    other_fee: fee.saturating_sub(storage_fee),
                    in_msg: tx.in_msg,
                    out_msgs,
                });
                buffer.clear();
                res
            })())
        })
        .take(self.limit as _)
        .collect::<Result<(), _>>()?;

        seq.end()
    }
}

#[derive(Serialize)]
struct TonlibTransaction<'a> {
    #[serde(rename = "@type")]
    ty: &'static str,
    address: TonlibAddress<'a>,
    utime: u32,
    data: &'a str,
    transaction_id: TonlibTransactionId,
    #[serde(with = "serde_helpers::string")]
    fee: Tokens,
    #[serde(with = "serde_helpers::string")]
    storage_fee: Tokens,
    #[serde(with = "serde_helpers::string")]
    other_fee: Tokens,
    #[serde(serialize_with = "TonlibMsg::serialize_in_msg")]
    in_msg: Option<Cell>,
    out_msgs: Vec<TonlibMsg>,
}

impl TonlibTransaction<'_> {
    const TY: &'static str = "raw.transaction";
}

#[derive(Serialize)]
struct TonlibMsg {
    #[serde(rename = "@type")]
    ty: &'static str,
    #[serde(with = "serde_tonlib_hash")]
    hash: HashBytes,
    #[serde(with = "serde_option_tonlib_address")]
    source: Option<StdAddr>,
    #[serde(with = "serde_option_tonlib_address")]
    destination: Option<StdAddr>,
    #[serde(with = "serde_helpers::string")]
    value: Tokens,
    extra_currencies: [(); 0],
    fwd_fee: Tokens,
    ihr_fee: Tokens,
    #[serde(with = "serde_helpers::string")]
    created_lt: u64,
    #[serde(with = "serde_tonlib_hash")]
    body_hash: HashBytes,
    msg_data: TonlibMsgData,
}

impl TonlibMsg {
    const TY: &str = "raw.message";

    fn parse(cell: &Cell) -> Result<Self, everscale_types::error::Error> {
        fn to_std_addr(addr: IntAddr) -> Option<StdAddr> {
            match addr {
                IntAddr::Std(addr) => Some(addr),
                IntAddr::Var(_) => None,
            }
        }

        let hash = *cell.repr_hash();
        let msg = cell.parse::<Message<'_>>()?;

        let source;
        let destination;
        let value;
        let fwd_fee;
        let ihr_fee;
        let created_lt;
        match msg.info {
            MsgInfo::Int(info) => {
                source = to_std_addr(info.src);
                destination = to_std_addr(info.dst);
                value = info.value.tokens;
                fwd_fee = info.fwd_fee;
                ihr_fee = info.ihr_fee;
                created_lt = info.created_lt;
            }
            MsgInfo::ExtIn(info) => {
                source = None;
                destination = to_std_addr(info.dst);
                value = Tokens::ZERO;
                fwd_fee = Tokens::ZERO;
                ihr_fee = Tokens::ZERO;
                created_lt = 0;
            }
            MsgInfo::ExtOut(info) => {
                source = to_std_addr(info.src);
                destination = None;
                value = Tokens::ZERO;
                fwd_fee = Tokens::ZERO;
                ihr_fee = Tokens::ZERO;
                created_lt = info.created_lt;
            }
        }

        let body = CellBuilder::build_from(msg.body)?;
        let body_hash = *body.repr_hash();
        let init_state = msg.init.map(CellBuilder::build_from).transpose()?;

        Ok(Self {
            ty: Self::TY,
            hash,
            source,
            destination,
            value,
            extra_currencies: [],
            fwd_fee,
            ihr_fee,
            created_lt,
            body_hash,
            msg_data: TonlibMsgData {
                ty: TonlibMsgData::TY,
                body,
                init_state,
            },
        })
    }

    fn serialize_in_msg<S>(cell: &Option<Cell>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;

        match cell {
            Some(cell) => {
                let msg = Self::parse(cell).map_err(Error::custom)?;
                serializer.serialize_some(&msg)
            }
            None => serializer.serialize_none(),
        }
    }
}

#[derive(Serialize)]
struct TonlibMsgData {
    #[serde(rename = "@type")]
    ty: &'static str,
    #[serde(with = "Boc")]
    body: Cell,
    #[serde(with = "serde_boc_or_empty")]
    init_state: Option<Cell>,
}

impl TonlibMsgData {
    const TY: &str = "msg.dataRaw";
}

#[derive(Serialize)]
struct TonlibAddress<'a> {
    #[serde(rename = "@type")]
    ty: &'static str,
    #[serde(with = "serde_tonlib_address")]
    account_address: &'a StdAddr,
}

impl<'a> TonlibAddress<'a> {
    const TY: &'static str = "accountAddress";

    fn new(address: &'a StdAddr) -> Self {
        Self {
            ty: Self::TY,
            account_address: address,
        }
    }
}

#[derive(Serialize)]
struct TonlibRunResult {
    ty: &'static str,
    exit_code: i32,
    gas_used: u64,
    #[serde(serialize_with = "TonlibRunResult::serialize_stack")]
    stack: Option<tycho_vm::SafeRc<tycho_vm::Stack>>,
    last_transaction_id: TonlibTransactionId,
    block_id: TonlibBlockId,
    #[serde(rename = "@extra")]
    extra: TonlibExtra,
}

impl TonlibRunResult {
    const TY: &str = "smc.runResult";

    fn serialize_stack<S>(
        value: &Option<tycho_vm::SafeRc<tycho_vm::Stack>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(stack) => {
                let mut seq = serializer.serialize_seq(Some(stack.depth()))?;
                for item in &stack.items {
                    seq.serialize_element(&StackValueOutputWrapper(item.as_ref()))?;
                }
                seq.end()
            }
            None => [(); 0].serialize(serializer),
        }
    }

    fn serialize_stack_item<S>(value: &DynStackValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        use tycho_vm::StackValueType;

        let is_limit_ok = STACK_ITEMS_LIMIT.with(|limit| {
            let current_limit = limit.get();
            if current_limit > 0 {
                limit.set(current_limit - 1);
                true
            } else {
                false
            }
        });

        if !is_limit_ok {
            return Err(Error::custom("too many stack items in response"));
        }

        struct Num<'a>(&'a BigInt);

        impl std::fmt::Display for Num<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let sign = if self.0.sign() == num_bigint::Sign::Minus {
                    "-"
                } else {
                    ""
                };
                write!(f, "{sign}0x{:x}", self.0)
            }
        }

        impl Serialize for Num<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }

        struct List<'a>(&'a DynStackValue, &'a DynStackValue);

        impl Serialize for List<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let mut seq = s.serialize_seq(None)?;
                seq.serialize_element(&StackValueOutputWrapper(self.0))?;
                let mut next = self.1;
                while !next.is_null() {
                    let (head, tail) = next
                        .as_pair()
                        .ok_or_else(|| Error::custom("invalid list"))?;
                    seq.serialize_element(&StackValueOutputWrapper(head))?;
                    next = tail;
                }
                seq.end()
            }
        }

        struct Tuple<'a>(&'a [tycho_vm::RcStackValue]);

        impl Serialize for Tuple<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let mut seq = s.serialize_seq(Some(self.0.len()))?;
                for item in self.0 {
                    seq.serialize_element(&StackValueOutputWrapper(item.as_ref()))?;
                }
                seq.end()
            }
        }

        #[derive(Serialize)]
        struct CellBytes<'a> {
            #[serde(with = "Boc")]
            bytes: &'a Cell,
        }

        match value.ty() {
            StackValueType::Null => ("list", [(); 0]).serialize(serializer),
            StackValueType::Int => match value.as_int() {
                Some(int) => ("num", Num(int)).serialize(serializer),
                None => ("num", "(null)").serialize(serializer),
            },
            StackValueType::Cell => {
                let cell = value
                    .as_cell()
                    .ok_or_else(|| Error::custom("invalid cell"))?;
                ("cell", CellBytes { bytes: cell }).serialize(serializer)
            }
            StackValueType::Slice => {
                let slice = value
                    .as_cell_slice()
                    .ok_or_else(|| Error::custom("invalid slice"))?;

                let built;
                let cell = if slice.range().is_full(slice.cell().as_ref()) {
                    slice.cell()
                } else {
                    built = CellBuilder::build_from(slice.apply()).map_err(Error::custom)?;
                    &built
                };

                ("cell", CellBytes { bytes: cell }).serialize(serializer)
            }
            StackValueType::Tuple => match value.as_list() {
                Some((head, tail)) => ("list", List(head, tail)).serialize(serializer),
                None => {
                    let tuple = value
                        .as_tuple()
                        .ok_or_else(|| Error::custom("invalid list"))?;
                    ("tuple", Tuple(tuple)).serialize(serializer)
                }
            },
            _ => Err(Error::custom("unsupported stack item")),
        }
    }
}

thread_local! {
    static STACK_ITEMS_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[repr(transparent)]
struct StackValueOutputWrapper<'a>(&'a DynStackValue);

impl Serialize for StackValueOutputWrapper<'_> {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;

        const MAX_DEPTH: usize = 16;

        thread_local! {
            static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        }

        let is_depth_ok = DEPTH.with(|depth| {
            let current_depth = depth.get();
            if current_depth < MAX_DEPTH {
                depth.set(current_depth + 1);
                true
            } else {
                false
            }
        });

        if !is_depth_ok {
            return Err(Error::custom("too deep stack item"));
        }

        scopeguard::defer! {
            DEPTH.with(|depth| {
                depth.set(depth.get() - 1);
            })
        };

        TonlibRunResult::serialize_stack_item(self.0, serializer)
    }
}

type DynStackValue = dyn tycho_vm::StackValue + 'static;

#[derive(Debug, Clone, Copy)]
struct EmptyOk;

impl Serialize for EmptyOk {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("Ok", 1)?;
        s.serialize_field("@type", "ok")?;
        s.end()
    }
}

fn ok_to_response<T: Serialize>(id: String, result: T) -> Response {
    JrpcOkResponse::new(id, result).into_response()
}

fn error_to_response(id: String, e: RpcStateError) -> Response {
    let (code, message) = match e {
        RpcStateError::NotReady => (NOT_READY_CODE, Cow::Borrowed("not ready")),
        RpcStateError::NotSupported => (NOT_SUPPORTED_CODE, Cow::Borrowed("method not supported")),
        RpcStateError::Internal(e) => (INTERNAL_ERROR_CODE, e.to_string().into()),
    };

    JrpcErrorResponse {
        id: Some(id),
        code,
        message,
    }
    .into_response()
}

fn too_large_limit_response(id: String) -> Response {
    JrpcErrorResponse {
        id: Some(id),
        code: TOO_LARGE_LIMIT_CODE,
        message: Cow::Borrowed("limit is too large"),
    }
    .into_response()
}

#[derive(Debug)]
pub enum SerdeInputStackItem {
    Num(num_bigint::BigInt),
    Cell(Cell),
    Slice(Cell),
    Builder(Cell),
}

impl TryFrom<SerdeInputStackItem> for tycho_vm::RcStackValue {
    type Error = everscale_types::error::Error;

    fn try_from(value: SerdeInputStackItem) -> Result<Self, Self::Error> {
        match value {
            SerdeInputStackItem::Num(num) => Ok(tycho_vm::RcStackValue::new_dyn_value(num)),
            SerdeInputStackItem::Cell(cell) => Ok(tycho_vm::RcStackValue::new_dyn_value(cell)),
            SerdeInputStackItem::Slice(cell) => {
                if cell.is_exotic() {
                    return Err(everscale_types::error::Error::UnexpectedExoticCell);
                }
                let slice = tycho_vm::OwnedCellSlice::new_allow_exotic(cell);
                Ok(tycho_vm::RcStackValue::new_dyn_value(slice))
            }
            SerdeInputStackItem::Builder(cell) => {
                let mut b = CellBuilder::new();
                b.store_slice(cell.as_slice_allow_exotic())?;
                Ok(tycho_vm::RcStackValue::new_dyn_value(b))
            }
        }
    }
}

impl<'de> Deserialize<'de> for SerdeInputStackItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        struct IntBounds {
            min: BigInt,
            max: BigInt,
        }

        impl IntBounds {
            fn get() -> &'static Self {
                static BOUNDS: OnceLock<IntBounds> = OnceLock::new();
                BOUNDS.get_or_init(|| Self {
                    min: BigInt::from(-1) << 256,
                    max: (BigInt::from(1) << 256) - 1,
                })
            }

            fn contains(&self, int: &BigInt) -> bool {
                *int >= self.min && *int <= self.max
            }
        }

        struct StackItemVisitor;

        impl<'de> serde::de::Visitor<'de> for StackItemVisitor {
            type Value = SerdeInputStackItem;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a tuple of two items")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                fn map_cell<T, F: FnOnce(Cell) -> T, E: Error>(
                    value: impl AsRef<str>,
                    f: F,
                ) -> Result<T, E> {
                    Boc::decode_base64(value.as_ref())
                        .map(f)
                        .map_err(Error::custom)
                }

                let Some(ty) = seq.next_element()? else {
                    return Err(Error::custom(
                        "expected the first item to be a stack item type",
                    ));
                };

                let Some(serde_helpers::BorrowedStr(value)) = seq.next_element()? else {
                    return Err(Error::custom(
                        "expected the second item to be a stack item value",
                    ));
                };

                if seq
                    .next_element::<&serde_json::value::RawValue>()?
                    .is_some()
                {
                    return Err(Error::custom("too many tuple items"));
                }

                match ty {
                    StackItemType::Num => {
                        const MAX_INT_LEN: usize = 79;

                        if value.len() > MAX_INT_LEN {
                            return Err(Error::invalid_length(
                                value.len(),
                                &"a decimal integer in range [-2^256, 2^256)",
                            ));
                        }

                        let int = BigInt::from_str(value.as_ref()).map_err(Error::custom)?;
                        if !IntBounds::get().contains(&int) {
                            return Err(Error::custom("integer out of bounds"));
                        }
                        Ok(SerdeInputStackItem::Num(int))
                    }
                    StackItemType::Cell => map_cell(value, SerdeInputStackItem::Cell),
                    StackItemType::Slice => map_cell(value, SerdeInputStackItem::Slice),
                    StackItemType::Builder => map_cell(value, SerdeInputStackItem::Builder),
                }
            }
        }

        deserializer.deserialize_seq(StackItemVisitor)
    }
}

#[derive(Deserialize)]
enum StackItemType {
    #[serde(rename = "num")]
    Num,
    #[serde(rename = "tvm.Cell")]
    Cell,
    #[serde(rename = "tvm.Slice")]
    Slice,
    #[serde(rename = "tvm.Builder")]
    Builder,
}

mod serde_option_tonlib_address {
    use super::*;

    pub fn serialize<S>(value: &Option<StdAddr>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Wrapper<'a>(#[serde(with = "serde_tonlib_address")] &'a StdAddr);

        value.as_ref().map(Wrapper).serialize(serializer)
    }
}

mod serde_tonlib_address {
    use everscale_types::models::{StdAddr, StdAddrBase64Repr};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<StdAddr, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        StdAddrBase64Repr::<true>::deserialize(deserializer)
    }

    pub fn serialize<S>(value: &StdAddr, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StdAddrBase64Repr::<true>::serialize(value, serializer)
    }
}

mod serde_option_tonlib_hash {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<HashBytes>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        struct Wrapper(#[serde(with = "serde_tonlib_hash")] HashBytes);

        Option::<Wrapper>::deserialize(deserializer).map(|x| x.map(|Wrapper(x)| x))
    }
}

mod serde_tonlib_hash {
    use std::str::FromStr;

    use serde::de::Error;

    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashBytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serde_helpers::BorrowedStr(s) = <_>::deserialize(deserializer)?;
        HashBytes::from_str(s.as_ref()).map_err(Error::custom)
    }

    pub fn serialize<S>(value: &HashBytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut res = [0u8; 44];
        BASE64_STANDARD
            .encode_slice(value.as_array(), &mut res)
            .unwrap();
        // SAFETY: `res` is guaranteed to contain a valid ASCII base64.
        let res = unsafe { std::str::from_utf8_unchecked(&res) };

        serializer.serialize_str(res)
    }

    pub fn serialize_or_empty<S>(
        value: &Option<HashBytes>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(value) => serialize(value, serializer),
            None => serializer.serialize_str(""),
        }
    }
}

mod serde_boc_or_empty {
    use super::*;

    pub fn serialize<S>(value: &Option<Cell>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(value) => Boc::serialize(value, serializer),
            None => serializer.serialize_str(""),
        }
    }
}

mod serde_method_id {
    use super::*;

    const MAX_METHOD_NAME_LEN: usize = 128;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum MethodId<'a> {
            Int(i64),
            String(#[serde(borrow)] Cow<'a, str>),
        }

        Ok(match <_>::deserialize(deserializer)? {
            MethodId::Int(int) => int,
            MethodId::String(str) => {
                let bytes = str.as_bytes();
                if bytes.len() > MAX_METHOD_NAME_LEN {
                    return Err(Error::custom("method name is too long"));
                }
                everscale_types::crc::crc_16(bytes) as i64 | 0x10000
            }
        })
    }
}
