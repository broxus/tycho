use std::borrow::Cow;
use std::cell::RefCell;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use base64::prelude::{Engine as _, BASE64_STANDARD};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use rand::Rng;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use tycho_block_util::message::{validate_external_message, ExtMsgRepr};
use tycho_storage::TransactionsIterBuilder;
use tycho_util::metrics::HistogramGuard;
use tycho_util::serde_helpers::{self, Base64BytesWithLimit};

use crate::state::{LoadedAccountState, RpcState, RpcStateError};
use crate::util::error_codes::*;
use crate::util::jrpc_extractor::{declare_jrpc_method, Jrpc, JrpcErrorResponse, JrpcOkResponse};

declare_jrpc_method! {
    pub enum MethodParams: Method {
        GetAddressInformation(AccountParams),
        GetTransactions(TransactionsParams),
        SendBoc(SendBocParams),
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
            mc_block_id,
            state,
            gen_utime,
            mc_ref_handle,
        } => {
            last_transaction_id =
                TonlibTransactionId::new(state.last_trans_lt, state.last_trans_hash);
            block_id = TonlibBlockId::from(*mc_block_id);
            sync_utime = gen_utime;

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
    #[serde(default)]
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

// === Responses ===

#[derive(Serialize)]
pub struct AddressInformationResponse {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    #[serde(with = "serde_helpers::string")]
    pub balance: Tokens,
    pub extra_currencies: [(); 0],
    #[serde(with = "serde_boc_or_empty")]
    pub code: Option<Cell>,
    #[serde(with = "serde_boc_or_empty")]
    pub data: Option<Cell>,
    pub last_transaction_id: TonlibTransactionId,
    pub block_id: TonlibBlockId,
    #[serde(serialize_with = "serde_tonlib_hash::serialize_or_empty")]
    pub frozen_hash: Option<HashBytes>,
    pub sync_utime: u32,
    pub extra: TonlibExtra,
    pub state: TonlibAccountStatus,
}

impl AddressInformationResponse {
    pub const TY: &str = "raw.fullAccountState";
}

#[derive(Serialize)]
pub struct TonlibTransactionId {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    pub lt: u64,
    pub hash: HashBytes,
}

impl TonlibTransactionId {
    pub const TY: &str = "internal.transactionId";

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
pub struct TonlibBlockId {
    #[serde(rename = "@type")]
    pub ty: &'static str,
    pub workchain: i32,
    #[serde(with = "serde_helpers::string")]
    pub shard: i64,
    pub seqno: u32,
    #[serde(with = "serde_tonlib_hash")]
    pub root_hash: HashBytes,
    #[serde(with = "serde_tonlib_hash")]
    pub file_hash: HashBytes,
}

impl TonlibBlockId {
    pub const TY: &str = "ton.blockIdExt";
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
#[serde(rename = "lowercase")]
pub enum TonlibAccountStatus {
    Uninitialized,
    Frozen,
    Active,
}

#[derive(Debug, Clone, Copy)]
pub struct TonlibExtra;

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
            .as_secs_f32();

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
