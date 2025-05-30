use std::borrow::Cow;
use std::cell::RefCell;
use std::future::Future;

use anyhow::anyhow;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Query, Request, State};
use axum::http::status::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, RequestExt};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use futures_util::future::Either;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use tycho_block_util::message::{
    normalize_external_message, parse_external_message, validate_external_message,
};
use tycho_storage::BlockTransactionsCursor;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;

use self::models::*;
use crate::endpoint::{get_mime_type, APPLICATION_JSON};
use crate::state::{
    BadRequestError, LoadedAccountState, RpcState, RpcStateError, RunGetMethodPermit,
};
use crate::util::error_codes::*;
use crate::util::jrpc_extractor::{declare_jrpc_method, Jrpc, JrpcErrorResponse, JrpcOkResponse};

pub mod models;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new()
        .route("/", post(post_jrpc))
        .route("/jsonRPC", post(post_jrpc))
        .route("/getMasterchainInfo", get(get_masterchain_info))
        .route("/getBlockHeader", get(get_block_header))
        .route("/shards", get(get_shards))
        .route("/detectAddress", get(get_detect_address))
        .route("/getAddressInformation", get(get_address_information))
        .route(
            "/getExtendedAddressInformation",
            get(get_extended_address_information),
        )
        .route("/getWalletInformation", get(get_wallet_information))
        .route("/getTransactions", get(get_transactions))
        .route("/getBlockTransactions", get(get_block_transactions))
        .route("/getBlockTransactionsExt", get(get_block_transactions_ext))
        .route("/sendBoc", post(post_send_boc))
        .route("/sendBocReturnHash", post(post_send_boc_return_hash))
        .route("/runGetMethod", post(post_run_get_method))
}

// === POST /jsonRPC ===

async fn post_jrpc(state: State<RpcState>, req: Request) -> Response {
    use axum::http::StatusCode;

    match get_mime_type(&req) {
        Some(mime) if mime.starts_with(APPLICATION_JSON) => match req.extract().await {
            Ok(method) => post_jrpc_impl(state, method).await,
            Err(e) => e.into_response(),
        },
        _ => StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response(),
    }
}

declare_jrpc_method! {
    pub enum MethodParams: Method {
        GetMasterchainInfo(EmptyParams),
        GetBlockHeader(BlockHeaderParams),
        Shards(GetShardsParams),
        DetectAddress(DetectAddressParams),
        GetAddressInformation(AccountParams),
        GetExtendedAddressInformation(AccountParams),
        GetWalletInformation(AccountParams),
        GetTransactions(TransactionsParams),
        GetBlockTransactions(BlockTransactionsParams),
        GetBlockTransactionsExt(BlockTransactionsParams),
        SendBoc(SendBocParams),
        SendBocReturnHash(SendBocParams),
        RunGetMethod(RunGetMethodParams),
    }
}

async fn post_jrpc_impl(State(state): State<RpcState>, req: Jrpc<JrpcId, Method>) -> Response {
    let label = [("method", req.method)];
    let _hist = HistogramGuard::begin_with_labels("tycho_jrpc_request_time", &label);
    match req.params {
        MethodParams::GetMasterchainInfo(_) => handle_get_masterchain_info(req.id, state).await,
        MethodParams::GetBlockHeader(p) => handle_get_block_header(req.id, state, p).await,
        MethodParams::Shards(p) => handle_get_shards(req.id, state, p).await,
        MethodParams::DetectAddress(p) => handle_detect_address(req.id, p).await,
        MethodParams::GetAddressInformation(p) => {
            handle_get_address_information(req.id, state, p).await
        }
        MethodParams::GetExtendedAddressInformation(p) => {
            handle_get_extended_address_information(req.id, state, p).await
        }
        MethodParams::GetWalletInformation(p) => {
            handle_get_wallet_information(req.id, state, p).await
        }
        MethodParams::GetTransactions(p) => handle_get_transactions(req.id, state, p).await,
        MethodParams::GetBlockTransactions(p) => {
            handle_get_block_transactions(req.id, state, p).await
        }
        MethodParams::GetBlockTransactionsExt(p) => {
            handle_get_block_transactions_ext(req.id, state, p).await
        }
        MethodParams::SendBoc(p) => handle_send_boc(req.id, state, p).await,
        MethodParams::SendBocReturnHash(p) => handle_send_boc_return_hash(req.id, state, p).await,
        MethodParams::RunGetMethod(p) => handle_run_get_method(req.id, state, p).await,
    }
}

// === GET /getMasterchainInfo ===

fn get_masterchain_info(State(state): State<RpcState>) -> impl Future<Output = Response> {
    handle_get_masterchain_info(JrpcId::Skip, state)
}

async fn handle_get_masterchain_info(id: JrpcId, state: RpcState) -> Response {
    let info = state.get_latest_mc_info();
    let zerostate_id = state.zerostate_id();
    ok_to_response(id, MasterchainInfoResponse {
        ty: MasterchainInfoResponse::TY,
        last: TonlibBlockId::from(*info.block_id),
        state_root_hash: info.state_hash,
        init: TonlibBlockId {
            ty: TonlibBlockId::TY,
            workchain: -1,
            shard: ShardIdent::PREFIX_FULL as i64,
            seqno: 0,
            root_hash: zerostate_id.root_hash,
            file_hash: zerostate_id.file_hash,
        },
        extra: TonlibExtra,
    })
}

// === GET /getBlockHeader ===

fn get_block_header(
    State(state): State<RpcState>,
    query: Result<Query<BlockHeaderParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => Either::Left(handle_get_block_header(JrpcId::Skip, state, params)),
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_get_block_header(id: JrpcId, state: RpcState, p: BlockHeaderParams) -> Response {
    let Some(shard) = ShardIdent::new(p.workchain as i32, p.shard as u64) else {
        return error_to_response(
            id,
            RpcStateError::bad_request(anyhow!("invalid shard prefix")),
        );
    };

    let (block_id, info) = match state.get_brief_block_info(
        &BlockIdShort {
            shard,
            seqno: p.seqno,
        },
        None,
    ) {
        Ok(Some(info)) => info,
        Ok(None) => {
            let e = RpcStateError::Internal(anyhow!("block {} not found", p.seqno));
            return error_to_response(id, e);
        }
        Err(e) => return error_to_response(id, e),
    };

    let mut is_block_id_ok = true;
    if let Some(root_hash) = &p.root_hash {
        is_block_id_ok &= block_id.root_hash == *root_hash;
    }
    if let Some(file_hash) = &p.file_hash {
        is_block_id_ok &= block_id.file_hash == *file_hash;
    }

    if !is_block_id_ok {
        return error_to_response(
            id,
            RpcStateError::bad_request(anyhow!(
                "block root or file hash mismatch, \
                better don't specify them anyway"
            )),
        );
    }

    ok_to_response(id, BlockHeaderResponse {
        ty: BlockHeaderResponse::TY,
        id: TonlibBlockId::from(block_id),
        global_id: info.global_id,
        version: info.version,
        flags: info.flags,
        after_merge: info.after_merge,
        after_split: info.after_split,
        before_split: info.before_split,
        want_merge: info.want_merge,
        want_split: info.want_split,
        validator_list_hash_short: info.validator_list_hash_short,
        catchain_seqno: info.catchain_seqno,
        min_ref_mc_seqno: info.min_ref_mc_seqno,
        is_key_block: info.is_key_block,
        prev_key_block_seqno: info.prev_key_block_seqno,
        start_lt: info.start_lt,
        end_lt: info.end_lt,
        gen_utime: info.gen_utime,
        vert_seqno: info.vert_seqno,
        prev_blocks: info
            .prev_blocks
            .into_iter()
            .map(TonlibBlockId::from)
            .collect(),
        extra: TonlibExtra,
    })
}

// === GET /shards ===

fn get_shards(
    State(state): State<RpcState>,
    query: Result<Query<GetShardsParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => Either::Left(handle_get_shards(JrpcId::Skip, state, params)),
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_get_shards(id: JrpcId, state: RpcState, p: GetShardsParams) -> Response {
    let shards = match state.get_brief_shards_descr(p.seqno, None) {
        Ok(Some(shards)) => shards,
        Ok(None) => {
            let e = RpcStateError::Internal(anyhow!("masterchain block {} not found", p.seqno));
            return error_to_response(id, e);
        }
        Err(e) => return error_to_response(id, e),
    };

    ok_to_response(id, ShardsResponse {
        ty: ShardsResponse::TY,
        shards: &shards,
        extra: TonlibExtra,
    })
}

// === GET /detectAddress ===

fn get_detect_address(
    query: Result<Query<DetectAddressParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => Either::Left(handle_detect_address(JrpcId::Skip, params)),
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_detect_address(id: JrpcId, p: DetectAddressParams) -> Response {
    let (test_only, given_type) = match p.base64_flags {
        None => (false, AddressType::RawForm),
        Some(flags) => (
            flags.testnet,
            if flags.bounceable {
                AddressType::FriendlyBounceable
            } else {
                AddressType::FriendlyNonBounceable
            },
        ),
    };

    ok_to_response(id, AddressFormsResponse {
        raw_form: p.address,
        given_type,
        test_only,
    })
}

// === GET /getAddressInformation ===

fn get_address_information(
    State(state): State<RpcState>,
    query: Result<Query<AccountParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => {
            Either::Left(handle_get_address_information(JrpcId::Skip, state, params))
        }
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_get_address_information(id: JrpcId, state: RpcState, p: AccountParams) -> Response {
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

// === GET /getExtendedAddressInformation ===

fn get_extended_address_information(
    State(state): State<RpcState>,
    query: Result<Query<AccountParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => Either::Left(handle_get_extended_address_information(
            JrpcId::Skip,
            state,
            params,
        )),
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_get_extended_address_information(
    id: JrpcId,
    state: RpcState,
    p: AccountParams,
) -> Response {
    let item = match state.get_account_state(&p.address) {
        Ok(item) => item,
        Err(e) => return error_to_response(id, e),
    };

    let mut balance = Tokens::ZERO;
    let mut parsed = ParsedAccountState::Uninit { frozen_hash: None };
    let last_transaction_id;
    let block_id;
    let sync_utime;
    let mut revision = 0;

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
                            if let Some(res) =
                                account.code.as_ref().zip(account.data.as_ref()).and_then(
                                    |(code, data)| {
                                        let info = BasicContractInfo::guess(code.repr_hash())?;
                                        let parsed = info.read_init_data(data.as_ref()).ok()?;
                                        Some((info.revision, parsed))
                                    },
                                )
                            {
                                revision = res.0;
                                parsed = res.1;
                            } else {
                                parsed = ParsedAccountState::Raw {
                                    code: account.code,
                                    data: account.data,
                                    frozen_hash: None,
                                }
                            }
                        }
                        AccountState::Frozen(hash) => {
                            parsed = ParsedAccountState::Uninit {
                                frozen_hash: Some(hash),
                            }
                        }
                        AccountState::Uninit => {}
                    }
                }
                Ok(None) => {}
                Err(e) => return error_to_response(id, RpcStateError::Internal(e.into())),
            }
        }
    }

    ok_to_response(id, ExtendedAddressInformationResponse {
        ty: ExtendedAddressInformationResponse::TY,
        address: TonlibAddress::new(&p.address),
        balance,
        extra_currencies: [],
        last_transaction_id,
        block_id,
        sync_utime,
        account_state: parsed,
        revision,
        extra: TonlibExtra,
    })
}

// === GET /getWalletInformation ===

fn get_wallet_information(
    State(state): State<RpcState>,
    query: Result<Query<AccountParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => {
            Either::Left(handle_get_wallet_information(JrpcId::Skip, state, params))
        }
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_get_wallet_information(id: JrpcId, state: RpcState, p: AccountParams) -> Response {
    let item = match state.get_account_state(&p.address) {
        Ok(item) => item,
        Err(e) => return error_to_response(id, e),
    };

    let mut last_transaction_id = TonlibTransactionId::default();
    let mut balance = Tokens::ZERO;
    let mut fields = None::<WalletFields>;
    let mut account_state = TonlibAccountStatus::Uninitialized;

    let _mc_ref_handle;
    if let LoadedAccountState::Found {
        state,
        mc_ref_handle,
        ..
    } = item
    {
        last_transaction_id = TonlibTransactionId::new(state.last_trans_lt, state.last_trans_hash);

        match state.load_account() {
            Ok(Some(loaded)) => {
                _mc_ref_handle = mc_ref_handle;

                balance = loaded.balance.tokens;
                match loaded.state {
                    AccountState::Active(state) => {
                        account_state = TonlibAccountStatus::Active;
                        if let (Some(code), Some(data)) = (state.code, state.data) {
                            fields = WalletFields::load_from(code.as_ref(), data.as_ref()).ok();
                        }
                    }
                    AccountState::Frozen(..) => {
                        account_state = TonlibAccountStatus::Frozen;
                    }
                    AccountState::Uninit => {}
                }
            }
            Ok(None) => {}
            Err(e) => return error_to_response(id, RpcStateError::Internal(e.into())),
        }
    }

    ok_to_response(id, WalletInformationResponse {
        wallet: fields.is_some(),
        balance,
        extra_currencies: [(); 0],
        account_state,
        fields,
        last_transaction_id,
    })
}

// === GET /getTransactions ===

fn get_transactions(
    State(state): State<RpcState>,
    query: Result<Query<TransactionsParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => Either::Left(handle_get_transactions(JrpcId::Skip, state, params)),
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_get_transactions(id: JrpcId, state: RpcState, p: TransactionsParams) -> Response {
    const MAX_LIMIT: u8 = 100;

    if p.limit == 0 || p.lt.unwrap_or(u64::MAX) < p.to_lt {
        return JrpcOkResponse::new(id, [(); 0]).into_response();
    } else if p.limit > MAX_LIMIT {
        return too_large_limit_response(id);
    }

    match state.get_transactions(&p.address, p.lt, p.to_lt, None) {
        Ok(list) => ok_to_response(id, GetTransactionsResponse {
            address: &p.address,
            list: RefCell::new(Some(list)),
            limit: p.limit,
            to_lt: p.to_lt,
        }),
        Err(e) => error_to_response(id, e),
    }
}

// === GET /getBlockTransactions ===

fn get_block_transactions(
    State(state): State<RpcState>,
    query: Result<Query<BlockTransactionsParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => {
            Either::Left(handle_get_block_transactions(JrpcId::Skip, state, params))
        }
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_get_block_transactions(
    id: JrpcId,
    state: RpcState,
    p: BlockTransactionsParams,
) -> Response {
    const MAX_LIMIT: u8 = 100;

    let Some(shard) = ShardIdent::new(p.workchain as i32, p.shard as u64) else {
        return error_to_response(
            id,
            RpcStateError::bad_request(anyhow!("invalid shard prefix")),
        );
    };
    let block_id = BlockIdShort {
        shard,
        seqno: p.seqno,
    };
    let cursor = if p.after_hash.is_some() || p.after_lt.is_some() {
        Some(BlockTransactionsCursor {
            hash: p.after_hash.unwrap_or_default(),
            lt: p.after_lt.unwrap_or_default(),
        })
    } else {
        None
    };

    let iter = match state.get_block_transaction_ids(&block_id, cursor.as_ref(), None) {
        Ok(Some(iter)) => iter,
        Ok(None) => {
            let e = RpcStateError::Internal(anyhow!("block {block_id} not found"));
            return error_to_response(id, e);
        }
        Err(e) => return error_to_response(id, e),
    };

    let mut is_block_id_ok = true;
    if let Some(root_hash) = &p.root_hash {
        is_block_id_ok &= iter.block_id().root_hash == *root_hash;
    }
    if let Some(file_hash) = &p.file_hash {
        is_block_id_ok &= iter.block_id().file_hash == *file_hash;
    }

    if !is_block_id_ok {
        return error_to_response(
            id,
            RpcStateError::bad_request(anyhow!(
                "block root or file hash mismatch, \
                better don't specify them anyway"
            )),
        );
    }

    ok_to_large_response(id, GetBlockTransactionsResponse {
        req_count: std::cmp::min(p.count.get(), MAX_LIMIT),
        transactions: RefCell::new(Some(iter)),
    })
    .await
}

// === GET /get_block_transactions_ext ===

fn get_block_transactions_ext(
    State(state): State<RpcState>,
    query: Result<Query<BlockTransactionsParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => Either::Left(handle_get_block_transactions_ext(
            JrpcId::Skip,
            state,
            params,
        )),
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_get_block_transactions_ext(
    id: JrpcId,
    state: RpcState,
    p: BlockTransactionsParams,
) -> Response {
    const MAX_LIMIT: u8 = 100;

    let Some(shard) = ShardIdent::new(p.workchain as i32, p.shard as u64) else {
        return error_to_response(
            id,
            RpcStateError::bad_request(anyhow!("invalid shard prefix")),
        );
    };
    let block_id = BlockIdShort {
        shard,
        seqno: p.seqno,
    };
    let cursor = if p.after_hash.is_some() || p.after_lt.is_some() {
        Some(BlockTransactionsCursor {
            hash: p.after_hash.unwrap_or_default(),
            lt: p.after_lt.unwrap_or_default(),
        })
    } else {
        None
    };

    let iter = match state.get_block_transactions(&block_id, cursor.as_ref(), None) {
        Ok(Some(iter)) => iter,
        Ok(None) => {
            let e = RpcStateError::Internal(anyhow!("block {block_id} not found"));
            return error_to_response(id, e);
        }
        Err(e) => return error_to_response(id, e),
    };

    let mut is_block_id_ok = true;
    if let Some(root_hash) = &p.root_hash {
        is_block_id_ok &= iter.block_id().root_hash == *root_hash;
    }
    if let Some(file_hash) = &p.file_hash {
        is_block_id_ok &= iter.block_id().file_hash == *file_hash;
    }

    if !is_block_id_ok {
        return error_to_response(
            id,
            RpcStateError::bad_request(anyhow!(
                "block root or file hash mismatch, \
                better don't specify them anyway"
            )),
        );
    }

    ok_to_large_response(id, GetBlockTransactionsExtResponse {
        req_count: std::cmp::min(p.count.get(), MAX_LIMIT),
        transactions: RefCell::new(Some(iter)),
    })
    .await
}

// === POST /sendBoc ===

fn post_send_boc(
    State(state): State<RpcState>,
    body: Result<Json<SendBocParams>, JsonRejection>,
) -> impl Future<Output = Response> {
    match body {
        Ok(Json(params)) => Either::Left(handle_send_boc(JrpcId::Skip, state, params)),
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_send_boc(id: JrpcId, state: RpcState, p: SendBocParams) -> Response {
    if let Err(e) = validate_external_message(&p.boc).await {
        return into_err_response(StatusCode::BAD_REQUEST, JrpcErrorResponse {
            id: Some(id),
            code: INVALID_BOC_CODE,
            message: e.to_string().into(),
        });
    }

    state.broadcast_external_message(&p.boc).await;
    ok_to_response(id, TonlibOk)
}

// === POST /sendBocReturnHash ===

fn post_send_boc_return_hash(
    State(state): State<RpcState>,
    body: Result<Json<SendBocParams>, JsonRejection>,
) -> impl Future<Output = Response> {
    match body {
        Ok(Json(params)) => Either::Left(handle_send_boc_return_hash(JrpcId::Skip, state, params)),
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_send_boc_return_hash(id: JrpcId, state: RpcState, p: SendBocParams) -> Response {
    let (hash, hash_norm) = match parse_external_message(&p.boc).await.and_then(|root| {
        let normalized = normalize_external_message(root.as_ref())?;
        Ok((*root.repr_hash(), *normalized.repr_hash()))
    }) {
        Ok(res) => res,
        Err(e) => {
            return into_err_response(StatusCode::BAD_REQUEST, JrpcErrorResponse {
                id: Some(id),
                code: INVALID_BOC_CODE,
                message: e.to_string().into(),
            })
        }
    };

    state.broadcast_external_message(&p.boc).await;
    ok_to_response(id, ExtMsgInfoResponse {
        ty: ExtMsgInfoResponse::TY,
        hash,
        hash_norm,
        extra: TonlibExtra,
    })
}

// === POST /runGetMethod ===

fn post_run_get_method(
    State(state): State<RpcState>,
    body: Result<Json<RunGetMethodParams>, JsonRejection>,
) -> impl Future<Output = Response> {
    match body {
        Ok(Json(params)) => Either::Left(handle_run_get_method(JrpcId::Skip, state, params)),
        Err(e) => Either::Right(handle_rejection(e)),
    }
}

async fn handle_run_get_method(id: JrpcId, state: RpcState, p: RunGetMethodParams) -> Response {
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
            return into_err_response(StatusCode::NOT_IMPLEMENTED, JrpcErrorResponse {
                id: Some(id),
                code: NOT_SUPPORTED_CODE,
                message: "method disabled".into(),
            });
        }
        RunGetMethodPermit::Timeout => {
            return into_err_response(StatusCode::REQUEST_TIMEOUT, JrpcErrorResponse {
                id: Some(id),
                code: TIMEOUT_CODE,
                message: "timeout while waiting for VM slot".into(),
            });
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
                return Ok(RunGetMethodResponse {
                    ty: RunGetMethodResponse::TY,
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
            .require_ton_v11();

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

        Ok::<_, RunMethodError>(RunGetMethodResponse {
            ty: RunGetMethodResponse::TY,
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
                RunGetMethodResponse::set_items_limit(max_response_stack_items);
                ok_to_response(id, res)
            }
            Err(RunMethodError::RpcError(e)) => error_to_response(id, e),
            Err(RunMethodError::InvalidParams(e)) => {
                into_err_response(StatusCode::BAD_REQUEST, JrpcErrorResponse {
                    id: Some(id),
                    code: INVALID_PARAMS_CODE,
                    message: format!("invalid stack item: {e}").into(),
                })
            }
            Err(RunMethodError::Internal(e)) => {
                error_to_response(id, RpcStateError::Internal(e.into()))
            }
            Err(RunMethodError::TooBigStack(n)) => error_to_response(
                id,
                RpcStateError::Internal(anyhow!("response stack is too big to send: {n}")),
            ),
        };

        // NOTE: Make sure that permit lives as long as the execution.
        drop(permit);

        res
    })
    .await
}

// === Helpers ===

fn handle_rejection<T: Into<BadRequestError>>(e: T) -> futures_util::future::Ready<Response> {
    futures_util::future::ready(error_to_response(
        JrpcId::Skip,
        RpcStateError::BadRequest(e.into()),
    ))
}

fn ok_to_response<T: Serialize>(id: JrpcId, result: T) -> Response {
    JrpcOkResponse::new(id, result).into_response()
}

async fn ok_to_large_response<T: Serialize + Send + 'static>(id: JrpcId, result: T) -> Response {
    // TODO: Use a separate pool of blocking tasks?
    let handle = tokio::task::spawn_blocking({
        let id = id.clone();
        move || ok_to_response(id, result)
    });

    match handle.await {
        Ok(res) => res,
        Err(_) => error_to_response(id, RpcStateError::Internal(anyhow!("request cancelled"))),
    }
}

fn error_to_response(id: JrpcId, e: RpcStateError) -> Response {
    let (status_code, code, message) = match e {
        RpcStateError::NotReady => (
            StatusCode::SERVICE_UNAVAILABLE,
            NOT_READY_CODE,
            Cow::Borrowed("not ready"),
        ),
        RpcStateError::NotSupported => (
            StatusCode::NOT_IMPLEMENTED,
            NOT_SUPPORTED_CODE,
            Cow::Borrowed("method not supported"),
        ),
        RpcStateError::Internal(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            INTERNAL_ERROR_CODE,
            e.to_string().into(),
        ),
        RpcStateError::BadRequest(e) => (
            StatusCode::BAD_REQUEST,
            INVALID_PARAMS_CODE,
            e.to_string().into(),
        ),
    };

    into_err_response(status_code, JrpcErrorResponse {
        id: Some(id),
        code,
        message,
    })
}

fn too_large_limit_response(id: JrpcId) -> Response {
    into_err_response(StatusCode::BAD_REQUEST, JrpcErrorResponse {
        id: Some(id),
        code: TOO_LARGE_LIMIT_CODE,
        message: Cow::Borrowed("limit is too large"),
    })
}

fn into_err_response(mut status_code: StatusCode, mut res: JrpcErrorResponse<JrpcId>) -> Response {
    if matches!(&res.id, Some(JrpcId::Skip)) {
        res.code = status_code.as_u16() as i32;
    } else {
        status_code = StatusCode::OK;
    }
    (status_code, res).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_wallet_v4() {
        let wallet = Boc::decode_base64("te6ccgECFgEAAzoAAm6AGE3DuCEqSX0GkrvxdALj9iNdEFOQuNKSjSq5im7DoelEWQschn68LPAAAbC9fDcKGgD4sgMmAgEAUQAAAAEpqaMXHSwYXPOWkRNYxRHGUjggtZsD/elIXIA9ob2KZ9cZI6JAART/APSkE/S88sgLAwIBIAkEBPjygwjXGCDTH9Mf0x8C+CO78mTtRNDTH9Mf0//0BNFRQ7ryoVFRuvKiBfkBVBBk+RDyo/gAJKTIyx9SQMsfUjDL/1IQ9ADJ7VT4DwHTByHAAJ9sUZMg10qW0wfUAvsA6DDgIcAB4wAhwALjAAHAA5Ew4w0DpMjLHxLLH8v/CAcGBQAK9ADJ7VQAbIEBCNcY+gDTPzBSJIEBCPRZ8qeCEGRzdHJwdIAYyMsFywJQBc8WUAP6AhPLassfEss/yXP7AABwgQEI1xj6ANM/yFQgR4EBCPRR8qeCEG5vdGVwdIAYyMsFywJQBs8WUAT6AhTLahLLH8s/yXP7AAIAbtIH+gDU1CL5AAXIygcVy//J0Hd0gBjIywXLAiLPFlAF+gIUy2sSzMzJc/sAyEAUgQEI9FHypwICAUgTCgIBIAwLAFm9JCtvaiaECAoGuQ+gIYRw1AgIR6STfSmRDOaQPp/5g3gSgBt4EBSJhxWfMYQCASAODQARuMl+1E0NcLH4AgFYEg8CASAREAAZrx32omhAEGuQ64WPwAAZrc52omhAIGuQ64X/wAA9sp37UTQgQFA1yH0BDACyMoHy//J0AGBAQj0Cm+hMYALm0AHQ0wMhcbCSXwTgItdJwSCSXwTgAtMfIYIQcGx1Z70ighBkc3RyvbCSXwXgA/pAMCD6RAHIygfL/8nQ7UTQgQFA1yH0BDBcgQEI9ApvoTGzkl8H4AXTP8glghBwbHVnupI4MOMNA4IQZHN0crqSXwbjDRUUAIpQBIEBCPRZMO1E0IEBQNcgyAHPFvQAye1UAXKwjiOCEGRzdHKDHrFwgBhQBcsFUAPPFiP6AhPLassfyz/JgED7AJJfA+IAeAH6APQEMPgnbyIwUAqhIb7y4FCCEHBsdWeDHrFwgBhQBMsFJs8WWPoCGfQAy2kXyx9SYMs/IMmAQPsABg==").unwrap();
        let account = wallet.parse::<Account>().unwrap();

        if let AccountState::Active(state) = &account.state {
            let code = state.code.as_ref().unwrap();
            let data = state.data.as_ref().unwrap();

            let info = BasicContractInfo::guess(code.repr_hash()).unwrap();
            let ParsedAccountState::WalletV4 { wallet_id, seqno } =
                info.read_init_data(data.as_ref()).unwrap()
            else {
                panic!("invalid parsed state ");
            };

            assert_eq!(wallet_id, 698983191);
            assert_eq!(seqno, 1);

            let fields = WalletFields::load_from(code.as_ref(), data.as_ref()).unwrap();
            assert_eq!(fields.wallet_type, WalletType::WalletV4R2);
            assert_eq!(fields.seqno, Some(1));
            assert_eq!(fields.wallet_id, Some(698983191));
            assert_eq!(fields.is_signature_allowed, None);
        }

        println!(
            "{}",
            Boc::encode_base64(CellBuilder::build_from(OptionalAccount(Some(account))).unwrap())
        );
    }
}
