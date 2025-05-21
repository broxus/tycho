use std::borrow::Cow;
use std::cell::RefCell;
use std::future::Future;

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
use tycho_block_util::message::validate_external_message;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;

use self::models::*;
use crate::endpoint::{get_mime_type, APPLICATION_JSON};
use crate::state::{LoadedAccountState, RpcState, RpcStateError, RunGetMethodPermit};
use crate::util::error_codes::*;
use crate::util::jrpc_extractor::{declare_jrpc_method, Jrpc, JrpcErrorResponse, JrpcOkResponse};

pub mod models;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new()
        .route("/", post(post_jrpc))
        .route("/jsonRPC", post(post_jrpc))
        .route("/getAddressInformation", get(get_address_information))
        .route("/getTransactions", get(get_transactions))
        .route("/sendBoc", post(post_send_boc))
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
        GetAddressInformation(AccountParams),
        GetTransactions(TransactionsParams),
        SendBoc(SendBocParams),
        RunGetMethod(RunGetMethodParams),
    }
}

async fn post_jrpc_impl(State(state): State<RpcState>, req: Jrpc<JrpcId, Method>) -> Response {
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

// === GET /getAddressInformation ===

fn get_address_information(
    State(state): State<RpcState>,
    query: Result<Query<AccountParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => {
            Either::Left(handle_get_address_information(JrpcId::Skip, state, params))
        }
        Err(e) => Either::Right(futures_util::future::ready(error_to_response(
            JrpcId::Skip,
            RpcStateError::BadRequest(e.into()),
        ))),
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

// === GET /getTransactions ===

fn get_transactions(
    State(state): State<RpcState>,
    query: Result<Query<TransactionsParams>, QueryRejection>,
) -> impl Future<Output = Response> {
    match query {
        Ok(Query(params)) => Either::Left(handle_get_transactions(JrpcId::Skip, state, params)),
        Err(e) => Either::Right(futures_util::future::ready(error_to_response(
            JrpcId::Skip,
            RpcStateError::BadRequest(e.into()),
        ))),
    }
}

async fn handle_get_transactions(id: JrpcId, state: RpcState, p: TransactionsParams) -> Response {
    const MAX_LIMIT: u8 = 100;

    if p.limit == 0 || p.lt.unwrap_or(u64::MAX) < p.to_lt {
        return JrpcOkResponse::new(id, [(); 0]).into_response();
    } else if p.limit > MAX_LIMIT {
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

// === POST /sendBoc ===

fn post_send_boc(
    State(state): State<RpcState>,
    body: Result<Json<SendBocParams>, JsonRejection>,
) -> impl Future<Output = Response> {
    match body {
        Ok(Json(params)) => Either::Left(handle_send_boc(JrpcId::Skip, state, params)),
        Err(e) => Either::Right(futures_util::future::ready(error_to_response(
            JrpcId::Skip,
            RpcStateError::BadRequest(e.into()),
        ))),
    }
}

async fn handle_send_boc(id: JrpcId, state: RpcState, p: SendBocParams) -> Response {
    if let Err(e) = validate_external_message(&p.boc).await {
        return JrpcErrorResponse {
            id: Some(id),
            code: INVALID_BOC_CODE,
            message: e.to_string().into(),
        }
        .into_response();
    }

    state.broadcast_external_message(&p.boc).await;
    ok_to_response(id, TonlibOk)
}

// === POST /runGetMethod ===

fn post_run_get_method(
    State(state): State<RpcState>,
    body: Result<Json<RunGetMethodParams>, JsonRejection>,
) -> impl Future<Output = Response> {
    match body {
        Ok(Json(params)) => Either::Left(handle_run_get_method(JrpcId::Skip, state, params)),
        Err(e) => Either::Right(futures_util::future::ready(error_to_response(
            JrpcId::Skip,
            RpcStateError::BadRequest(e.into()),
        ))),
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

// === Helpers ===

fn ok_to_response<T: Serialize>(id: JrpcId, result: T) -> Response {
    JrpcOkResponse::new(id, result).into_response()
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
