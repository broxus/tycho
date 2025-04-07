use std::borrow::Cow;
use std::cell::RefCell;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use everscale_types::models::*;
use everscale_types::num::Tokens;
use everscale_types::prelude::*;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use tycho_block_util::message::validate_external_message;
use tycho_util::metrics::HistogramGuard;
use tycho_util::sync::rayon_run;

use self::models::*;
use crate::state::{LoadedAccountState, RpcState, RpcStateError, RunGetMethodPermit};
use crate::util::error_codes::*;
use crate::util::jrpc_extractor::{declare_jrpc_method, Jrpc, JrpcErrorResponse, JrpcOkResponse};

pub mod models;

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
    ok_to_response(id, TonlibOk)
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
