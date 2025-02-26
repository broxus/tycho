use axum::extract::{Path, State};
use axum::routing::get;
use axum::Json;
use everscale_types::models::StdAddr;

use super::responses::{status_to_string, GetAccountResponse};
use crate::endpoint::error::{Error, Result};
use crate::state::LoadedAccountState;
use crate::RpcState;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new().route("/accounts/:account", get(get_account))
}

async fn get_account(
    Path(address): Path<StdAddr>,
    State(state): State<RpcState>,
) -> Result<Json<GetAccountResponse>> {
    let item = state.get_account_state(&address)?;

    match &item {
        &LoadedAccountState::NotFound { .. } => Err(Error::NotFound("account not found")),
        LoadedAccountState::Found { state, .. } => {
            let account = state.load_account()?;
            match account {
                Some(loaded) => {
                    Ok(Json(GetAccountResponse {
                        address: address.to_string(),
                        balance: loaded.balance.tokens.into_inner() as u64,
                        extra_balance: None, // TODO: fill with correct extra balance
                        status: status_to_string(loaded.state.status()),
                        currencies_balance: Some(
                            loaded
                                .balance
                                .other
                                .as_dict()
                                .iter()
                                .filter_map(|res| res.ok())
                                .map(|(id, balance)| (id, balance.as_usize() as u64))
                                .collect(),
                        ),
                        last_activity: loaded.last_trans_lt,
                        interfaces: None,    // TODO: fill with correct interfaces,
                        name: None,          // TODO: fill with correct name,
                        is_scam: None,       // TODO: fill with correct is_scam,
                        icon: None,          // TODO: fill with correct icon,
                        memo_required: None, // TODO: fill with correct memo_required,
                        get_methods: vec![], // TODO: fill with correct get_methods,
                        is_suspended: None,  // TODO: fill with correct is_suspended,
                        is_wallet: true,     // TODO: fill with correct is_wallet,
                    }))
                }
                None => Err(Error::NotFound("account not found")),
            }
        }
    }
}
