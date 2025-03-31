use crate::RpcState;

mod accounts;
mod blockchain;
mod error;
mod requests;
mod responses;
mod utils;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new()
        .nest("/accounts", accounts::router())
        .nest("/blockchain", blockchain::router())
}
