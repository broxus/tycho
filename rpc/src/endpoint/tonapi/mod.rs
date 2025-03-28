use crate::RpcState;

mod accounts;
mod blockchain;
mod requests;
mod responses;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new()
        .nest("/accounts", accounts::router())
        .nest("/blockchain", blockchain::router())
}
