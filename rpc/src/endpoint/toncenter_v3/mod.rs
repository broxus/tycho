use std::borrow::Cow;

use anyhow::{anyhow, Context};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use everscale_types::models::{BlockIdShort, IntAddr, IntMsgInfo, MsgType, ShardIdent};
use everscale_types::prelude::*;
use serde::Serialize;
use tycho_storage::{RpcSnapshot, TransactionInfo};

use self::models::{Transaction, *};
use crate::state::{RpcState, RpcStateError};

mod models;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new()
        .route("/masterchainInfo", get(get_masterchain_info))
        .route(
            "/transactionsByMasterchainBlock",
            get(get_transactions_by_mc_block),
        )
        .route("/adjacentTransactions", get(get_adjacent_transactions))
        .route("/transactionsByMessage", get(get_transactions_by_message))
}

// === GET /masterchainInfo ===

async fn get_masterchain_info(State(state): State<RpcState>) -> Result<Response, ErrorResponse> {
    let Some(snapshot) = state.rpc_storage_snapshot() else {
        return Err(RpcStateError::NotReady.into());
    };

    let Some((from_seqno, to_seqno)) = state.get_known_mc_blocks_range(Some(&snapshot))? else {
        return Err(RpcStateError::NotReady.into());
    };

    let get_info = |seqno: u32| {
        let block_id = BlockIdShort {
            shard: ShardIdent::MASTERCHAIN,
            seqno,
        };
        state
            .get_brief_block_info(&block_id, Some(&snapshot))?
            .ok_or_else(|| {
                RpcStateError::Internal(anyhow!(
                    "missing block info for masterchain block {from_seqno}"
                ))
            })
    };

    let (first_block_id, first_info) = get_info(from_seqno)?;
    let (last_block_id, last_info) = get_info(to_seqno)?;

    Ok(ok_to_response(MasterchainInfoResponse {
        last: Block::from_stored(&last_block_id, last_info),
        first: Block::from_stored(&first_block_id, first_info),
    }))
}

// === GET /transactionsByMasterchainBlock ===

async fn get_transactions_by_mc_block(
    State(state): State<RpcState>,
    query: Result<Query<TransactionsByMcBlockRequest>, QueryRejection>,
) -> Result<Response, ErrorResponse> {
    const MAX_LIMIT: usize = 1000;

    let Query(query) = query?;
    if query.limit.get() > MAX_LIMIT {
        return Err(RpcStateError::bad_request(anyhow!(
            "`limit` is too big, at most {MAX_LIMIT} is allowed"
        ))
        .into());
    }

    let Some(snapshot) = state.rpc_storage_snapshot() else {
        return Err(RpcStateError::NotReady.into());
    };

    let Some(block_ids) = state.get_blocks_by_mc_seqno(query.seqno, Some(snapshot.clone()))? else {
        return Err(ErrorResponse::not_found(format!(
            "masterchain block {} not found",
            query.seqno
        )));
    };

    handle_blocking(move || {
        let reverse = query.sort == SortDirection::Desc;

        // Collect blocks in ascending order.
        let mut block_ids = block_ids.collect::<Vec<_>>();
        if reverse {
            // Apply sorting to blocks first.
            block_ids.reverse();
        }

        let mc_seqno = query.seqno;
        let range_from = query.offset;
        let range_to = query.offset + query.limit.get();

        let mut i = 0usize;
        let mut transactions = Vec::new();
        for block_id in block_ids {
            if i >= range_to {
                break;
            }

            let Some(block_transactions) = state.get_block_transactions(
                &block_id.as_short_id(),
                reverse,
                None,
                Some(snapshot.clone()),
            )?
            else {
                return Err(ErrorResponse::internal(anyhow!(
                    "block transactions not found for {block_id}"
                )));
            };

            // NOTE: Transactions are only briefly sorted by LT.
            // A proper sort will require either a separate index
            // or collecting all items first. Neither is optimal, so
            // let's assume that no one is dependent on the true LT order.
            block_transactions
                .map(|account, lt, tx| {
                    if i >= range_to {
                        return None;
                    } else if i < range_from {
                        i += 1;
                        return Some(Ok(()));
                    }

                    let info = TransactionInfo {
                        account: account.clone(),
                        lt,
                        block_id,
                        mc_seqno,
                    };

                    let res = (|| {
                        let cell = Boc::decode(tx)?;
                        transactions.push(Transaction::load_raw(&info, cell.as_ref())?);
                        Ok::<_, anyhow::Error>(())
                    })();

                    i += 1;
                    Some(res)
                })
                .collect::<Result<(), anyhow::Error>>()
                .map_err(RpcStateError::internal)?;
        }

        let mut address_book = AddressBook::default();
        address_book.fill_from_transactions(&transactions);

        Ok(ok_to_response(TransactionsResponse {
            transactions,
            address_book,
        }))
    })
    .await
}

// === GET /adjacentTransactions

async fn get_adjacent_transactions(
    State(state): State<RpcState>,
    query: Result<Query<AdjacentTransactionsRequest>, QueryRejection>,
) -> Result<Response, ErrorResponse> {
    let Query(query) = query?;

    let Some(snapshot) = state.rpc_storage_snapshot() else {
        return Err(RpcStateError::NotReady.into());
    };

    let Some(tx) = state.get_transaction(&query.hash, Some(&snapshot))? else {
        return Err(ErrorResponse::not_found("adjacent transactions not found"));
    };
    let tx: everscale_types::models::Transaction =
        BocRepr::decode(tx).map_err(RpcStateError::internal)?;

    let mut tx_count = 0usize;

    let mut in_msg_source = None;
    let mut out_msg_hashes = Vec::new();

    (|| {
        if query.direction.is_none() || matches!(query.direction, Some(MessageDirection::In)) {
            if let Some(in_msg) = tx.in_msg {
                let mut cs = in_msg.as_slice()?;
                if MsgType::load_from(&mut cs)? == MsgType::Int {
                    let info = IntMsgInfo::load_from(&mut cs)?;
                    if let IntAddr::Std(addr) = info.src {
                        in_msg_source = Some((addr, info.created_lt));
                        tx_count += 1;
                    }
                }
            }
        }

        if query.direction.is_none() || matches!(query.direction, Some(MessageDirection::Out)) {
            for root in tx.out_msgs.values() {
                let msg = root?;
                let mut cs = msg.as_slice()?;
                if MsgType::load_from(&mut cs)? != MsgType::Int {
                    continue;
                }
                let info = IntMsgInfo::load_from(&mut cs)?;
                if !matches!(info.dst, IntAddr::Std(_)) {
                    continue;
                }
                out_msg_hashes.push(*msg.repr_hash());
                tx_count += 1;
            }
        }

        Ok::<_, anyhow::Error>(())
    })()
    .map_err(RpcStateError::internal)?;

    if in_msg_source.is_none() && out_msg_hashes.is_empty() {
        return Ok(axum::Json(TransactionsResponse::default()).into_response());
    }

    handle_blocking(move || {
        let mut transactions = Vec::with_capacity(tx_count);

        fn handle_transaction(
            tx: impl AsRef<[u8]>,
            state: &RpcState,
            snapshot: &RpcSnapshot,
            transactions: &mut Vec<Transaction>,
        ) -> Result<(), RpcStateError> {
            let tx = Boc::decode(tx).map_err(RpcStateError::internal)?;
            let Some(info) = state.get_transaction_info(tx.repr_hash(), Some(snapshot))? else {
                return Err(RpcStateError::Internal(anyhow!(
                    "transaction info not found"
                )));
            };

            transactions.push(
                Transaction::load_raw(&info, tx.as_ref())
                    .context("failed to convert transaction")
                    .map_err(RpcStateError::Internal)?,
            );

            Ok(())
        }

        (|| {
            if let Some((src, message_lt)) = in_msg_source {
                let Some(tx) = state.get_src_transaction(&src, message_lt, Some(&snapshot))? else {
                    return Err(RpcStateError::Internal(anyhow!(
                        "src transaction not found"
                    )));
                };
                handle_transaction(tx, &state, &snapshot, &mut transactions)?;
            }

            for msg_hash in out_msg_hashes {
                let Some(tx) = state.get_dst_transaction(&msg_hash, Some(&snapshot))? else {
                    return Err(RpcStateError::Internal(anyhow!(
                        "dst transaction not found"
                    )));
                };
                handle_transaction(tx, &state, &snapshot, &mut transactions)?;
            }

            Ok::<_, RpcStateError>(())
        })()?;

        let mut address_book = AddressBook::default();
        address_book.fill_from_transactions(&transactions);

        Ok(ok_to_response(TransactionsResponse {
            transactions,
            address_book,
        }))
    })
    .await
}

// === GET /transactionsByMessage ===

async fn get_transactions_by_message(
    State(state): State<RpcState>,
    query: Result<Query<TransactionsByMessageRequest>, QueryRejection>,
) -> Result<Response, ErrorResponse> {
    // Validate query.
    let Query(query) = query?;

    if query.body_hash.is_some() {
        return Err(ErrorResponse::internal(anyhow!(
            "search by `body_hash` is not supported yet"
        )));
    } else if query.opcode.is_some() {
        return Err(ErrorResponse::internal(anyhow!(
            "search by `opcode` is not supported yet"
        )));
    }

    if query.offset >= 2 {
        return Ok(axum::Json(TransactionsResponse::default()).into_response());
    }

    // Get snapshot.
    let Some(snapshot) = state.rpc_storage_snapshot() else {
        return Err(RpcStateError::NotReady.into());
    };

    // Find destination transaction by message.
    let Some(dst_tx_root) = state.get_dst_transaction(&query.msg_hash, Some(&snapshot))? else {
        return Err(ErrorResponse::not_found("adjacent transactions not found"));
    };
    let dst_tx_root = Boc::decode(dst_tx_root).map_err(RpcStateError::internal)?;

    handle_blocking(move || {
        let mut transactions = Vec::with_capacity(2);

        let mut in_msg_source = None;

        // Try to handle destination transaction first.
        let both_directions = query.direction.is_none();
        if both_directions || matches!(query.direction, Some(MessageDirection::Out)) {
            // Fully parse destination transaction.
            let Some(info) =
                state.get_transaction_info(dst_tx_root.repr_hash(), Some(&snapshot))?
            else {
                return Err(ErrorResponse::internal(anyhow!(
                    "transaction info not found"
                )));
            };

            let out_tx = Transaction::load_raw(&info, dst_tx_root.as_ref())
                .context("failed to convert transaction")
                .map_err(RpcStateError::Internal)?;

            if both_directions {
                // Get message info to find its source transaction.
                if let Some(Message {
                    source: Some(source),
                    created_lt: Some(created_lt),
                    ..
                }) = &out_tx.in_msg
                {
                    in_msg_source = Some((source.clone(), *created_lt));
                }
            }

            // Add to the response.
            transactions.push(out_tx);
        } else {
            (|| {
                // Partially parse destination transaction.
                let tx = dst_tx_root.parse::<everscale_types::models::Transaction>()?;
                // Parse message info to find its source transaction.
                if let Some(in_msg) = tx.in_msg {
                    let mut cs = in_msg.as_slice()?;
                    if MsgType::load_from(&mut cs)? == MsgType::Int {
                        let info = IntMsgInfo::load_from(&mut cs)?;
                        if let IntAddr::Std(addr) = info.src {
                            in_msg_source = Some((addr, info.created_lt));
                        }
                    }
                }

                Ok::<_, everscale_types::error::Error>(())
            })()
            .map_err(ErrorResponse::internal)?;
        }

        // Try to find source transaction next.
        if let Some((src, message_lt)) = in_msg_source {
            // Find transaction by message LT.
            let Some(tx) = state.get_src_transaction(&src, message_lt, Some(&snapshot))? else {
                return Err(ErrorResponse::internal(anyhow!(
                    "src transaction not found"
                )));
            };

            // Decode and find transaction info.
            let tx = Boc::decode(tx).map_err(ErrorResponse::internal)?;
            let Some(info) = state.get_transaction_info(tx.repr_hash(), Some(&snapshot))? else {
                return Err(ErrorResponse::internal(anyhow!(
                    "transaction info not found"
                )));
            };

            // Add to the response.
            transactions.push(
                Transaction::load_raw(&info, tx.as_ref())
                    .context("failed to convert transaction")
                    .map_err(ErrorResponse::internal)?,
            );
        }

        // Apply transaction sort.
        if query.sort == SortDirection::Asc {
            transactions.reverse();
        }

        // Apply limit/offset
        if query.offset >= transactions.len() {
            transactions.clear();
        } else {
            transactions.drain(..query.offset);
        }
        transactions.truncate(query.limit.get());

        // Build response.
        let mut address_book = AddressBook::default();
        address_book.fill_from_transactions(&transactions);

        Ok(ok_to_response(TransactionsResponse {
            transactions,
            address_book,
        }))
    })
    .await
}

// === Helpers ===

fn ok_to_response<T: Serialize>(result: T) -> Response {
    axum::Json(result).into_response()
}

async fn handle_blocking<F>(f: F) -> Result<Response, ErrorResponse>
where
    F: FnOnce() -> Result<Response, ErrorResponse> + Send + 'static,
{
    // TODO: Use a separate pool of blocking tasks?
    let handle = tokio::task::spawn_blocking(f);

    match handle.await {
        Ok(res) => res,
        Err(_) => Err(ErrorResponse {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            error: "request cancelled".into(),
        }),
    }
}

#[derive(Debug)]
struct ErrorResponse {
    status_code: StatusCode,
    error: Cow<'static, str>,
}

impl ErrorResponse {
    fn internal<E: Into<anyhow::Error>>(error: E) -> Self {
        RpcStateError::Internal(error.into()).into()
    }

    fn not_found<T: Into<Cow<'static, str>>>(msg: T) -> Self {
        Self {
            status_code: StatusCode::NOT_FOUND,
            error: msg.into(),
        }
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Response<'a> {
            error: &'a str,
        }

        IntoResponse::into_response((
            self.status_code,
            axum::Json(Response { error: &self.error }),
        ))
    }
}

impl From<RpcStateError> for ErrorResponse {
    fn from(value: RpcStateError) -> Self {
        let (status_code, error) = match value {
            RpcStateError::NotReady => {
                (StatusCode::SERVICE_UNAVAILABLE, Cow::Borrowed("not ready"))
            }
            RpcStateError::NotSupported => (
                StatusCode::NOT_IMPLEMENTED,
                Cow::Borrowed("method not supported"),
            ),
            RpcStateError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string().into()),
            RpcStateError::BadRequest(e) => (StatusCode::BAD_REQUEST, e.to_string().into()),
        };

        Self { status_code, error }
    }
}

impl From<QueryRejection> for ErrorResponse {
    fn from(value: QueryRejection) -> Self {
        Self::from(RpcStateError::BadRequest(value.into()))
    }
}
