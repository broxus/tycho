use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::sync::OnceLock;

use anyhow::{anyhow, Context};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::{self, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use everscale_types::models::{
    BlockId, BlockIdShort, IntAddr, IntMsgInfo, MsgType, ShardIdent, StdAddr,
};
use everscale_types::prelude::*;
use serde::Serialize;
use tycho_storage::{RpcSnapshot, TransactionInfo};
use tycho_util::FastHashSet;

use self::models::{Transaction, *};
use crate::endpoint::APPLICATION_JSON;
use crate::state::{RpcState, RpcStateError};

mod models;

pub fn router() -> axum::Router<RpcState> {
    axum::Router::new()
        .route("/masterchainInfo", get(get_masterchain_info))
        .route("/transactions", get(get_transactions))
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

// === GET /transactions ===

async fn get_transactions(
    State(state): State<RpcState>,
    query: Result<Query<TransactionsRequest>, QueryRejection>,
) -> Result<Response, ErrorResponse> {
    const MAX_LIMIT: usize = 1000;

    struct Filters {
        by_block: Option<BlockIdShort>,
        by_mc_seqno: Option<u32>,
        by_hash: Option<HashBytes>,
        lt: Option<u64>,
        start_lt: Option<u64>,
        end_lt: Option<u64>,
        account: FastHashSet<StdAddr>,
        exclude_account: FastHashSet<StdAddr>,
        limit: NonZeroUsize,
        offset: usize,
        reverse: bool,
    }

    impl Filters {
        fn contains(&self, info: &TransactionInfo) -> bool {
            if let Some(block_id) = &self.by_block {
                if info.block_id.as_short_id() != *block_id {
                    return false;
                }
            }
            if let Some(mc_seqno) = self.by_mc_seqno {
                if info.mc_seqno != mc_seqno {
                    return false;
                }
            }
            if let Some(lt) = self.lt {
                if info.lt != lt {
                    return false;
                }
            }
            if let Some(start_lt) = self.start_lt {
                if info.lt < start_lt {
                    return false;
                }
            }
            if let Some(end_lt) = self.end_lt {
                if info.lt > end_lt {
                    return false;
                }
            }
            if !self.account.is_empty() && !self.account.contains(&info.account) {
                return false;
            }
            if !self.exclude_account.is_empty() && self.exclude_account.contains(&info.account) {
                return false;
            }
            true
        }
    }

    impl TryFrom<TransactionsRequest> for Filters {
        type Error = anyhow::Error;

        fn try_from(q: TransactionsRequest) -> Result<Self, Self::Error> {
            anyhow::ensure!(
                q.start_utime.is_none() && q.end_utime.is_none(),
                "filter by `start_utime` or `end_utime` is not supported yet"
            );

            let by_block = if q.workchain.is_some() || q.shard.is_some() || q.seqno.is_some() {
                let (Some(workchain), Some(ShardPrefix(prefix)), Some(seqno)) =
                    (q.workchain, q.shard, q.seqno)
                else {
                    anyhow::bail!("`workchain`, `shard` and `seqno` fields must be used together");
                };
                let Some(shard) = ShardIdent::new(workchain, prefix) else {
                    anyhow::bail!("invalid shard prefix");
                };
                Some(BlockIdShort { shard, seqno })
            } else {
                None
            };

            anyhow::ensure!(
                q.limit.get() <= MAX_LIMIT,
                "`limit` is too big, at most {MAX_LIMIT} is allowed"
            );

            Ok(Self {
                by_block,
                by_mc_seqno: q.mc_seqno,
                by_hash: q.hash,
                lt: q.lt,
                start_lt: q.start_lt,
                end_lt: q.end_lt,
                account: q.account,
                exclude_account: q.exclude_account,
                limit: q.limit,
                offset: q.offset,
                reverse: q.sort == SortDirection::Desc,
            })
        }
    }

    let Query(query) = query?;
    let mut query = Filters::try_from(query).map_err(RpcStateError::bad_request)?;

    let Some(snapshot) = state.rpc_storage_snapshot() else {
        return Err(RpcStateError::NotReady.into());
    };

    #[allow(clippy::too_many_arguments)]
    fn handle_block_transaction(
        block_id: &BlockId,
        mc_seqno: u32,
        account: &StdAddr,
        lt: u64,
        tx: &[u8],
        query: &Filters,
        range_from: usize,
        range_to: usize,
        transactions: &mut Vec<Transaction>,
        i: &mut usize,
    ) -> Option<Result<(), anyhow::Error>> {
        if *i >= range_to {
            return None;
        }

        let info = TransactionInfo {
            account: account.clone(),
            lt,
            block_id: *block_id,
            mc_seqno,
        };
        if !query.contains(&info) {
            return Some(Ok(()));
        }

        if *i < range_from {
            *i += 1;
            return Some(Ok(()));
        }

        let res = (|| {
            let cell = Boc::decode(tx)?;
            transactions.push(Transaction::load_raw(&info, cell.as_ref())?);
            Ok::<_, anyhow::Error>(())
        })();

        *i += 1;
        Some(res)
    }

    handle_blocking(move || {
        'empty_lt_range_check: {
            match (query.start_lt, query.end_lt, query.lt) {
                (Some(start_lt), Some(end_lt), _) if start_lt > end_lt => {}
                (Some(start_lt), _, Some(lt)) if lt < start_lt => {}
                (_, Some(end_lt), Some(lt)) if lt > end_lt => {}
                _ => break 'empty_lt_range_check,
            }
            return Ok(ok_no_transactions());
        }

        let range_from = query.offset;
        let range_to = query.offset + query.limit.get();

        let mut transactions = Vec::new();

        if let Some(hash) = &query.by_hash {
            // The simplest case when searching by the exact transactino hash.
            if query.offset > 0 {
                return Ok(ok_no_transactions());
            }
            let Some(tx) = state.get_transaction_ext(hash, None)? else {
                return Ok(ok_no_transactions());
            };
            if !query.contains(&tx.info) {
                return Ok(ok_no_transactions());
            }
            (|| {
                let cell = Boc::decode(tx.data)?;
                transactions.push(Transaction::load_raw(&tx.info, cell.as_ref())?);
                Ok::<_, anyhow::Error>(())
            })()
            .map_err(ErrorResponse::internal)?;
        } else if let Some(block_id) = &query.by_block {
            // Searching for all transactions whithin a single block.
            let Some(iter) =
                state.get_block_transactions(block_id, query.reverse, None, Some(snapshot))?
            else {
                return Err(ErrorResponse::internal(anyhow!(
                    "block transactions not found for {block_id}"
                )));
            };

            // TODO: Should we return an error here?
            if matches!(query.by_mc_seqno, Some(mc_seqno) if mc_seqno != iter.ref_by_mc_seqno()) {
                return Ok(ok_no_transactions());
            }

            let block_id = *iter.block_id();
            let mc_seqno = iter.ref_by_mc_seqno();
            let mut i = 0;

            // NOTE: Transactions are only briefly sorted by LT.
            // A proper sort will require either a separate index
            // or collecting all items first. Neither is optimal, so
            // let's assume that no one is dependent on the true LT order.
            #[allow(clippy::map_collect_result_unit)]
            iter.map(|account, lt, tx| {
                handle_block_transaction(
                    &block_id,
                    mc_seqno,
                    account,
                    lt,
                    tx,
                    &query,
                    range_from,
                    range_to,
                    &mut transactions,
                    &mut i,
                )
            })
            .collect::<Result<(), anyhow::Error>>()
            .map_err(RpcStateError::internal)?;
        } else if let Some(mc_seqno) = query.by_mc_seqno {
            // Searching for all transactions whithin multiple blocks,
            // referenced by a single masterchain block.

            let Some(block_ids) = state.get_blocks_by_mc_seqno(mc_seqno, Some(snapshot.clone()))?
            else {
                return Err(ErrorResponse::not_found(format!(
                    "masterchain block {mc_seqno} not found"
                )));
            };
            let mut block_ids = block_ids.collect::<Vec<_>>();
            if query.reverse {
                block_ids.reverse();
            }

            let mut i = 0usize;
            for block_id in block_ids {
                if i >= range_to {
                    break;
                }

                let Some(iter) = state.get_block_transactions(
                    &block_id.as_short_id(),
                    query.reverse,
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
                #[allow(clippy::map_collect_result_unit)]
                iter.map(|account, lt, tx| {
                    handle_block_transaction(
                        &block_id,
                        mc_seqno,
                        account,
                        lt,
                        tx,
                        &query,
                        range_from,
                        range_to,
                        &mut transactions,
                        &mut i,
                    )
                })
                .collect::<Result<(), anyhow::Error>>()
                .map_err(RpcStateError::internal)?;
            }
        } else if let Some(account) = query.account.iter().next().cloned() {
            if query.account.len() > 1 {
                return Err(RpcStateError::bad_request(anyhow!(
                    "search by account supports only a sinle item in the list"
                ))
                .into());
            }

            if let Some(lt) = query.lt {
                query.start_lt = Some(lt);
                query.end_lt = Some(lt);
            }

            let mut i = 0;
            state
                .get_transactions(
                    &account,
                    query.start_lt,
                    query.end_lt,
                    query.reverse,
                    Some(snapshot.clone()),
                )?
                .map_ext(|lt, hash, tx| {
                    if i >= range_to {
                        return None;
                    }

                    let info = match state.get_transaction_info(hash, Some(&snapshot)) {
                        Ok(Some(info)) => info,
                        Ok(None) => {
                            return Some(Err(RpcStateError::Internal(anyhow!(
                                "no transaction info found for tx: lt={lt}, hash={hash}"
                            ))))
                        }
                        Err(e) => return Some(Err(e)),
                    };
                    if !query.contains(&info) {
                        return Some(Ok(()));
                    }

                    if i < range_from {
                        i += 1;
                        return Some(Ok(()));
                    }

                    let res = (|| {
                        let cell = Boc::decode(tx)?;
                        transactions.push(Transaction::load_raw(&info, cell.as_ref())?);
                        Ok::<_, anyhow::Error>(())
                    })()
                    .map_err(RpcStateError::Internal);

                    i += 1;
                    Some(res)
                })
                .collect::<Result<(), RpcStateError>>()?;
        } else {
            return Err(RpcStateError::bad_request(anyhow!(
                "any of `workchain+shard+seqno`, `mc_seqno`, `hash` or `account` \
                filters is required"
            ))
            .into());
        }

        Ok(ok_to_response(TransactionsResponse::new(transactions)))
    })
    .await
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

            let Some(iter) = state.get_block_transactions(
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
            #[allow(clippy::map_collect_result_unit)]
            iter.map(|account, lt, tx| {
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

        Ok(ok_to_response(TransactionsResponse::new(transactions)))
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
        return Ok(ok_no_transactions());
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

        Ok(ok_to_response(TransactionsResponse::new(transactions)))
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
        return Ok(ok_no_transactions());
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
        Ok(ok_to_response(TransactionsResponse::new(transactions)))
    })
    .await
}

// === Helpers ===

fn ok_no_transactions() -> Response {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = BYTES
        .get_or_init(|| serde_json::to_vec(&TransactionsResponse::default()).unwrap())
        .as_slice();
    (JSON_HEADERS, Bytes::from_static(bytes)).into_response()
}

const JSON_HEADERS: [(http::header::HeaderName, http::header::HeaderValue); 1] = [(
    http::header::CONTENT_TYPE,
    http::header::HeaderValue::from_static(APPLICATION_JSON),
)];

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
