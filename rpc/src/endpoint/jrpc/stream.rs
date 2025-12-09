use std::convert::Infallible;
use std::sync::Arc;

use anyhow::anyhow;
use axum::extract::{FromRef, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tycho_types::models::StdAddr;
use tycho_util::serde_helpers;
use uuid::Uuid;

use super::RpcStateError;
use crate::state::{AccountUpdate, McTick, RegisterError, RpcState, RpcSubscriptions};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionEmptyRequest {
    #[serde(with = "serde_helpers::string")]
    pub uuid: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionUpdateRequest {
    #[serde(with = "serde_helpers::string")]
    pub uuid: Uuid,
    pub addrs: Vec<StdAddr>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubStatusResponse {
    #[serde(with = "serde_helpers::string")]
    pub uuid: Uuid,
    pub client_id: u32,
    pub subscription_count: usize,
    pub max_per_client: u8,
    pub max_clients: u32,
    pub max_addrs: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubListResponse {
    pub addresses: Vec<StdAddr>,
}

#[derive(Debug, Clone, Copy)]
pub enum SubscribeAction {
    Sub,
    Unsub,
}

pub fn handle_sub(
    subs: &RpcSubscriptions,
    req: SubscriptionUpdateRequest,
    action: SubscribeAction,
) -> Result<(), RpcStateError> {
    subs.client_id(req.uuid)
        .ok_or_else(|| RpcStateError::BadRequest(anyhow!("client not registered").into()))?;
    match action {
        SubscribeAction::Sub => subs
            .subscribe(req.uuid, req.addrs)
            .map_err(|e| RpcStateError::BadRequest(anyhow!(e.to_string()).into())),
        SubscribeAction::Unsub => subs
            .unsubscribe(req.uuid, req.addrs)
            .map_err(|e| RpcStateError::BadRequest(anyhow!(e.to_string()).into())),
    }
}

pub fn handle_unsub_all(subs: &RpcSubscriptions, uuid: Uuid) -> Result<(), RpcStateError> {
    subs.client_id(uuid)
        .ok_or_else(|| RpcStateError::BadRequest(anyhow!("client not registered").into()))?;
    subs.unsubscribe_all(uuid)
        .map_err(|e| RpcStateError::BadRequest(anyhow!(e.to_string()).into()))
}

pub fn handle_status(
    subs: &RpcSubscriptions,
    uuid: Uuid,
) -> Result<SubStatusResponse, RpcStateError> {
    let status = subs
        .status(uuid)
        .map_err(|e| RpcStateError::BadRequest(anyhow!(e.to_string()).into()))?;
    Ok(SubStatusResponse {
        uuid,
        client_id: status.client_id.0,
        subscription_count: status.subscription_count,
        max_per_client: status.max_per_client,
        max_clients: status.max_clients,
        max_addrs: status.max_addrs,
    })
}

pub fn handle_list(subs: &RpcSubscriptions, uuid: Uuid) -> Result<SubListResponse, RpcStateError> {
    subs.client_id(uuid)
        .ok_or_else(|| RpcStateError::BadRequest(anyhow!("client not registered").into()))?;
    let addresses = subs
        .list_subscriptions(uuid)
        .map_err(|e| RpcStateError::BadRequest(anyhow!(e.to_string()).into()))?;
    Ok(SubListResponse { addresses })
}

#[derive(Debug, Deserialize)]
pub struct StreamParams {
    #[serde(default)]
    pub binary: bool,
}

#[derive(Clone)]
pub struct StreamContext {
    pub subs: Arc<RpcSubscriptions>,
}

pub type SubscriptionsState = Arc<StreamContext>;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamUpdate {
    address: StdAddr,
    max_lt: u64,
    gen_utime: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    dropped: Option<u64>,
}

struct StreamState {
    rx: mpsc::Receiver<AccountUpdate>,
    uuid: Uuid,
    subs: Arc<RpcSubscriptions>,
    last_dropped: u64,
    mc_rx: watch::Receiver<McTick>,
}

impl Drop for StreamState {
    fn drop(&mut self) {
        self.subs.unregister(self.uuid);
    }
}

impl StreamState {
    fn new(
        uuid: Uuid,
        rx: mpsc::Receiver<AccountUpdate>,
        subs: Arc<RpcSubscriptions>,
        mc_rx: watch::Receiver<McTick>,
    ) -> Self {
        let last_dropped = subs.dropped(uuid).unwrap_or(0);

        Self {
            rx,
            uuid,
            subs,
            last_dropped,
            mc_rx,
        }
    }

    fn refresh_dropped(&mut self) -> u64 {
        let dropped = self.subs.dropped(self.uuid).unwrap_or(self.last_dropped);
        self.last_dropped = dropped;
        dropped
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct KeepAlivePayload {
    #[serde(with = "serde_helpers::string")]
    lt: u64,
    utime: u32,
    seqno: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    dropped: Option<u64>,
}

pub fn stream_router<S>() -> axum::Router<S>
where
    RpcState: FromRef<S>,
    S: Clone + Send + Sync + 'static,
{
    axum::Router::new().route("/stream", get(stream_route::<S>))
}

pub async fn stream_route<S>(
    State(state): State<RpcState>,
    Query(params): Query<StreamParams>,
) -> Response
where
    RpcState: FromRef<S>,
    S: Clone + Send + Sync + 'static,
{
    let subs = Arc::<RpcSubscriptions>::from_ref(&state);
    let mc_rx = state.subscribe_mc_tick();

    stream_route_inner(subs, mc_rx, params).await
}

async fn stream_route_inner(
    subs: Arc<RpcSubscriptions>,
    mc_rx: watch::Receiver<McTick>,
    params: StreamParams,
) -> Response {
    use axum::Json;

    let err = |status: StatusCode, body: serde_json::Value| (status, Json(body)).into_response();

    if params.binary {
        return err(
            StatusCode::NOT_IMPLEMENTED,
            json!({"error": "binary stream is not implemented"}),
        );
    }

    let (uuid, _, rx) = match subs.register() {
        Ok(res) => res,
        Err(RegisterError::Subscribe(tycho_rpc_subscriptions::SubscribeError::MaxClients {
            max_clients,
        })) => {
            return err(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({
                    "error": "max clients reached",
                    "maxClients": max_clients,
                }),
            );
        }
        Err(e) => {
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error": format!("{e}")}),
            );
        }
    };

    let uuid_str = uuid.to_string();
    let uuid_stream =
        stream::once(
            async move { Ok::<_, Infallible>(Event::default().event("uuid").data(uuid_str)) },
        );

    let (initial_dropped, initial_tick) = (subs.dropped(uuid).unwrap_or(0), *mc_rx.borrow());
    let initial_payload = KeepAlivePayload {
        lt: initial_tick.lt,
        utime: initial_tick.utime,
        seqno: initial_tick.seqno,
        dropped: (initial_dropped > 0).then_some(initial_dropped),
    };
    let initial_data =
        serde_json::to_string(&initial_payload).expect("serialize stream keep-alive payload");
    let initial_stream = stream::once(async move {
        Ok::<_, Infallible>(Event::default().event("keep-alive").data(initial_data))
    });

    let main_stream = stream::unfold(
        StreamState::new(uuid, rx, subs, mc_rx),
        |mut st| async move {
            tokio::select! {
                maybe_update = st.rx.recv() => {
                    let update = maybe_update?;
                    let dropped = st.refresh_dropped();
                    let payload = StreamUpdate {
                        address: update.address,
                        max_lt: update.max_lt,
                        gen_utime: update.gen_utime,
                        dropped: (dropped > 0).then_some(dropped),
                    };

                    let data = serde_json::to_string(&payload)
                        .expect("serialize stream update payload");

                    Some((Ok(Event::default().event("update").data(data)), st))
                }

                changed = st.mc_rx.changed() => {
                    if changed.is_err() {
                        return None;
                    }

                    let tick = *st.mc_rx.borrow();
                    let dropped = st.refresh_dropped();
                    let payload = KeepAlivePayload {
                        lt: tick.lt,
                        utime: tick.utime,
                        seqno: tick.seqno,
                        dropped: (dropped > 0).then_some(dropped),
                    };

                    let data = serde_json::to_string(&payload)
                        .expect("serialize stream keep-alive payload");

                    Some((Ok(Event::default().event("keep-alive").data(data)), st))
                }
            }
        },
    );

    Sse::new(uuid_stream.chain(initial_stream).chain(main_stream)).into_response()
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::str::FromStr;
    use std::time::Duration;

    use http_body_util::BodyExt;
    use serde_json::Value;
    use tokio::sync::watch;
    use tycho_rpc_subscriptions::SubscriberManagerConfig;
    use tycho_types::models::StdAddr;
    use tycho_types::prelude::HashBytes;

    use super::*;

    fn take_one_sse_chunk(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
        let pos = buf.windows(2).position(|w| w == b"\n\n")?;

        let end = pos + 2;
        Some(buf.drain(..end).collect())
    }

    fn parse_one_event(chunk: &[u8]) -> (String, String) {
        let text = std::str::from_utf8(chunk).expect("SSE chunk must be UTF-8");

        let mut event: Option<String> = None;
        let mut data = String::new();

        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("event: ") {
                event = Some(rest.to_owned());
            } else if let Some(rest) = line.strip_prefix("data: ") {
                if !data.is_empty() {
                    data.push('\n'); // just in case there are multiple data lines
                }
                data.push_str(rest);
            }
        }

        (event.expect("missing `event:` field"), data)
    }

    async fn next_sse_event<B>(body: &mut B, buf: &mut Vec<u8>) -> Option<(String, String)>
    where
        B: BodyExt + Unpin,
        B::Error: Debug,
        B::Data: AsRef<[u8]>,
    {
        loop {
            if let Some(chunk) = take_one_sse_chunk(buf) {
                return Some(parse_one_event(&chunk));
            }

            let frame = body.frame().await?;
            let frame = frame.expect("stream frame");
            let bytes = frame
                .into_data()
                .unwrap_or_else(|_| panic!("stream frame must be data"));
            buf.extend_from_slice(bytes.as_ref());
        }
    }

    async fn next_sse_event_timeout<B>(
        body: &mut B,
        buf: &mut Vec<u8>,
        dur: Duration,
    ) -> Option<(String, String)>
    where
        B: BodyExt + Unpin,
        B::Error: Debug,
        B::Data: AsRef<[u8]>,
    {
        tokio::time::timeout(dur, next_sse_event(body, buf))
            .await
            .expect("timed out waiting for SSE event")
    }

    #[tokio::test]
    async fn stream_lifecycle_with_drops_and_unsubscribe() {
        const EVENT_TIMEOUT: Duration = Duration::from_secs(1);
        const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

        let subs = Arc::new(RpcSubscriptions::new(
            SubscriberManagerConfig::new(1024, 16_384),
            1,
        ));

        let (_mc_tx, mc_rx) = watch::channel(McTick {
            seqno: 0,
            lt: 0,
            utime: 0,
        });

        let response =
            stream_route_inner(subs.clone(), mc_rx, StreamParams { binary: false }).await;

        let mut body = response.into_body();
        let mut buf = Vec::new();
        let mut events: Vec<(String, String)> = Vec::new();

        // Must receive UUID
        let (name, data) = next_sse_event_timeout(&mut body, &mut buf, EVENT_TIMEOUT)
            .await
            .expect("uuid event");
        assert_eq!(name, "uuid");
        let uuid = Uuid::from_str(&data).unwrap();
        events.push((name, data));

        // Sub and send one update
        let addr = StdAddr {
            anycast: None,
            workchain: 0,
            address: HashBytes([1; 32]),
        };
        subs.subscribe(uuid, [addr.clone()].into_iter()).unwrap();

        subs.fanout_updates([AccountUpdate {
            address: addr.clone(),
            max_lt: 1,
            gen_utime: 1,
        }])
        .await;

        // Read until we see the update event (ignore any keep-alives)
        let (name, data) = loop {
            let ev = next_sse_event_timeout(&mut body, &mut buf, EVENT_TIMEOUT)
                .await
                .expect("stream ended unexpectedly");
            if ev.0 == "update" {
                break ev;
            }
            events.push(ev);
        };
        let update_v: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(update_v["maxLt"], 1);
        events.push((name, data));

        let burst: Vec<AccountUpdate> = (1u64..=10)
            .map(|lt| AccountUpdate {
                address: addr.clone(),
                max_lt: lt,
                gen_utime: lt as _,
            })
            .collect();
        subs.fanout_updates(burst).await;

        let update_v: Value = loop {
            let (name, data) = next_sse_event_timeout(&mut body, &mut buf, EVENT_TIMEOUT)
                .await
                .expect("stream ended unexpectedly");
            events.push((name.clone(), data.clone()));

            if name == "update" {
                break serde_json::from_str::<Value>(&data).unwrap();
            }
        };

        let max_lt = update_v["maxLt"].as_u64().expect("maxLt present");
        assert!((1..=10).contains(&max_lt), "unexpected maxLt: {max_lt}");

        let dropped = update_v
            .get("dropped")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        assert_eq!(dropped, 9);

        // Unsubscribe, then ensure further updates are ignored
        subs.unsubscribe(uuid, [addr.clone()].into_iter()).unwrap();

        subs.fanout_updates([AccountUpdate {
            address: addr.clone(),
            max_lt: 42,
            gen_utime: 42,
        }])
        .await;

        // End the stream and drain remaining frames
        subs.unregister(uuid);

        tokio::time::timeout(DRAIN_TIMEOUT, async {
            while let Some((name, data)) = next_sse_event(&mut body, &mut buf).await {
                events.push((name, data));
            }
        })
        .await
        .expect("stream did not terminate in time");

        let updates: Vec<Value> = events
            .iter()
            .filter(|(n, _)| n == "update")
            .map(|(_, d)| serde_json::from_str::<Value>(d).unwrap())
            .collect();

        assert_eq!(updates.len(), 2);

        assert!(
            !updates.iter().any(|u| u["maxLt"] == 42),
            "updates after unsubscribe should be ignored"
        );
    }
}
