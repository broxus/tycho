use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tycho_types::models::StdAddr;
use tycho_util::serde_helpers;
use uuid::Uuid;

use super::RpcStateError;
use crate::state::RpcSubscriptions;

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
