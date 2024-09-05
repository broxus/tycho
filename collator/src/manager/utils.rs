use std::sync::OnceLock;

use anyhow::{Context, Result};
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::models::{BlockId, Lazy, OutMsgDescr, ValidatorDescription};
use tycho_block_util::queue::QueueDiffStuff;

use super::types::LoadedQueueDiffContext;
use crate::state_node::StateNodeAdapter;
use crate::tracing_targets;
use crate::types::DisplaySlice;

pub fn find_us_in_collators_set(
    keypair: &KeyPair,
    collators_set: &[ValidatorDescription],
) -> Option<PublicKey> {
    let local_pubkey = keypair.public_key;
    let local_pubkey_hash = local_pubkey.as_bytes();
    for node in collators_set {
        if local_pubkey_hash == &node.public_key {
            return Some(local_pubkey);
        }
    }
    None
}

pub async fn load_block_queue_diff_stuff(
    state_node_adapter: &dyn StateNodeAdapter,
    block_id: &BlockId,
) -> Result<LoadedQueueDiffContext> {
    static EMPTY_OUT_MSGS: OnceLock<Lazy<OutMsgDescr>> = OnceLock::new();

    if block_id.seqno == 0 {
        return Ok(LoadedQueueDiffContext {
            prev_ids: Vec::new(),
            queue_diff: QueueDiffStuff::new_empty(block_id),
            out_msgs: EMPTY_OUT_MSGS
                .get_or_init(|| Lazy::new(&OutMsgDescr::new()).unwrap())
                .clone(),
        });
    }

    let Some(block_stuff) = state_node_adapter.load_block(block_id).await? else {
        anyhow::bail!("block not found: {block_id}");
    };

    let out_msgs = block_stuff.block().load_extra()?.out_msg_description;
    let queue_diff = state_node_adapter.load_diff(block_id).await?.unwrap();

    let (prev1, prev2) = block_stuff.construct_prev_id()?;

    let mut prev_ids = Vec::new();
    prev_ids.push(prev1);
    prev_ids.extend(prev2);

    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
        prev_block_ids = %DisplaySlice(&prev_ids),
        "loaded block and queue diff stuff",
    );

    Ok(LoadedQueueDiffContext {
        prev_ids,
        queue_diff,
        out_msgs,
    })
}

pub async fn load_only_queue_diff_stuff(
    state_node_adapter: &dyn StateNodeAdapter,
    block_id: &BlockId,
) -> Result<QueueDiffStuff> {
    if block_id.seqno == 0 {
        return Ok(QueueDiffStuff::new_empty(block_id));
    }

    state_node_adapter
        .load_diff(block_id)
        .await?
        .with_context(|| format!("block not found: {block_id}"))
}
