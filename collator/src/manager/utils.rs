use std::sync::Arc;

use anyhow::Result;
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::models::{BlockId, Lazy, OutMsgDescr, ValidatorDescription};
use tycho_block_util::queue::QueueDiffStuff;

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
    state_node_adapter: Arc<dyn StateNodeAdapter>,
    block_id: &BlockId,
) -> Result<(Vec<BlockId>, Option<(QueueDiffStuff, Lazy<OutMsgDescr>)>)> {
    let mut prev_block_ids = vec![];

    let Some(block_stuff) = state_node_adapter.load_block(block_id).await? else {
        return Ok((prev_block_ids, None));
    };

    let lazy_out_msgs = block_stuff.block().load_extra()?.out_msg_description;
    let queue_diff_stuff = state_node_adapter.load_diff(block_id).await?.unwrap();

    let prev_ids_info = block_stuff.construct_prev_id()?;
    prev_block_ids.push(prev_ids_info.0);
    if let Some(id) = prev_ids_info.1 {
        prev_block_ids.push(id);
    }

    tracing::debug!(target: tracing_targets::COLLATION_MANAGER,
        prev_block_ids = %DisplaySlice(&prev_block_ids),
        "loaded block and queue diff stuff",
    );

    Ok((prev_block_ids, Some((queue_diff_stuff, lazy_out_msgs))))
}
