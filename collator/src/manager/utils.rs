use anyhow::{Context, Result};
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::models::{BlockId, ValidatorDescription};
use tycho_block_util::queue::QueueDiffStuff;
use tycho_util::FastHashMap;

use crate::state_node::StateNodeAdapter;

pub fn find_us_in_collators_set(
    keypair: &KeyPair,
    set: &FastHashMap<[u8; 32], ValidatorDescription>,
) -> Option<PublicKey> {
    let local_pubkey = keypair.public_key;
    if set.contains_key(local_pubkey.as_bytes()) {
        Some(local_pubkey)
    } else {
        None
    }
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
