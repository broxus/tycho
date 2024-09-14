use anyhow::{Context, Result};
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::models::{BlockId, ValidatorDescription};
use tycho_block_util::queue::QueueDiffStuff;

use crate::state_node::StateNodeAdapter;

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
