use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::models::ValidatorDescription;

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
