use tycho_crypto::ed25519::{KeyPair, PublicKey};
use tycho_util::FastHashMap;

#[cfg(not(any(feature = "test", test)))]
pub fn find_us_in_collators_set<T>(
    keypair: &KeyPair,
    set: &FastHashMap<[u8; 32], T>,
) -> Option<PublicKey> {
    let local_pubkey = keypair.public_key;
    if set.contains_key(local_pubkey.as_bytes()) {
        Some(local_pubkey)
    } else {
        None
    }
}

#[cfg(any(test, feature = "test"))]
pub fn find_us_in_collators_set<T>(
    keypair: &KeyPair,
    _set: &FastHashMap<[u8; 32], T>,
) -> Option<PublicKey> {
    Some(keypair.public_key)
}
