use anyhow::{Context, Result};
use base64::prelude::{Engine as _, BASE64_STANDARD};
use everscale_crypto::ed25519;

pub fn parse_secret_key(key: &[u8], raw_key: bool) -> Result<ed25519::SecretKey> {
    parse_hash(key, raw_key).map(ed25519::SecretKey::from_bytes)
}

pub fn parse_public_key(key: &[u8], raw_key: bool) -> Result<ed25519::PublicKey> {
    parse_hash(key, raw_key)
        .and_then(|bytes| ed25519::PublicKey::from_bytes(bytes).context("invalid public key"))
}

fn parse_hash(key: &[u8], raw: bool) -> Result<[u8; 32]> {
    let key = if raw {
        key.try_into().ok()
    } else {
        let key = std::str::from_utf8(key)?.trim();
        match key.len() {
            44 => BASE64_STANDARD.decode(key)?.try_into().ok(),
            64 => hex::decode(key)?.try_into().ok(),
            _ => None,
        }
    };

    key.context("invalid key length")
}
