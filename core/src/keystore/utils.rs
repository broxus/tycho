use anyhow::Result;
use ed25519_dalek::{Keypair, PublicKey, SecretKey, Signature, Verifier, SECRET_KEY_LENGTH};
use rand::rngs::ThreadRng;
use rand::Rng;

pub fn generate_key_pair(rng: &mut ThreadRng) -> Result<Keypair> {
    let secret = SecretKey::from_bytes(&rng.gen::<[u8; SECRET_KEY_LENGTH]>())?;
    let public = PublicKey::from(&secret);
    Ok(Keypair { secret, public })
}

pub fn verify_signatures(
    message: &[u8],
    signatures: Vec<(PublicKey, &[u8; 64])>,
) -> Vec<std::result::Result<PublicKey, PublicKey>> {
    let mut result = vec![];
    for (public, sign) in signatures {
        let r = if let Ok(sign) = Signature::from_bytes(sign) {
            match public.verify(message, &sign) {
                Ok(_) => Ok(public),
                Err(_) => Err(public),
            }
        } else {
            Err(public)
        };
        result.push(r);
    }
    result
}
