use std::io::{IsTerminal, Read};

use anyhow::Result;
use everscale_crypto::ed25519;

use crate::util::parse_secret_key;

/// Generate a new key pair
#[derive(clap::Parser)]
pub struct CmdGenKey {
    /// secret key (reads from stdin if only flag is provided)
    #[clap(long)]
    key: Option<Option<String>>,

    /// expect a raw key input (32 bytes)
    #[clap(short, long, requires = "key")]
    raw_key: bool,
}

impl CmdGenKey {
    pub fn run(self) -> Result<()> {
        let secret = match self.key {
            Some(flag) => {
                let key = match flag {
                    Some(key) => key.into_bytes(),
                    None => {
                        let mut key = Vec::new();
                        std::io::stdin().read_to_end(&mut key)?;
                        key
                    }
                };
                parse_secret_key(&key, self.raw_key)?
            }
            None => ed25519::SecretKey::generate(&mut rand::thread_rng()),
        };

        let public = ed25519::PublicKey::from(&secret);

        let keypair = serde_json::json!({
            "public": hex::encode(public.as_bytes()),
            "secret": hex::encode(secret.as_bytes()),
        });

        let output = if std::io::stdin().is_terminal() {
            serde_json::to_string_pretty(&keypair)
        } else {
            serde_json::to_string(&keypair)
        }?;
        println!("{output}");
        Ok(())
    }
}
