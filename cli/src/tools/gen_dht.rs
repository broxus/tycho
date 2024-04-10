use std::io::{IsTerminal, Read};

use anyhow::Result;
use everscale_crypto::ed25519;
use tycho_network::{Address, PeerId, PeerInfo};
use tycho_util::time::now_sec;

use crate::util::parse_secret_key;

/// Generate a DHT entry for a node.
#[derive(clap::Parser)]
pub struct CmdGenDht {
    /// a list of node addresses
    #[clap(required = true)]
    addr: Vec<Address>,

    /// node secret key (reads from stdin if not provided)
    #[clap(long)]
    key: Option<String>,

    /// expect a raw key input (32 bytes)
    #[clap(short, long)]
    raw_key: bool,

    /// time to live in seconds (default: unlimited)
    #[clap(long)]
    ttl: Option<u32>,
}

impl CmdGenDht {
    pub fn run(self) -> Result<()> {
        // Read key
        let key = match self.key {
            Some(key) => key.into_bytes(),
            None => {
                let mut key = Vec::new();
                std::io::stdin().read_to_end(&mut key)?;
                key
            }
        };
        let key = parse_secret_key(&key, self.raw_key)?;
        let entry = make_peer_info(&key, self.addr, self.ttl);

        let output = if std::io::stdin().is_terminal() {
            serde_json::to_string_pretty(&entry)
        } else {
            serde_json::to_string(&entry)
        }?;
        println!("{output}");
        Ok(())
    }
}

fn make_peer_info(key: &ed25519::SecretKey, addresses: Vec<Address>, ttl: Option<u32>) -> PeerInfo {
    let keypair = ed25519::KeyPair::from(key);
    let peer_id = PeerId::from(keypair.public_key);

    let now = now_sec();
    let mut node_info = PeerInfo {
        id: peer_id,
        address_list: addresses.into_boxed_slice(),
        created_at: now,
        expires_at: ttl.unwrap_or(u32::MAX),
        signature: Box::new([0; 64]),
    };
    *node_info.signature = keypair.sign(&node_info);
    node_info
}
