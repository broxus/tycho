use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use ed25519_dalek::{Keypair, PublicKey, SecretKey};

use crate::KeyStoreImpl;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct NodeKeys {
    pub network_key: String,
    pub validator_keys: HashMap<i64, String>,
}

impl TryFrom<NodeKeys> for KeyStoreImpl {
    type Error = anyhow::Error;

    fn try_from(value: NodeKeys) -> Result<Self> {
        let secret = SecretKey::from_bytes(&hex::decode(value.network_key)?)?;
        let public = PublicKey::from(&secret);
        let network_key = Arc::new(Keypair { secret, public });

        let validator_keys_dash: DashMap<i64, Keypair> = DashMap::new();

        for (timestamp, key) in value.validator_keys {
            let secret = SecretKey::from_bytes(&hex::decode(key)?)?;
            let public = PublicKey::from(&secret);
            let key = Keypair { secret, public };
            validator_keys_dash.insert(timestamp, key);
        }
        Ok(Self {
            network_key,
            validator_keys: Arc::new(validator_keys_dash),
        })
    }
}

impl From<&KeyStoreImpl> for NodeKeys {
    fn from(value: &KeyStoreImpl) -> Self {
        let network_key = hex::encode(&value.network_key.secret);

        let mut validator_keys = HashMap::new();

        for r in value.validator_keys.iter() {
            validator_keys.insert(*r.key(), hex::encode(&r.value().secret));
        }
        Self {
            network_key,
            validator_keys,
        }
    }
}
