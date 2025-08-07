use std::path::Path;

use anyhow::{Context, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tycho_crypto::ed25519;
use tycho_types::cell::HashBytes;

#[derive(Debug)]
pub struct NodeKeys {
    pub secret: HashBytes,
}

impl NodeKeys {
    pub fn generate() -> Self {
        rand::random()
    }

    /// Tries to load keys from the specified path.
    ///
    /// Generates and saves new keys if the file doesn't exist.
    pub fn load_or_create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            NodeKeys::from_file(path).context("failed to load node keys")
        } else {
            let keys = NodeKeys::generate();
            tracing::warn!(
                node_keys_path = %path.display(),
                public_key = %keys.public_key(),
                "generated new node keys",
            );

            keys.save_to_file(path)
                .context("failed to save new node keys")?;
            Ok(keys)
        }
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        tycho_util::serde_helpers::load_json_from_file(path)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        tycho_util::serde_helpers::save_json_to_file(self, path)
    }

    pub fn as_secret(&self) -> ed25519::SecretKey {
        ed25519::SecretKey::from_bytes(self.secret.0)
    }

    pub fn public_key(&self) -> ed25519::PublicKey {
        ed25519::PublicKey::from(&self.as_secret())
    }
}

impl<'de> Deserialize<'de> for NodeKeys {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        struct PartialKeys {
            secret: HashBytes,
            #[serde(default)]
            public: Option<HashBytes>,
        }

        let partial = PartialKeys::deserialize(deserializer)?;

        let secret = ed25519::SecretKey::from_bytes(partial.secret.0);
        let public = ed25519::PublicKey::from(&secret);

        if let Some(stored_public) = partial.public
            && stored_public.as_array() != public.as_bytes()
        {
            return Err(Error::custom(format!(
                "public key mismatch (stored: {stored_public}, expected: {public})",
            )));
        }

        Ok(NodeKeys {
            secret: partial.secret,
        })
    }
}

impl Serialize for NodeKeys {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct FullKeys<'a> {
            secret: &'a HashBytes,
            public: &'a HashBytes,
        }

        let secret = ed25519::SecretKey::from_bytes(self.secret.0);
        let public = ed25519::PublicKey::from(&secret);

        FullKeys {
            secret: &self.secret,
            public: HashBytes::wrap(public.as_bytes()),
        }
        .serialize(serializer)
    }
}

impl rand::distr::Distribution<NodeKeys> for rand::distr::StandardUniform {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> NodeKeys {
        NodeKeys {
            secret: rand::distr::StandardUniform.sample(rng),
        }
    }
}
