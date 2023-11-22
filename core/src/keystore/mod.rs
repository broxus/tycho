use std::fs::File;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::DashMap;
use ed25519_dalek::{Keypair, PublicKey, Signer};

use crate::keystore::models::NodeKeys;
use crate::utils::generate_key_pair;

mod models;
pub mod utils;

pub trait KeyStore {
    fn regenerate_network_key(&mut self) -> Result<PublicKey>;
    fn create_validator_key(&self, timestamp: i64) -> Result<PublicKey>;
    fn get_validator_key(&self, timestamp: i64) -> Option<PublicKey>;
    fn delete_validator_key(&self, timestamp: i64);
    fn sign_by_network_key(&self, data: &[u8]) -> [u8; 64];
    fn sign_by_validator_key(&self, timestamp: i64, data: &[u8]) -> Result<[u8; 64]>;
}

#[derive(Clone)]
pub struct KeyStoreImpl {
    network_key: Arc<Keypair>,
    validator_keys: Arc<DashMap<i64, Keypair>>,
}

impl KeyStoreImpl {
    pub fn generate(timestamp: i64) -> Result<Self> {
        let mut rng = rand::thread_rng();
        let network_key = Arc::new(generate_key_pair(&mut rng)?);
        let keypair = generate_key_pair(&mut rng)?;

        let validator_keys = DashMap::new();
        validator_keys.insert(timestamp, keypair);

        Ok(Self {
            network_key,
            validator_keys: Arc::new(validator_keys),
        })
    }

    pub fn load<P>(path: P, force_regenerate: bool, timestamp: Option<i64>) -> Result<Self>
    where
        P: AsRef<Path> + std::fmt::Display + Clone,
    {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path.clone())
            .context("Failed to open keystore keys")?;

        let keys = if force_regenerate {
            Self::generate(timestamp.unwrap_or_default())
        } else {
            if let Ok(keys) = serde_json::from_reader::<&File, NodeKeys>(&file) {
                Self::try_from(keys)
            } else {
                log::warn!("failed to read keys, generating new");
                Self::generate(timestamp.unwrap_or_default())
            }
        }?;

        keys.save(file)
            .context(format!("Failed to save keys to {}", path))?;

        Ok(keys)
    }

    pub fn save<W>(&self, mut file: W) -> Result<()>
    where
        W: Write + Seek,
    {
        file.rewind()?;
        serde_json::to_writer_pretty(file, &NodeKeys::from(self))?;
        Ok(())
    }
}

impl KeyStore for KeyStoreImpl {
    fn regenerate_network_key(&mut self) -> Result<PublicKey> {
        let mut rng = rand::thread_rng();
        let key_pair = Arc::new(generate_key_pair(&mut rng)?);
        let public_key = key_pair.public;
        self.network_key = key_pair;
        Ok(public_key)
    }

    fn create_validator_key(&self, timestamp: i64) -> Result<PublicKey> {
        let mut rng = rand::thread_rng();
        let key_pair = generate_key_pair(&mut rng)?;
        let public_key = key_pair.public;
        self.validator_keys.insert(timestamp, key_pair);
        Ok(public_key)
    }

    fn get_validator_key(&self, timestamp: i64) -> Option<PublicKey> {
        self.validator_keys
            .get(&timestamp)
            .map(|r| r.value().public)
    }

    fn delete_validator_key(&self, timestamp: i64) {
        self.validator_keys.remove(&timestamp);
    }

    fn sign_by_network_key(&self, data: &[u8]) -> [u8; 64] {
        self.network_key.sign(data).to_bytes()
    }

    fn sign_by_validator_key(&self, timestamp: i64, data: &[u8]) -> Result<[u8; 64]> {
        if let Some(r) = self.validator_keys.get(&timestamp) {
            let keypair = r.value();
            Ok(keypair.sign(data).to_bytes())
        } else {
            Err(anyhow::anyhow!("No key for timestamp {} found", timestamp))
        }
    }
}
