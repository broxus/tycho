use std::io::IsTerminal;
use std::path::Path;

use anyhow::{Context, Result};
use base64::prelude::{Engine as _, BASE64_STANDARD};
use everscale_crypto::ed25519;
use everscale_types::models::{Account, StorageUsed};
use everscale_types::num::VarUint56;
use everscale_types::prelude::*;
use serde::Serialize;

#[cfg(feature = "jemalloc")]
pub mod alloc;

pub fn create_dir_all<P: AsRef<Path>>(path: P) -> Result<()> {
    std::fs::create_dir_all(path.as_ref())
        .with_context(|| format!("failed to create a directory {}", path.as_ref().display()))
}

pub fn print_json<T: Serialize>(output: T) -> Result<()> {
    let output = if std::io::stdin().is_terminal() {
        serde_json::to_string_pretty(&output)
    } else {
        serde_json::to_string(&output)
    }?;

    println!("{output}");
    Ok(())
}

// TODO: move into types
pub fn compute_storage_used(account: &Account) -> Result<StorageUsed> {
    let cell = {
        let cx = &mut Cell::empty_context();
        let mut storage = CellBuilder::new();
        storage.store_u64(account.last_trans_lt)?;
        account.balance.store_into(&mut storage, cx)?;
        account.state.store_into(&mut storage, cx)?;
        if account.init_code_hash.is_some() {
            account.init_code_hash.store_into(&mut storage, cx)?;
        }
        storage.build_ext(cx)?
    };

    let res = cell
        .compute_unique_stats(usize::MAX)
        .context("max size exceeded")?;

    let res = StorageUsed {
        cells: VarUint56::new(res.cell_count),
        bits: VarUint56::new(res.bit_count),
        public_cells: Default::default(),
    };

    anyhow::ensure!(res.bits.is_valid(), "bit count overflow");
    anyhow::ensure!(res.cells.is_valid(), "cell count overflow");

    Ok(res)
}

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
