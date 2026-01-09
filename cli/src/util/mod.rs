use std::io::IsTerminal;
use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use tycho_crypto::ed25519;
use tycho_types::abi::{AbiType, AbiValue, FromAbi, IntoAbi, WithAbiType};
use tycho_types::models::{Account, StorageUsed};
use tycho_types::num::{Tokens, VarUint56};
use tycho_types::prelude::*;

#[cfg(feature = "jemalloc")]
pub mod alloc;
pub mod config;
pub mod elector;
pub mod getter;
pub mod jrpc_client;
pub mod rpc_wu_updater;
pub mod wallet;

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
        let cx = Cell::empty_context();
        let mut storage = CellBuilder::new();
        storage.store_u64(account.last_trans_lt)?;
        account.balance.store_into(&mut storage, cx)?;
        account.state.store_into(&mut storage, cx)?;
        storage.build_ext(cx)?
    };

    let res = cell
        .compute_unique_stats::<BuildCellHasher>(usize::MAX)
        .context("max size exceeded")?;

    let res = StorageUsed {
        cells: VarUint56::new(res.cell_count),
        bits: VarUint56::new(res.bit_count),
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

#[derive(Default, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct FpTokens(pub u128);

impl FromStr for FpTokens {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        const DECIMALS: usize = 9;
        const ONE: u128 = 10u128.pow(DECIMALS as _);

        let (int, frac) = match s.split_once('.') {
            None => (s.parse::<u128>()?, 0),
            Some((int, frac)) => {
                let int = int.parse::<u128>()?;
                if frac.is_empty() || frac.len() > DECIMALS {
                    anyhow::bail!("invalid fraction part");
                }

                let leading_zeros = frac.len() - frac.trim_start_matches('0').len();
                let frac = if leading_zeros == frac.len() {
                    0
                } else {
                    let trailing_zeros = DECIMALS - frac.len();
                    frac[leading_zeros..].parse::<u128>()? * 10u128.pow(trailing_zeros as _)
                };

                (int, frac)
            }
        };
        debug_assert!(frac < ONE);

        let Some(int) = int.checked_mul(ONE) else {
            anyhow::bail!("too big integer part");
        };

        Ok(Self(int + frac))
    }
}

impl Serialize for FpTokens {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for FpTokens {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        String::deserialize(deserializer)?
            .parse::<Self>()
            .map_err(Error::custom)
    }
}

impl std::fmt::Debug for FpTokens {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for FpTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num: u128 = self.0;
        let int = num / 1000000000;
        let mut frac = num % 1000000000;

        int.fmt(f)?;
        if frac > 0 {
            let mut len = 9usize;
            while frac.is_multiple_of(10) && frac > 0 {
                len -= 1;
                frac /= 10;
            }
            f.write_fmt(format_args!(".{frac:0len$}"))?;
        }
        Ok(())
    }
}

impl std::ops::Deref for FpTokens {
    type Target = u128;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for FpTokens {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl WithAbiType for FpTokens {
    fn abi_type() -> AbiType {
        Tokens::abi_type()
    }
}

impl IntoAbi for FpTokens {
    fn as_abi(&self) -> AbiValue {
        Tokens::from(self).into_abi()
    }

    fn into_abi(self) -> AbiValue
    where
        Self: Sized,
    {
        Tokens::from(self).into_abi()
    }
}

impl FromAbi for FpTokens {
    fn from_abi(value: AbiValue) -> Result<Self> {
        Tokens::from_abi(value).map(Self::from)
    }
}

impl From<u128> for FpTokens {
    fn from(value: u128) -> Self {
        FpTokens(value)
    }
}

impl From<Tokens> for FpTokens {
    fn from(value: Tokens) -> Self {
        FpTokens(value.into_inner())
    }
}

impl From<&Tokens> for FpTokens {
    fn from(value: &Tokens) -> Self {
        FpTokens(value.into_inner())
    }
}

impl From<FpTokens> for u128 {
    #[inline]
    fn from(value: FpTokens) -> Self {
        value.0
    }
}

impl From<FpTokens> for Tokens {
    #[inline]
    fn from(value: FpTokens) -> Self {
        Tokens::new(value.0)
    }
}

impl From<&FpTokens> for Tokens {
    #[inline]
    fn from(value: &FpTokens) -> Self {
        Tokens::new(value.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fp_tokens_from_str() {
        assert_eq!("0".parse::<FpTokens>().unwrap(), FpTokens::default());
        assert_eq!("0.0".parse::<FpTokens>().unwrap(), FpTokens::default());
        assert_eq!("0.0000".parse::<FpTokens>().unwrap(), FpTokens::default());

        assert_eq!("0.1".parse::<FpTokens>().unwrap(), FpTokens(100_000_000));
        assert_eq!("0.01".parse::<FpTokens>().unwrap(), FpTokens(10_000_000));
        assert_eq!("0.001".parse::<FpTokens>().unwrap(), FpTokens(1_000_000));
        assert_eq!("0.0001".parse::<FpTokens>().unwrap(), FpTokens(100_000));
        assert_eq!("0.00001".parse::<FpTokens>().unwrap(), FpTokens(10_000));
        assert_eq!("0.000001".parse::<FpTokens>().unwrap(), FpTokens(1_000));
        assert_eq!("0.0000001".parse::<FpTokens>().unwrap(), FpTokens(100));
        assert_eq!("0.00000001".parse::<FpTokens>().unwrap(), FpTokens(10));
        assert_eq!("0.000000001".parse::<FpTokens>().unwrap(), FpTokens(1));

        assert_eq!(
            "123123".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_000_000)
        );
        assert_eq!(
            "123123.0".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_000_000)
        );
        assert_eq!(
            "123123.0000".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_000_000)
        );

        assert_eq!(
            "123123.1".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_100_000_000)
        );
        assert_eq!(
            "123123.01".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_010_000_000)
        );
        assert_eq!(
            "123123.001".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_001_000_000)
        );
        assert_eq!(
            "123123.0001".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_100_000)
        );
        assert_eq!(
            "123123.00001".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_010_000)
        );
        assert_eq!(
            "123123.000001".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_001_000)
        );
        assert_eq!(
            "123123.0000001".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_000_100)
        );
        assert_eq!(
            "123123.00000001".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_000_010)
        );
        assert_eq!(
            "123123.000000001".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_000_001)
        );

        assert_eq!(
            "123123.000456001".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_000_456_001)
        );
        assert_eq!(
            "123123.789456001".parse::<FpTokens>().unwrap(),
            FpTokens(123_123_789_456_001)
        );

        assert!("".parse::<FpTokens>().is_err());
        assert!("0.000000000000".parse::<FpTokens>().is_err());
        assert!("test".parse::<FpTokens>().is_err());
        assert!("123.deafbeaf".parse::<FpTokens>().is_err());

        assert_eq!(FpTokens(0).to_string(), "0");
        assert_eq!(FpTokens(1).to_string(), "0.000000001");
        assert_eq!(FpTokens(10).to_string(), "0.00000001");
        assert_eq!(FpTokens(100).to_string(), "0.0000001");
        assert_eq!(FpTokens(1000).to_string(), "0.000001");
        assert_eq!(FpTokens(10000).to_string(), "0.00001");
        assert_eq!(FpTokens(100000).to_string(), "0.0001");
        assert_eq!(FpTokens(1000000).to_string(), "0.001");
        assert_eq!(FpTokens(10000000).to_string(), "0.01");
        assert_eq!(FpTokens(100000000).to_string(), "0.1");
        assert_eq!(FpTokens(1000000000).to_string(), "1");
    }
}
