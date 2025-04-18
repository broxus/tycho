use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::*;
use everscale_types::prelude::*;

/// Parsed shard state accounts.
#[derive(Clone)]
#[repr(transparent)]
pub struct ShardStateData {
    inner: Arc<Inner>,
}

impl ShardStateData {
    pub fn from_root(root: Cell, mask: ShardAccountsMask) -> Result<Self> {
        let accounts = root.parse::<ShardAccounts>()?;
        Self::from_accounts_and_root(accounts, root, mask)
    }

    pub fn from_accounts(accounts: ShardAccounts, mask: ShardAccountsMask) -> Result<Self> {
        let root = CellBuilder::build_from(&accounts)?;
        Self::from_accounts_and_root(accounts, root, mask)
    }

    pub fn from_accounts_and_root(
        accounts: ShardAccounts,
        root: Cell,
        mask: ShardAccountsMask,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner {
                accounts,
                mask,
                root,
            }),
        })
    }

    pub fn mask(&self) -> ShardAccountsMask {
        self.inner.mask
    }

    pub fn root_cell(&self) -> &Cell {
        &self.inner.root
    }
}

#[doc(hidden)]
pub struct Inner {
    accounts: ShardAccounts,
    mask: ShardAccountsMask,
    root: Cell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardAccountsMask(u8);

impl ShardAccountsMask {
    const SHARD_MASK: u8 = 0xF0;
    const NUM_SHARDS: usize = 16;

    pub fn from_shard_id(id: usize) -> Option<Self> {
        if id < Self::NUM_SHARDS {
            Some(Self((id as u8) << 4))
        } else {
            None
        }
    }

    pub fn to_shard_id(self) -> usize {
        ((self.0 & Self::SHARD_MASK) >> 4) as usize
    }

    pub fn iter() -> impl Iterator<Item = Self> {
        (0..Self::NUM_SHARDS).map(|id| Self((id << 4) as u8))
    }

    pub fn from_account(account: &[u8; 32]) -> Self {
        Self(account[0] & Self::SHARD_MASK)
    }

    pub fn num_shards() -> usize {
        Self::NUM_SHARDS
    }

    pub fn mask(&self) -> u8 {
        self.0
    }
}
