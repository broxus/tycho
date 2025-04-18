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
    pub fn from_root(root: Cell) -> Result<Self> {
        let accounts = root.parse::<ShardAccounts>()?;
        Self::from_accounts_and_root(accounts, root)
    }

    pub fn from_accounts(accounts: ShardAccounts) -> Result<Self> {
        let root = CellBuilder::build_from(&accounts)?;
        Self::from_accounts_and_root(accounts, root)
    }

    pub fn from_accounts_and_root(accounts: ShardAccounts, root: Cell) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner { accounts, root }),
        })
    }

    pub fn root_cell(&self) -> &Cell {
        &self.inner.root
    }
}

#[doc(hidden)]
pub struct Inner {
    accounts: ShardAccounts,
    root: Cell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardStateDataId(pub u8);

impl ShardStateDataId {
    pub const NUM_SHARDS: u8 = 16;
    const SHARD_MASK: u8 = 0xF0;

    pub fn new(id: u8) -> Option<Self> {
        if id < Self::NUM_SHARDS {
            Some(Self(id))
        } else {
            None
        }
    }

    pub fn from_account(account: &[u8; 32]) -> Self {
        Self(account[0] & Self::SHARD_MASK)
    }

    pub fn iter() -> impl Iterator<Item = Self> {
        (0..Self::NUM_SHARDS).map(|id| Self(id << 4))
    }
}
