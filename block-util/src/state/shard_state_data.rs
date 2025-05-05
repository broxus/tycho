use std::collections::BTreeMap;
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

    pub fn accounts(&self) -> &ShardAccounts {
        &self.inner.accounts
    }
}

#[doc(hidden)]
pub struct Inner {
    accounts: ShardAccounts,
    root: Cell,
}

pub fn split_shard(
    shard: &ShardIdent,
    accounts: &ShardAccounts,
    depth: u8,
    shards: &mut BTreeMap<u64, ShardAccounts>,
) -> Result<()> {
    fn split_shard_impl(
        shard: &ShardIdent,
        accounts: &ShardAccounts,
        depth: u8,
        shards: &mut BTreeMap<u64, ShardAccounts>,
        builder: &mut CellBuilder,
    ) -> Result<()> {
        let (left_shard_ident, right_shard_ident) = 'split: {
            if depth > 0 {
                if let Some((left, right)) = shard.split() {
                    break 'split (left, right);
                }
            }
            shards.insert(shard.prefix(), accounts.clone());
            return Ok(());
        };

        let (left_accounts, right_accounts) = {
            builder.clear_bits();
            let prefix_len = shard.prefix_len();
            if prefix_len > 0 {
                builder.store_uint(shard.prefix() >> (64 - prefix_len), prefix_len)?;
            }
            accounts.split_by_prefix(&builder.as_data_slice())?
        };

        split_shard_impl(
            &left_shard_ident,
            &left_accounts,
            depth - 1,
            shards,
            builder,
        )?;
        split_shard_impl(
            &right_shard_ident,
            &right_accounts,
            depth - 1,
            shards,
            builder,
        )
    }

    split_shard_impl(shard, accounts, depth, shards, &mut CellBuilder::new())
}
