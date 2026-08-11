use anyhow::{Context, Result};
use tycho_types::models::ShardIdent;
use tycho_util::FastHashSet;

pub use self::consensus_info::choose_genesis_info;
pub use self::min_ref_mc_state::{MinRefMcStateTracker, RefMcStateHandle};
pub use self::shard_state_stuff::ShardStateStuff;
pub use self::state_proof::{check_zerostate_proof, prepare_master_state_proof};

mod consensus_info;
mod min_ref_mc_state;
mod shard_state_stuff;
mod state_proof;

/// Checks that provided prefixes is a subset of `shard_ident`
/// split to `split_depth` (depth is relative to the shard).
pub fn validate_shard_prefixes(
    shard_ident: ShardIdent,
    split_depth: u8,
    prefixes: impl IntoIterator<IntoIter: ExactSizeIterator<Item = u64>>,
) -> Result<()> {
    let prefixes = prefixes.into_iter();

    anyhow::ensure!(
        !shard_ident.is_masterchain(),
        "masterchain state cannot be split into parts"
    );

    let Some(max_prefixes) = 1usize.checked_shl(split_depth as u32) else {
        anyhow::bail!("invalid split depth");
    };

    anyhow::ensure!(
        prefixes.len() <= max_prefixes,
        "too many prefixes: prefixes={}, max={max_prefixes}",
        prefixes.len()
    );

    let base_depth = shard_ident.prefix_len();

    let mut unique_prefixes = FastHashSet::default();
    for prefix in prefixes {
        let ident = ShardIdent::new(shard_ident.workchain(), prefix)
            .with_context(|| format!("invalid shard prefix: {prefix:016x}"))?;

        let prefix_len = ident.prefix_len();
        let expected_len = base_depth + split_depth as u16;
        anyhow::ensure!(
            prefix_len == expected_len,
            "invalid shard prefix: {prefix:016x} \
            (prefix_len={prefix_len}, expected_len={expected_len})"
        );
        anyhow::ensure!(
            shard_ident.is_ancestor_of(&ident),
            "unrelated shard prefix: {prefix:016x}"
        );

        anyhow::ensure!(
            unique_prefixes.insert(prefix),
            "duplicate shard prefix: {prefix:016x}"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_shard_prefixes_works() -> Result<()> {
        validate_shard_prefixes(ShardIdent::BASECHAIN, 0, Vec::<u64>::new()).unwrap();
        validate_shard_prefixes(ShardIdent::BASECHAIN, 2, vec![
            0x2000000000000000,
            0xa000000000000000,
        ])
        .unwrap();
        let non_full_shard = ShardIdent::new(0, 0x4000000000000000).unwrap();
        validate_shard_prefixes(non_full_shard, 1, vec![
            0x2000000000000000,
            0x6000000000000000,
        ])
        .unwrap();
        // `10...` is not a child of `0...`
        validate_shard_prefixes(non_full_shard, 1, vec![0xa000000000000000]).unwrap_err();

        let deep_shard = ShardIdent::new(0, 0x1000000000000000).unwrap();
        // `0001...`  cannot be split into smaller prefixes like `001...`
        validate_shard_prefixes(deep_shard, 1, vec![0x2000000000000000]).unwrap_err();

        // Too many parts.
        validate_shard_prefixes(ShardIdent::BASECHAIN, 1, vec![
            0x4000000000000000,
            0xc000000000000000,
            0x4000000000000000,
        ])
        .unwrap_err();

        // Duplicate parts
        validate_shard_prefixes(ShardIdent::BASECHAIN, 2, vec![
            0x2000000000000000,
            0x2000000000000000,
        ])
        .unwrap_err();

        // Part prefix at the wrong depth.
        validate_shard_prefixes(ShardIdent::BASECHAIN, 2, vec![0x4000000000000000]).unwrap_err();
        Ok(())
    }
}
