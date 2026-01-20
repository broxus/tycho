use anyhow::Context;
use tycho_types::cell::{Cell, UsageTree, UsageTreeMode};
use tycho_types::merkle::MerkleProof;
use tycho_types::models::ShardStateUnsplit;
use tycho_types::prelude::HashBytes;

pub fn prepare_master_state_proof(root: &Cell) -> anyhow::Result<Cell> {
    let usage_tree = UsageTree::new(UsageTreeMode::OnLoad);
    let tracked_cell = usage_tree.track(root);

    let state = tracked_cell.parse::<ShardStateUnsplit>()?;
    let extra = state.custom.context("McStateExtra not found in state")?;
    // Visit `McStateExtra` root data.
    let extra = extra.load()?;
    // NOTE: When we add cells to `extra.consensus_info` we must also visit them here.

    // Visit all cells in config.
    if let Some(root) = extra.config.params.as_dict().root() {
        root.touch_recursive();
    }

    // Visit shard hashes.
    if let Some(root) = extra.shards.as_dict().root() {
        root.touch_recursive();
    }

    // Build merkle proof.
    let merkle_proof = MerkleProof::create(root.as_ref(), usage_tree).build()?;
    Ok(merkle_proof.cell)
}

pub fn check_zerostate_proof(
    zerostate_root_hash: HashBytes,
    zerostate_proof: &Cell,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        *zerostate_proof.hash(0) == zerostate_root_hash,
        "proof has an incorrect root hash(found: {}, expected: {})",
        *zerostate_proof.hash(0),
        zerostate_root_hash
    );

    let zerostate = zerostate_proof
        .virtualize()
        .parse::<ShardStateUnsplit>()
        .context("failed to parse zerostate cell")?;
    let custom_opt = zerostate.load_custom()?;
    let Some(extra) = custom_opt else {
        anyhow::bail!("zerostate does not contain mc state extra");
    };

    let Some(config_cell) = extra.config.as_dict().root() else {
        anyhow::bail!("zerostate config is empty");
    };

    anyhow::ensure!(
        config_cell.descriptor().level_mask().is_empty(),
        "zerostate proof has pruned branches"
    );

    let Some(shard_hashes) = extra.shards.as_dict().root() else {
        anyhow::bail!("zerostate shard_hashes are empty");
    };

    anyhow::ensure!(
        shard_hashes.descriptor().level_mask().is_empty(),
        "zerostate shard_hashes has pruned branches"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use tycho_types::cell::{Cell, CellBuilder, CellFamily, UsageTree, UsageTreeMode};
    use tycho_types::dict::{AugDict, Dict};
    use tycho_types::merkle::MerkleProof;
    use tycho_types::models::{
        BlockchainConfig, ConsensusInfo, CurrencyCollection, McStateExtra, ShardHashes,
        ShardStateUnsplit, ValidatorInfo,
    };
    use tycho_types::prelude::HashBytes;

    use super::*;

    fn make_config() -> anyhow::Result<BlockchainConfig> {
        let mut config = BlockchainConfig::new_empty(HashBytes::ZERO);
        config.set_raw(1, Cell::empty_cell())?;
        config.set_raw(2, Cell::empty_cell())?;
        Ok(config)
    }

    fn make_shards() -> anyhow::Result<ShardHashes> {
        let mut shards_dict = Dict::<i32, Cell>::new();
        shards_dict.set(0, Cell::empty_cell())?;
        shards_dict.set(1, Cell::empty_cell())?;
        Ok(ShardHashes::from_dict(shards_dict))
    }

    fn make_extra(shards: ShardHashes, config: BlockchainConfig) -> McStateExtra {
        McStateExtra {
            shards,
            config,
            validator_info: ValidatorInfo {
                validator_list_hash_short: 0,
                catchain_seqno: 0,
                nx_cc_updated: false,
            },
            consensus_info: ConsensusInfo::ZEROSTATE,
            prev_blocks: AugDict::new(),
            after_key_block: false,
            last_key_block: None,
            block_create_stats: None,
            global_balance: CurrencyCollection::ZERO,
        }
    }

    fn make_state_cell_with(extra: Option<&McStateExtra>) -> anyhow::Result<Cell> {
        let mut state = ShardStateUnsplit::default();
        state.set_custom(extra)?;
        Ok(CellBuilder::build_from(&state)?)
    }

    fn make_state_cell() -> anyhow::Result<Cell> {
        let shards = make_shards()?;
        let config = make_config()?;
        let extra = make_extra(shards, config);

        make_state_cell_with(Some(&extra))
    }

    fn make_pruned_state_proof(root: &Cell) -> anyhow::Result<Cell> {
        let usage_tree = UsageTree::new(UsageTreeMode::OnLoad);
        let tracked_cell = usage_tree.track(root);

        let state = tracked_cell.parse::<ShardStateUnsplit>()?;
        let extra = state.custom.context("McStateExtra not found in state")?;
        let _ = extra.load()?;

        let merkle_proof = MerkleProof::create(root.as_ref(), usage_tree).build()?;
        Ok(merkle_proof.cell)
    }

    #[test]
    fn check_zerostate_proof_test() -> anyhow::Result<()> {
        let state_root = make_state_cell()?;
        let expected_root_hash = *state_root.repr_hash();

        let full_proof = prepare_master_state_proof(&state_root)?;
        check_zerostate_proof(expected_root_hash, &full_proof)?;

        let pruned_proof = make_pruned_state_proof(&state_root)?;
        let err = check_zerostate_proof(expected_root_hash, &pruned_proof).unwrap_err();
        assert!(
            err.to_string().contains("pruned branches"),
            "unexpected error: {err:#}",
        );

        Ok(())
    }

    #[test]
    fn check_invalid_zerostate_proof_test() -> anyhow::Result<()> {
        let state_root = make_state_cell()?;
        let full_proof = prepare_master_state_proof(&state_root)?;

        let expected_hash = *state_root.repr_hash();
        let wrong_hash = if expected_hash == HashBytes::ZERO {
            HashBytes([1; 32])
        } else {
            HashBytes::ZERO
        };

        let err = check_zerostate_proof(wrong_hash, &full_proof).unwrap_err();
        assert!(
            err.to_string().contains("incorrect root hash"),
            "unexpected error: {err:#}",
        );

        let state_root = make_state_cell_with(None)?;
        let expected_root_hash = *state_root.repr_hash();

        let err = check_zerostate_proof(expected_root_hash, &state_root).unwrap_err();
        assert!(
            err.to_string().contains("mc state extra"),
            "unexpected error: {err:#}",
        );

        let shards = ShardHashes::default();
        let config = make_config()?;
        let extra = make_extra(shards, config);
        let state_root = make_state_cell_with(Some(&extra))?;
        let expected_root_hash = *state_root.repr_hash();
        let full_proof = prepare_master_state_proof(&state_root)?;

        let err = check_zerostate_proof(expected_root_hash, &full_proof).unwrap_err();
        assert!(
            err.to_string().contains("shard_hashes are empty"),
            "unexpected error: {err:#}",
        );

        Ok(())
    }
}
