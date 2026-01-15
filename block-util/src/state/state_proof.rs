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
        !config_cell.descriptor().level_mask().is_empty(),
        "zerostate proof has pruned branches"
    );

    let Some(shard_hashes) = extra.shards.as_dict().root() else {
        anyhow::bail!("zerostate shard_hashes are empty");
    };

    anyhow::ensure!(
        !shard_hashes.descriptor().level_mask().is_empty(),
        "zerostate shard_hashes has pruned branches"
    );

    Ok(())
}
