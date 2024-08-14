use std::sync::Arc;

use anyhow::Result;
use everscale_types::merkle::*;
use everscale_types::models::*;
use everscale_types::prelude::*;

use crate::archive::WithArchiveData;
use crate::state::ShardStateStuff;

pub type BlockProofStuffAug = WithArchiveData<BlockProofStuff>;

/// Deserialized block proof.
#[derive(Clone)]
#[repr(transparent)]
pub struct BlockProofStuff {
    inner: Arc<Inner>,
}

impl BlockProofStuff {
    pub fn from_proof(proof: Box<BlockProof>) -> Self {
        Self {
            inner: Arc::new(Inner { proof }),
        }
    }

    pub fn deserialize(block_id: &BlockId, data: &[u8]) -> Result<Self> {
        let proof = BocRepr::decode::<Box<BlockProof>, _>(data)?;

        anyhow::ensure!(
            &proof.proof_for == block_id,
            "proof block id mismatch (found: {})",
            proof.proof_for,
        );

        Ok(Self {
            inner: Arc::new(Inner { proof }),
        })
    }

    pub fn with_archive_data(self, data: Vec<u8>) -> WithArchiveData<Self> {
        WithArchiveData::new(self, data)
    }

    pub fn id(&self) -> &BlockId {
        &self.inner.proof.proof_for
    }

    pub fn proof(&self) -> &BlockProof {
        &self.inner.proof
    }

    pub fn is_link(&self) -> bool {
        self.inner.proof.proof_for.is_masterchain()
    }

    pub fn virtualize_block_root(&self) -> Result<&DynCell> {
        let merkle_proof = self.inner.proof.root.parse::<MerkleProofRef<'_>>()?;
        let block_virt_root = merkle_proof.cell.virtualize();

        anyhow::ensure!(
            &self.inner.proof.proof_for.root_hash == block_virt_root.repr_hash(),
            "merkle proof has invalid virtual hash (found: {}, expected: {})",
            block_virt_root.repr_hash(),
            self.inner.proof.proof_for
        );

        Ok(block_virt_root)
    }

    pub fn virtualize_block(&self) -> Result<(Block, HashBytes)> {
        let cell = self.virtualize_block_root()?;
        let hash = cell.repr_hash();
        Ok((cell.parse::<Block>()?, *hash))
    }

    pub fn check_with_prev_key_block_proof(
        &self,
        prev_key_block_proof: &BlockProofStuff,
    ) -> Result<()> {
        let (virt_block, virt_block_info) = self.pre_check_block_proof()?;
        check_with_prev_key_block_proof(self, prev_key_block_proof, &virt_block, &virt_block_info)?;
        Ok(())
    }

    pub fn check_with_master_state(&self, master_state: &ShardStateStuff) -> Result<()> {
        anyhow::ensure!(
            !self.is_link(),
            "cannot check proof link using master state"
        );

        let (virt_block, virt_block_info) = self.pre_check_block_proof()?;
        check_with_master_state(self, master_state, &virt_block, &virt_block_info)?;
        Ok(())
    }

    pub fn check_proof_link(&self) -> Result<()> {
        anyhow::ensure!(self.is_link(), "cannot check full proof as link");

        self.pre_check_block_proof()?;
        Ok(())
    }

    pub fn pre_check_block_proof(&self) -> Result<(Block, BlockInfo)> {
        let block_id = self.id();
        anyhow::ensure!(
            block_id.is_masterchain() || self.inner.proof.signatures.is_none(),
            "proof for non-masterchain block must not contain signatures",
        );

        let (virt_block, virt_block_hash) = self.virtualize_block()?;
        anyhow::ensure!(
            virt_block_hash == block_id.root_hash,
            "proof contains an invalid block (found: {virt_block_hash}, expected: {})",
            block_id.root_hash,
        );

        let info = virt_block.load_info()?;
        let _value_flow = virt_block.load_value_flow()?;
        let _state_update = virt_block.load_state_update()?;

        anyhow::ensure!(
            info.version == 0,
            "proof has an unsupported block structure version: {}",
            info.version,
        );

        anyhow::ensure!(
            info.seqno == block_id.seqno,
            "proof has an incorrect block seqno (found: {}, expected: {})",
            info.seqno,
            block_id.seqno,
        );

        anyhow::ensure!(
            info.shard == block_id.shard,
            "proof has an incorrect block shard (found: {}, expected: {})",
            info.shard,
            block_id.shard,
        );

        anyhow::ensure!(
            info.load_master_ref()?.is_none() == info.shard.is_masterchain(),
            "proof has an invalid `not_master` flag in block info",
        );

        anyhow::ensure!(
            !block_id.is_masterchain()
                || !info.after_merge && !info.before_split && !info.after_split,
            "proof has incorrect split/merge flags in block info",
        );

        anyhow::ensure!(
            !info.after_merge || !info.after_split,
            "proof has both split and merge flags in block info",
        );

        anyhow::ensure!(
            !info.after_split || !info.shard.is_full(),
            "proof has `after_split` flag set for a full shard in block info",
        );

        anyhow::ensure!(
            !info.after_merge || info.shard.can_split(),
            "proof has `after_merge` flag set for the tiniest shard in block info",
        );

        anyhow::ensure!(
            !info.key_block || block_id.is_masterchain(),
            "proof has `key_block` flag set for a non-masterchain block in block info",
        );

        Ok((virt_block, info))
    }

    fn check_signatures(&self, subset: &ValidatorSubsetInfo) -> Result<()> {
        // Prepare
        let Some(signatures) = self.inner.proof.signatures.as_ref() else {
            anyhow::bail!("proof doesn't have signatures to check");
        };

        anyhow::ensure!(
            signatures.validator_info.validator_list_hash_short == subset.short_hash,
            "proof contains an invalid validator set hash (found: {}, expected: {})",
            signatures.validator_info.validator_list_hash_short,
            subset.short_hash,
        );

        let expected_count = signatures.signature_count as usize;

        {
            let mut count = 0usize;
            for value in signatures.signatures.raw_values() {
                value?;
                count += 1;
                if count > expected_count {
                    break;
                }
            }

            anyhow::ensure!(
                expected_count == count,
                "proof contains an invalid signature count (found: {count}, expected: {expected_count})",
            );
        }

        // Check signatures
        let checked_data = Block::build_data_for_sign(self.id());

        let mut total_weight = 0u64;
        for v in subset.validators.iter() {
            match total_weight.checked_add(v.weight) {
                Some(new_total_weight) => total_weight = new_total_weight,
                None => anyhow::bail!("overflow while computing total weight of validators subset"),
            }
        }

        let weight = match signatures
            .signatures
            .check_signatures(&subset.validators, &checked_data)
        {
            Ok(weight) => weight,
            Err(e) => anyhow::bail!("proof contains invalid signatures: {e:?}"),
        };

        // Check weight
        anyhow::ensure!(
            weight == signatures.total_weight,
            "total signature weight mismatch (found: {weight}, expected: {})",
            signatures.total_weight
        );

        match (weight.checked_mul(3), total_weight.checked_mul(2)) {
            (Some(weight_x3), Some(total_weight_x2)) => {
                anyhow::ensure!(
                    weight_x3 > total_weight_x2,
                    "proof contains too small signatures weight"
                );
            }
            _ => anyhow::bail!("overflow while checking signature weight"),
        }

        Ok(())
    }

    fn process_given_state(
        &self,
        master_state: &ShardStateStuff,
        block_info: &BlockInfo,
    ) -> Result<ValidatorSubsetInfo> {
        anyhow::ensure!(
            master_state.block_id().is_masterchain(),
            "state for {} doesn't belong to the masterchain",
            master_state.block_id(),
        );

        anyhow::ensure!(
            self.id().is_masterchain(),
            "cannot check proof for a non-masterchain block using master state",
        );

        anyhow::ensure!(
            block_info.prev_key_block_seqno <= master_state.block_id().seqno,
            "cannot check proof using the master state {}, because it is older than the previous key block with seqno {}",
            master_state.block_id(),
            block_info.prev_key_block_seqno,
        );

        anyhow::ensure!(
            master_state.block_id().seqno < self.id().seqno,
            "cannot check proof using a newer master state {}",
            master_state.block_id(),
        );

        let (validator_set, catchain_config) = {
            let Some(custom) = master_state.state().load_custom()? else {
                anyhow::bail!("no additional masterchain data found in the master state");
            };
            let validator_set = custom.config.get_current_validator_set()?;
            let catchain_config = custom.config.get_catchain_config()?;
            (validator_set, catchain_config)
        };

        self.calc_validators_subset_standard(&validator_set, &catchain_config)
    }

    fn process_prev_key_block_proof(
        &self,
        prev_key_block_proof: &BlockProofStuff,
    ) -> Result<ValidatorSubsetInfo> {
        let (virt_key_block, prev_key_block_info) = prev_key_block_proof.pre_check_block_proof()?;

        anyhow::ensure!(
            prev_key_block_info.key_block,
            "expected a proof for a key block",
        );

        let (validator_set, catchain_config) = {
            let extra = virt_key_block.load_extra()?;
            let Some(custom) = extra.load_custom()? else {
                anyhow::bail!("no additional masterchain data found in the key block");
            };
            let Some(config) = custom.config.as_ref() else {
                anyhow::bail!("no config found in the key block");
            };

            let validator_set = config.get_current_validator_set()?;
            let catchain_config = config.get_catchain_config()?;
            (validator_set, catchain_config)
        };

        self.calc_validators_subset_standard(&validator_set, &catchain_config)
    }

    fn calc_validators_subset_standard(
        &self,
        validator_set: &ValidatorSet,
        catchain_config: &CatchainConfig,
    ) -> Result<ValidatorSubsetInfo> {
        let cc_seqno = self
            .inner
            .proof
            .signatures
            .as_ref()
            .map(|s| s.validator_info.catchain_seqno)
            .unwrap_or_default();

        ValidatorSubsetInfo::compute_standard(validator_set, self.id(), catchain_config, cc_seqno)
    }
}

impl AsRef<BlockProof> for BlockProofStuff {
    #[inline]
    fn as_ref(&self) -> &BlockProof {
        &self.inner.proof
    }
}

unsafe impl arc_swap::RefCnt for BlockProofStuff {
    type Base = Inner;

    fn into_ptr(me: Self) -> *mut Self::Base {
        arc_swap::RefCnt::into_ptr(me.inner)
    }

    fn as_ptr(me: &Self) -> *mut Self::Base {
        arc_swap::RefCnt::as_ptr(&me.inner)
    }

    unsafe fn from_ptr(ptr: *const Self::Base) -> Self {
        Self {
            inner: arc_swap::RefCnt::from_ptr(ptr),
        }
    }
}

#[doc(hidden)]
pub struct Inner {
    proof: Box<BlockProof>,
}

pub fn check_with_prev_key_block_proof(
    proof: &BlockProofStuff,
    prev_key_block_proof: &BlockProofStuff,
    virt_block: &Block,
    virt_block_info: &BlockInfo,
) -> Result<()> {
    let proof_id = proof.id();
    let key_block_id = prev_key_block_proof.id();

    anyhow::ensure!(
        proof_id.is_masterchain(),
        "cannot check a non-masterchain block using the previous key block",
    );

    anyhow::ensure!(
        key_block_id.is_masterchain(),
        "given previous key block is not a masterchain block (id: {key_block_id})",
    );

    let prev_key_block_seqno = virt_block_info.prev_key_block_seqno;
    anyhow::ensure!(
        key_block_id.seqno == prev_key_block_seqno,
        "given previous key block is not the one expected \
        (found: {key_block_id}, expected seqno: {prev_key_block_seqno})",
    );

    anyhow::ensure!(
        key_block_id.seqno < proof_id.seqno,
        "given previous key block has a greater seqno than the proof block ({} >= {})",
        key_block_id.seqno,
        proof_id.seqno,
    );

    let subset = proof.process_prev_key_block_proof(prev_key_block_proof)?;

    if virt_block_info.key_block {
        pre_check_key_block_proof(virt_block)?;
    }

    proof.check_signatures(&subset)
}

pub fn check_with_master_state(
    proof: &BlockProofStuff,
    master_state: &ShardStateStuff,
    virt_block: &Block,
    virt_block_info: &BlockInfo,
) -> Result<()> {
    if virt_block_info.key_block {
        pre_check_key_block_proof(virt_block)?;
    }

    let subset = proof.process_given_state(master_state, virt_block_info)?;
    proof.check_signatures(&subset)
}

fn pre_check_key_block_proof(virt_block: &Block) -> Result<()> {
    let extra = virt_block.load_extra()?;
    let Some(mc_extra) = extra.load_custom()? else {
        anyhow::bail!("proof contains a virtual block without masterchain block extra")
    };

    let Some(config) = mc_extra.config.as_ref() else {
        anyhow::bail!("proof contains a virtual block without config params")
    };

    if config.get::<ConfigParam34>()?.is_none() {
        anyhow::bail!("proof contains a virtual block without current validators config param");
    }

    for param in 32..=38 {
        if let Some(mut vset) = config.get_raw(param)? {
            ValidatorSet::load_from(&mut vset)?;
        }
    }

    // TODO: remove this once the new consensus is implemented
    let _catchain_config = config.get::<ConfigParam28>()?;

    Ok(())
}

#[derive(Clone, Debug)]
pub struct ValidatorSubsetInfo {
    pub validators: Vec<ValidatorDescription>,
    pub short_hash: u32,
}

impl ValidatorSubsetInfo {
    pub fn compute_standard(
        validator_set: &ValidatorSet,
        block_id: &BlockId,
        catchain_config: &CatchainConfig,
        cc_seqno: u32,
    ) -> Result<Self> {
        let Some((validators, short_hash)) =
            validator_set.compute_subset(block_id.shard, catchain_config, cc_seqno)
        else {
            anyhow::bail!("failed to compute a validator subset");
        };

        Ok(ValidatorSubsetInfo {
            validators,
            short_hash,
        })
    }
}
