use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::*;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
use tycho_block_util::block::{BlockStuffAug, ValidatorSubsetInfo};
use tycho_block_util::state::ShardStateStuff;
use tycho_network::PeerId;
use tycho_util::{serde_helpers, FastHashMap};

use crate::internal_queue::types::InternalMessageKey;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct CollationConfig {
    pub supported_block_version: u32,
    pub supported_capabilities: GlobalCapabilities,

    #[serde(with = "serde_helpers::humantime")]
    pub mc_block_min_interval: Duration,
    pub max_mc_block_delta_from_bc_to_await_own: i32,
    pub max_uncommitted_chain_length: u32,
    pub gas_used_to_import_next_anchor: u64,

    pub msgs_exec_params: MsgsExecutionParams,
}

impl Default for CollationConfig {
    fn default() -> Self {
        Self {
            supported_block_version: 50,
            supported_capabilities: supported_capabilities(),

            mc_block_min_interval: Duration::from_millis(2500),
            max_mc_block_delta_from_bc_to_await_own: 2,

            max_uncommitted_chain_length: 31,
            gas_used_to_import_next_anchor: 250_000_000u64,

            msgs_exec_params: MsgsExecutionParams::default(),
        }
    }
}

pub fn supported_capabilities() -> GlobalCapabilities {
    GlobalCapabilities::from([
        GlobalCapability::CapCreateStatsEnabled,
        GlobalCapability::CapBounceMsgBody,
        GlobalCapability::CapReportVersion,
        GlobalCapability::CapShortDequeue,
        GlobalCapability::CapInitCodeHash,
        GlobalCapability::CapOffHypercube,
        GlobalCapability::CapFixTupleIndexBug,
        GlobalCapability::CapFastStorageStat,
        GlobalCapability::CapMyCode,
        GlobalCapability::CapFullBodyInBounced,
        GlobalCapability::CapStorageFeeToTvm,
        GlobalCapability::CapWorkchains,
        GlobalCapability::CapStcontNewFormat,
        GlobalCapability::CapFastStorageStatBugfix,
        GlobalCapability::CapResolveMerkleCell,
        GlobalCapability::CapFeeInGasUnits,
        GlobalCapability::CapBounceAfterFailedAction,
        GlobalCapability::CapSuspendedList,
        GlobalCapability::CapsTvmBugfixes2022,
    ])
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct MsgsExecutionParams {
    pub buffer_limit: u32,
    pub group_limit: u32,
    pub group_vert_size: u32,
}

impl Default for MsgsExecutionParams {
    fn default() -> Self {
        Self {
            buffer_limit: 20000,
            group_limit: 100,
            group_vert_size: 10,
        }
    }
}

pub struct BlockCollationResult {
    pub candidate: Box<BlockCandidate>,
    pub mc_data: Option<Arc<McData>>,
    /// There are unprocessed internals in shard queue after block collation
    pub has_pending_internals: bool,
}

/// Processed up to info for externals and internals.
#[derive(Debug, Default, Clone)]
pub struct ProcessedUptoInfoStuff {
    /// Externals processed up to point and range
    pub externals: Option<ExternalsProcessedUpto>,
    /// Internals processed up to points and ranges by shards
    pub internals: BTreeMap<ShardIdent, InternalsProcessedUptoStuff>,
    /// Offset of processed messages from buffer.
    /// Will be `!=0` if there were unprocessed messages in buffer from prev collation.
    pub processed_offset: u32,
}

impl TryFrom<ProcessedUptoInfo> for ProcessedUptoInfoStuff {
    type Error = everscale_types::error::Error;

    fn try_from(value: ProcessedUptoInfo) -> std::result::Result<Self, Self::Error> {
        let mut res = Self {
            processed_offset: value.processed_offset,
            externals: value.externals,
            ..Default::default()
        };
        for item in value.internals.iter() {
            let (shard_id_full, int_upto_info) = item?;
            res.internals.insert(
                ShardIdent::try_from(shard_id_full)?,
                InternalsProcessedUptoStuff {
                    processed_to_msg: int_upto_info.processed_to_msg.into(),
                    read_to_msg: int_upto_info.read_to_msg.into(),
                },
            );
        }
        Ok(res)
    }
}

impl TryFrom<ProcessedUptoInfoStuff> for ProcessedUptoInfo {
    type Error = everscale_types::error::Error;

    fn try_from(value: ProcessedUptoInfoStuff) -> std::result::Result<Self, Self::Error> {
        let mut res = Self {
            processed_offset: value.processed_offset,
            externals: value.externals,
            ..Default::default()
        };
        for (shard_id, int_upto_info) in value.internals {
            res.internals.set(
                ShardIdentFull::from(shard_id),
                InternalsProcessedUpto::from(int_upto_info),
            )?;
        }
        Ok(res)
    }
}

#[derive(Debug, Clone)]
pub struct InternalsProcessedUptoStuff {
    /// Internals processed up to message (LT, Hash).
    /// All internals upto this point
    /// already processed during previous blocks collations.
    ///
    /// Needs to read internals from this point to reproduce buffer state from prev collation.
    pub processed_to_msg: InternalMessageKey,
    /// Needs to read internals to this point (LT, Hash) to reproduce buffer state from prev collation.
    pub read_to_msg: InternalMessageKey,
}

impl From<InternalsProcessedUptoStuff> for InternalsProcessedUpto {
    fn from(value: InternalsProcessedUptoStuff) -> Self {
        Self {
            processed_to_msg: value.processed_to_msg.into_tuple(),
            read_to_msg: value.read_to_msg.into_tuple(),
        }
    }
}

#[derive(Debug)]
pub struct McData {
    pub global_id: i32,
    pub block_id: BlockId,

    pub prev_key_block_seqno: u32,
    pub gen_lt: u64,
    pub libraries: Dict<HashBytes, LibDescr>,

    pub total_validator_fees: CurrencyCollection,

    pub global_balance: CurrencyCollection,
    pub shards: ShardHashes,
    pub config: BlockchainConfig,
    pub validator_info: ValidatorInfo,

    pub processed_upto: ProcessedUptoInfoStuff,
}

impl McData {
    pub fn load_from_state(state: &ShardStateStuff) -> Result<Arc<Self>> {
        let block_id = *state.block_id();
        let extra = state.state_extra()?;
        let state = state.as_ref();

        let prev_key_block_seqno = if extra.after_key_block {
            block_id.seqno
        } else if let Some(block_ref) = &extra.last_key_block {
            block_ref.seqno
        } else {
            0
        };

        Ok(Arc::new(Self {
            global_id: state.global_id,
            block_id,

            prev_key_block_seqno,
            gen_lt: state.gen_lt,
            libraries: state.libraries.clone(),
            total_validator_fees: state.total_validator_fees.clone(),

            global_balance: extra.global_balance.clone(),
            shards: extra.shards.clone(),
            config: extra.config.clone(),
            validator_info: extra.validator_info,

            processed_upto: state.processed_upto.load()?.try_into()?,
        }))
    }

    pub fn make_block_ref(&self) -> BlockRef {
        BlockRef {
            end_lt: self.gen_lt,
            seqno: self.block_id.seqno,
            root_hash: self.block_id.root_hash,
            file_hash: self.block_id.file_hash,
        }
    }

    pub fn lt_align(&self) -> u64 {
        1000000
    }
}

#[derive(Clone)]
pub struct BlockCandidate {
    pub block: BlockStuffAug,
    pub prev_blocks_ids: Vec<BlockId>,
    pub top_shard_blocks_ids: Vec<BlockId>,
    pub collated_data: Vec<u8>,
    pub collated_file_hash: HashBytes,
    pub chain_time: u64,
    pub ext_processed_upto_anchor_id: u32,
    pub fees_collected: CurrencyCollection,
    pub funds_created: CurrencyCollection,
    pub created_by: HashBytes,
}

#[derive(Default, Clone)]
pub struct BlockSignatures {
    pub signatures: FastHashMap<HashBytes, ArcSignature>,
}

pub type ArcSignature = Arc<[u8; 64]>;

pub struct ValidatedBlock {
    block: BlockId,
    signatures: BlockSignatures,
    valid: bool,
}

impl ValidatedBlock {
    pub fn new(block: BlockId, signatures: BlockSignatures, valid: bool) -> Self {
        Self {
            block,
            signatures,
            valid,
        }
    }

    pub fn id(&self) -> &BlockId {
        &self.block
    }

    pub fn signatures(&self) -> &BlockSignatures {
        &self.signatures
    }

    pub fn is_valid(&self) -> bool {
        self.valid
    }
    pub fn extract_signatures(self) -> BlockSignatures {
        self.signatures
    }
}

pub struct BlockStuffForSync {
    pub block_stuff_aug: BlockStuffAug,
    pub signatures: FastHashMap<PeerId, ArcSignature>,
    pub prev_blocks_ids: Vec<BlockId>,
    pub top_shard_blocks_ids: Vec<BlockId>,
}

/// (`ShardIdent`, seqno)
pub(crate) type CollationSessionId = (ShardIdent, u32);

#[derive(Clone)]
pub struct CollationSessionInfo {
    /// Sequence number of the collation session
    workchain: i32,
    seqno: u32,
    collators: ValidatorSubsetInfo,
    current_collator_keypair: Option<Arc<KeyPair>>,
}
impl CollationSessionInfo {
    pub fn new(
        workchain: i32,
        seqno: u32,
        collators: ValidatorSubsetInfo,
        current_collator_keypair: Option<Arc<KeyPair>>,
    ) -> Self {
        Self {
            workchain,
            seqno,
            collators,
            current_collator_keypair,
        }
    }
    pub fn workchain(&self) -> i32 {
        self.workchain
    }
    pub fn seqno(&self) -> u32 {
        self.seqno
    }
    pub fn collators(&self) -> &ValidatorSubsetInfo {
        &self.collators
    }

    pub fn current_collator_keypair(&self) -> Option<&Arc<KeyPair>> {
        self.current_collator_keypair.as_ref()
    }
}

pub trait IntAdrExt {
    fn get_address(&self) -> HashBytes;
}
impl IntAdrExt for IntAddr {
    fn get_address(&self) -> HashBytes {
        match self {
            Self::Std(std_addr) => std_addr.address,
            Self::Var(var_addr) => HashBytes::from_slice(var_addr.address.as_slice()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProofFunds {
    pub fees_collected: CurrencyCollection,
    pub funds_created: CurrencyCollection,
}

#[derive(Debug, Clone)]
pub struct TopBlockDescription {
    pub block_id: BlockId,
    pub block_info: BlockInfo,
    pub ext_processed_to_anchor_id: u32,
    pub value_flow: ValueFlow,
    pub proof_funds: ProofFunds,
    pub creators: Vec<HashBytes>,
}

#[derive(Debug)]
pub struct ShortAddr {
    workchain: i32,
    prefix: u64,
}

impl ShortAddr {
    pub fn new(workchain: i32, prefix: u64) -> Self {
        Self { workchain, prefix }
    }
}

impl Addr for ShortAddr {
    fn workchain(&self) -> i32 {
        self.workchain
    }

    fn prefix(&self) -> u64 {
        self.prefix
    }
}
