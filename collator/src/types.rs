use std::sync::Arc;
use std::time::Duration;

use everscale_crypto::ed25519::KeyPair;
use everscale_types::cell::HashBytes;
use everscale_types::models::{
    Block, BlockId, BlockInfo, BlockLimits, BlockParamLimits, CurrencyCollection,
    GlobalCapabilities, GlobalCapability, IntAddr, ShardIdent, Signature, ValueFlow,
};
use serde::{Deserialize, Serialize};
use tycho_block_util::block::{BlockStuffAug, ValidatorSubsetInfo};
use tycho_block_util::state::ShardStateStuff;
use tycho_network::{DhtClient, OverlayService, PeerResolver};
use tycho_util::{serde_helpers, FastHashMap};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct CollationConfig {
    pub supported_block_version: u32,
    pub supported_capabilities: GlobalCapabilities,

    #[serde(with = "serde_helpers::humantime")]
    pub mc_block_min_interval: Duration,
    pub max_mc_block_delta_from_bc_to_await_own: i32,
    pub max_uncommitted_chain_length: u32,
    pub uncommitted_chain_to_import_next_anchor: u32,

    pub block_limits: BlockLimits,

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
            uncommitted_chain_to_import_next_anchor: 4,

            block_limits: BlockLimits {
                bytes: BlockParamLimits {
                    underload: 131072,
                    soft_limit: 524288,
                    hard_limit: 1048576,
                },
                gas: BlockParamLimits {
                    underload: 900000,
                    soft_limit: 1200000,
                    hard_limit: 2000000,
                },
                lt_delta: BlockParamLimits {
                    underload: 1000,
                    soft_limit: 5000,
                    hard_limit: 10000,
                },
            },

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
    pub set_size: u32,
    pub min_externals_per_set: u32,
    pub group_limit: u32,
    pub group_vert_size: u32,
    pub max_exec_threads: u32,
}

impl Default for MsgsExecutionParams {
    fn default() -> Self {
        Self {
            set_size: 1000,
            min_externals_per_set: 300,
            group_limit: 1000,
            group_vert_size: 1000,
            max_exec_threads: 8,
        }
    }
}

pub struct BlockCollationResult {
    pub candidate: Box<BlockCandidate>,
    pub new_state_stuff: ShardStateStuff,
    /// There are unprocessed internals in shard queue after block collation
    pub has_pending_internals: bool,
}

#[derive(Clone)]
pub struct BlockCandidate {
    pub block_id: BlockId,
    pub block: Block,
    pub prev_blocks_ids: Vec<BlockId>,
    pub top_shard_blocks_ids: Vec<BlockId>,
    pub data: Vec<u8>,
    pub collated_data: Vec<u8>,
    pub collated_file_hash: HashBytes,
    pub chain_time: u64,
}

#[derive(Clone)]
pub enum OnValidatedBlockEvent {
    ValidByState,
    Invalid,
    Valid(BlockSignatures),
}

impl OnValidatedBlockEvent {
    pub fn is_valid(&self) -> bool {
        match self {
            Self::ValidByState | Self::Valid(_) => true,
            Self::Invalid => false,
        }
    }
}

#[derive(Default, Clone)]
pub struct BlockSignatures {
    pub signatures: FastHashMap<HashBytes, Signature>,
}

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
    // STUB: will not parse Block because candidate does not contain real block
    // TODO: remove `block_id` and make `block_stuff: BlockStuff` when collator will generate real blocks
    pub block_id: BlockId,
    pub block_stuff_aug: BlockStuffAug,
    pub signatures: FastHashMap<HashBytes, Signature>,
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

pub(crate) trait ShardIdentExt {
    fn contains_address(&self, addr: &IntAddr) -> bool;
}
impl ShardIdentExt for ShardIdent {
    fn contains_address(&self, addr: &IntAddr) -> bool {
        self.workchain() == addr.workchain() && self.contains_account(&addr.get_address())
    }
}

#[derive(Clone)]
pub struct ValidatorNetwork {
    pub overlay_service: OverlayService,
    pub peer_resolver: PeerResolver,
    pub dht_client: DhtClient,
}

impl From<NodeNetwork> for ValidatorNetwork {
    fn from(node_network: NodeNetwork) -> Self {
        Self {
            overlay_service: node_network.overlay_service,
            peer_resolver: node_network.peer_resolver,
            dht_client: node_network.dht_client,
        }
    }
}

#[derive(Clone)]
pub struct NodeNetwork {
    pub overlay_service: OverlayService,
    pub peer_resolver: PeerResolver,
    pub dht_client: DhtClient,
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
    pub value_flow: ValueFlow,
    pub proof_funds: ProofFunds,
    pub creators: Vec<HashBytes>,
}
