use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::*;
use everscale_types::prelude::*;
use processed_upto::{ProcessedUptoInfoExtension, ProcessedUptoInfoStuff};
use serde::{Deserialize, Serialize};
use tycho_block_util::block::{BlockStuffAug, ValidatorSubsetInfo};
use tycho_block_util::queue::{QueueDiffStuffAug, QueueKey, QueuePartitionIdx};
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

use crate::collator::ForceMasterCollation;
use crate::mempool::MempoolAnchorId;
use crate::utils::block::detect_top_processed_to_anchor;
use crate::validator::ValidationSessionId;

pub mod processed_upto;

#[derive(Debug, Clone)]
pub struct CollatorConfig {
    pub supported_block_version: u32,
    pub supported_capabilities: GlobalCapabilities,
    pub min_mc_block_delta_from_bc_to_sync: u32,
    pub check_value_flow: bool,
    pub validate_config: bool,
    pub fast_sync: bool,
}

impl Default for CollatorConfig {
    fn default() -> Self {
        Self {
            supported_block_version: 100,
            supported_capabilities: supported_capabilities(),
            min_mc_block_delta_from_bc_to_sync: 3,
            check_value_flow: false,
            validate_config: true,
            fast_sync: true,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Serialize, Deserialize)]
struct PartialCollatorConfig {
    min_mc_block_delta_from_bc_to_sync: u32,
    check_value_flow: bool,
    validate_config: bool,
    #[serde(default = "default_true")]
    fast_sync: bool,
}

impl<'de> serde::Deserialize<'de> for CollatorConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let partial = PartialCollatorConfig::deserialize(deserializer)?;

        Ok(Self {
            min_mc_block_delta_from_bc_to_sync: partial.min_mc_block_delta_from_bc_to_sync,
            check_value_flow: partial.check_value_flow,
            validate_config: partial.validate_config,
            fast_sync: partial.fast_sync,
            ..Default::default()
        })
    }
}

impl serde::Serialize for CollatorConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        PartialCollatorConfig {
            min_mc_block_delta_from_bc_to_sync: self.min_mc_block_delta_from_bc_to_sync,
            check_value_flow: self.check_value_flow,
            validate_config: self.validate_config,
            fast_sync: self.fast_sync,
        }
        .serialize(serializer)
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

pub struct BlockCollationResult {
    pub collation_session_id: CollationSessionId,
    pub candidate: Box<BlockCandidate>,
    pub prev_mc_block_id: BlockId,
    pub mc_data: Option<Arc<McData>>,
    pub collation_config: Arc<CollationConfig>,
    pub force_next_mc_block: ForceMasterCollation,
}

#[derive(Debug)]
pub struct McData {
    pub global_id: i32,
    pub block_id: BlockId,

    /// Last known key block seqno. Will be equal to `McData.block_id.seqno` if it is a key block.
    pub prev_key_block_seqno: u32,

    pub gen_lt: u64,
    pub gen_chain_time: u64,
    pub libraries: Dict<HashBytes, LibDescr>,

    pub total_validator_fees: CurrencyCollection,

    pub global_balance: CurrencyCollection,
    pub shards: Vec<(ShardIdent, ShardDescriptionShort)>,
    pub config: BlockchainConfig,
    pub validator_info: ValidatorInfo,
    pub consensus_info: ConsensusInfo,

    pub processed_upto: ProcessedUptoInfoStuff,

    /// Minimal of top processed to anchors
    /// from master block and its top shards
    pub top_processed_to_anchor: MempoolAnchorId,

    pub ref_mc_state_handle: RefMcStateHandle,

    pub shards_processed_to_by_partitions: FastHashMap<ShardIdent, (bool, ProcessedToByPartitions)>,
}

impl McData {
    pub fn load_from_state(
        state_stuff: &ShardStateStuff,
        all_shards_processed_to_by_partitions: FastHashMap<
            ShardIdent,
            (bool, ProcessedToByPartitions),
        >,
    ) -> Result<Arc<Self>> {
        let block_id = *state_stuff.block_id();
        let extra = state_stuff.state_extra()?;
        let state = state_stuff.as_ref();

        let prev_key_block_seqno = if extra.after_key_block {
            block_id.seqno
        } else if let Some(block_ref) = &extra.last_key_block {
            block_ref.seqno
        } else {
            0
        };

        let processed_upto: ProcessedUptoInfoStuff = state.processed_upto.load()?.try_into()?;

        let shards = extra.shards.as_vec()?;
        let top_processed_to_anchor = detect_top_processed_to_anchor(
            shards.iter().map(|(_, d)| *d),
            processed_upto.get_min_externals_processed_to()?.0,
        );

        let shards_processed_to_by_partitions = all_shards_processed_to_by_partitions
            .into_iter()
            .filter(|(shard_id, _)| !shard_id.is_masterchain())
            .collect();

        Ok(Arc::new(Self {
            global_id: state.global_id,
            block_id,

            prev_key_block_seqno,
            gen_lt: state.gen_lt,
            gen_chain_time: state_stuff.get_gen_chain_time(),
            libraries: state.libraries.clone(),
            total_validator_fees: state.total_validator_fees.clone(),

            global_balance: extra.global_balance.clone(),
            shards,
            config: extra.config.clone(),
            validator_info: extra.validator_info,
            consensus_info: extra.consensus_info,

            processed_upto,
            top_processed_to_anchor,

            ref_mc_state_handle: state_stuff.ref_mc_state_handle().clone(),
            shards_processed_to_by_partitions,
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
    pub ref_by_mc_seqno: u32,
    pub block: BlockStuffAug,
    pub is_key_block: bool,
    /// If current block is a key master block and `ConsensusConfig` was changed.
    /// `None` - if it is a shard block or not a key master block.
    pub consensus_config_changed: Option<bool>,
    pub prev_blocks_ids: Vec<BlockId>,
    pub top_shard_blocks_ids: Vec<BlockId>,
    pub collated_data: Vec<u8>,
    pub collated_file_hash: HashBytes,
    pub chain_time: u64,
    pub processed_to_anchor_id: u32,
    pub value_flow: ValueFlow,
    pub created_by: HashBytes,
    pub queue_diff_aug: QueueDiffStuffAug,
    pub consensus_info: ConsensusInfo,
    pub processed_upto: ProcessedUptoInfoStuff,
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
    /// A masterchain block seqno which will reference this block.
    pub ref_by_mc_seqno: u32,

    pub block_stuff_aug: BlockStuffAug,
    pub queue_diff_aug: QueueDiffStuffAug,
    pub signatures: FastHashMap<PeerId, ArcSignature>,
    pub total_signature_weight: u64,
    pub prev_blocks_ids: Vec<BlockId>,
    pub top_shard_blocks_ids: Vec<BlockId>,

    pub consensus_info: ConsensusInfo,
}

/// (`ShardIdent`, seqno, subset `short_hash`)
pub(crate) type CollationSessionId = (ShardIdent, u32, u32);

#[derive(Clone)]
pub struct CollationSessionInfo {
    shard: ShardIdent,
    /// Sequence number of the collation session
    seqno: u32,
    collators: ValidatorSubsetInfo,
    current_collator_keypair: Option<Arc<KeyPair>>,
}
impl CollationSessionInfo {
    pub fn new(
        shard: ShardIdent,
        seqno: u32,
        collators: ValidatorSubsetInfo,
        current_collator_keypair: Option<Arc<KeyPair>>,
    ) -> Self {
        Self {
            shard,
            seqno,
            collators,
            current_collator_keypair,
        }
    }

    pub fn id(&self) -> CollationSessionId {
        (self.shard, self.seqno, self.collators.short_hash)
    }

    pub fn get_validation_session_id(&self) -> ValidationSessionId {
        (self.seqno, self.collators.short_hash)
    }

    pub fn shard(&self) -> ShardIdent {
        self.shard
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
impl fmt::Debug for CollationSessionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollationSessionInfo")
            .field("shard", &self.shard)
            .field("seqno", &self.seqno)
            .field("collators", &self.collators)
            .field(
                "current_collator_pubkey",
                &self
                    .current_collator_keypair
                    .as_ref()
                    .map(|kp| kp.public_key),
            )
            .finish()
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

#[derive(Debug, Clone)]
pub struct TopBlockDescription {
    pub block_id: BlockId,
    pub block_info: BlockInfo,
    pub processed_to_anchor_id: u32,
    pub value_flow: ValueFlow,
    pub proof_funds: ShardFeeCreated,
    #[cfg(feature = "block-creator-stats")]
    pub creators: Vec<HashBytes>,
    pub processed_to_by_partitions: ProcessedToByPartitions,
}

#[derive(Debug, Clone)]
pub struct TopShardBlockInfo {
    pub block_id: BlockId,
    pub processed_to_by_partitions: ProcessedToByPartitions,
}

pub type ProcessedTo = BTreeMap<ShardIdent, QueueKey>;
pub type ProcessedToByPartitions = FastHashMap<QueuePartitionIdx, ProcessedTo>;

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

pub trait BlockIdExt {
    fn get_next_id_short(&self) -> BlockIdShort;
}
impl BlockIdExt for BlockId {
    fn get_next_id_short(&self) -> BlockIdShort {
        BlockIdShort {
            shard: self.shard,
            seqno: self.seqno + 1,
        }
    }
}
impl BlockIdExt for BlockIdShort {
    fn get_next_id_short(&self) -> BlockIdShort {
        BlockIdShort {
            shard: self.shard,
            seqno: self.seqno + 1,
        }
    }
}

pub trait ShardDescriptionExt {
    fn get_block_id(&self, shard_id: ShardIdent) -> BlockId;
}
impl ShardDescriptionExt for ShardDescription {
    fn get_block_id(&self, shard_id: ShardIdent) -> BlockId {
        BlockId {
            shard: shard_id,
            seqno: self.seqno,
            root_hash: self.root_hash,
            file_hash: self.file_hash,
        }
    }
}

pub struct DebugDisplay<T>(pub T);
impl<T: std::fmt::Display> std::fmt::Debug for DebugDisplay<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

pub struct DebugDisplayOpt<T>(pub Option<T>);
impl<T: std::fmt::Display> std::fmt::Debug for DebugDisplayOpt<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0.as_ref().map(DebugDisplay), f)
    }
}

pub(super) struct DisplayIter<I>(pub I);
impl<I> std::fmt::Display for DisplayIter<I>
where
    I: Iterator<Item: std::fmt::Display> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.clone().map(DebugDisplay))
            .finish()
    }
}

pub(super) struct DisplayIntoIter<I>(pub I);
impl<I> std::fmt::Display for DisplayIntoIter<I>
where
    I: IntoIterator<Item: std::fmt::Display> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.clone().into_iter().map(DebugDisplay))
            .finish()
    }
}

pub(super) struct DebugIter<I>(pub I);
impl<I> std::fmt::Debug for DebugIter<I>
where
    I: Iterator<Item: std::fmt::Debug> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.0.clone()).finish()
    }
}

pub(super) struct DisplayAsShortId<'a>(pub &'a BlockId);
impl std::fmt::Debug for DisplayAsShortId<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for DisplayAsShortId<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_short_id())
    }
}

pub(super) struct DisplayBlockIdsIter<I>(pub I);
impl<'a, I> std::fmt::Debug for DisplayBlockIdsIter<I>
where
    I: Iterator<Item = &'a BlockId> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl<'a, I> std::fmt::Display for DisplayBlockIdsIter<I>
where
    I: Iterator<Item = &'a BlockId> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.clone().map(DisplayAsShortId))
            .finish()
    }
}

pub(super) struct DisplayBlockIdsIntoIter<I>(pub I);
impl<'a, I> std::fmt::Debug for DisplayBlockIdsIntoIter<I>
where
    I: IntoIterator<Item = &'a BlockId> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl<'a, I> std::fmt::Display for DisplayBlockIdsIntoIter<I>
where
    I: IntoIterator<Item = &'a BlockId> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.clone().into_iter().map(DisplayAsShortId))
            .finish()
    }
}

pub(super) struct DisplayTupleRef<'a, T1, T2>(pub &'a (T1, T2));
impl<T1: std::fmt::Display, T2: std::fmt::Display> std::fmt::Debug for DisplayTupleRef<'_, T1, T2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl<T1: std::fmt::Display, T2: std::fmt::Display> std::fmt::Display
    for DisplayTupleRef<'_, T1, T2>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.0 .0, self.0 .1)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct ShardDescriptionShort {
    pub ext_processed_to_anchor_id: u32,
    pub top_sc_block_updated: bool,
    pub end_lt: u64,
    pub seqno: u32,
    pub root_hash: HashBytes,
    pub file_hash: HashBytes,
}

impl<BorrowShardDescription: Borrow<ShardDescription>> From<BorrowShardDescription>
    for ShardDescriptionShort
{
    fn from(borrow_shard: BorrowShardDescription) -> ShardDescriptionShort {
        let shard = borrow_shard.borrow();
        Self {
            ext_processed_to_anchor_id: shard.ext_processed_to_anchor_id,
            top_sc_block_updated: shard.top_sc_block_updated,
            end_lt: shard.end_lt,
            seqno: shard.seqno,
            root_hash: shard.root_hash,
            file_hash: shard.file_hash,
        }
    }
}

impl ShardDescriptionExt for ShardDescriptionShort {
    fn get_block_id(&self, shard_id: ShardIdent) -> BlockId {
        BlockId {
            shard: shard_id,
            seqno: self.seqno,
            root_hash: self.root_hash,
            file_hash: self.file_hash,
        }
    }
}

pub trait ShardHashesExt<T> {
    fn as_vec(&self) -> Result<Vec<(ShardIdent, T)>>;
}
impl<T> ShardHashesExt<T> for ShardHashes
where
    T: From<ShardDescription>,
{
    fn as_vec(&self) -> Result<Vec<(ShardIdent, T)>> {
        let mut res = vec![];
        for item in self.iter() {
            let (shard_id, descr) = item?;
            res.push((shard_id, descr.into()));
        }
        Ok(res)
    }
}

pub trait ShardIdentExt {
    fn contains_prefix(&self, workchain_id: i32, prefix_without_tag: u64) -> bool;
}

impl ShardIdentExt for ShardIdent {
    fn contains_prefix(&self, workchain_id: i32, prefix_without_tag: u64) -> bool {
        if self.workchain() == workchain_id {
            if self.prefix() == 0x8000_0000_0000_0000u64 {
                return true;
            }
            let shift = 64 - self.prefix_len();
            return (self.prefix() >> shift) == (prefix_without_tag >> shift);
        }
        false
    }
}
