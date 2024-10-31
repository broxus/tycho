use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::*;
use everscale_types::prelude::*;
use serde::{Deserialize, Serialize};
use tycho_block_util::block::{BlockStuffAug, ValidatorSubsetInfo};
use tycho_block_util::queue::{QueueDiffStuffAug, QueueKey};
use tycho_block_util::state::{RefMcStateHandle, ShardStateStuff};
use tycho_network::PeerId;
use tycho_util::FastHashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct CollatorConfig {
    pub supported_block_version: u32,
    pub supported_capabilities: GlobalCapabilities,
    pub min_mc_block_delta_from_bc_to_sync: u32,
}

impl Default for CollatorConfig {
    fn default() -> Self {
        Self {
            supported_block_version: 50,
            supported_capabilities: supported_capabilities(),
            min_mc_block_delta_from_bc_to_sync: 3,
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

pub struct BlockCollationResult {
    pub collation_session_id: CollationSessionId,
    pub candidate: Box<BlockCandidate>,
    pub prev_mc_block_id: BlockId,
    pub mc_data: Option<Arc<McData>>,
    pub collation_config: Arc<CollationConfig>,
    /// There are unprocessed messages in buffer
    /// or shard queue after block collation
    pub has_unprocessed_messages: bool,
}

/// Processed up to info for externals and internals.
#[derive(Default, Clone)]
pub struct ProcessedUptoInfoStuff {
    /// Externals processed up to point and range
    pub externals: Option<ExternalsProcessedUpto>,
    /// Internals processed up to points and ranges by shards
    pub internals: BTreeMap<ShardIdent, InternalsProcessedUptoStuff>,
    /// Offset of processed messages from buffer.
    /// Will be `!=0` if there were unprocessed messages in buffer from prev collation.
    pub processed_offset: u32,
}

impl std::fmt::Debug for ProcessedUptoInfoStuff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessedUpto")
            .field("offset", &self.processed_offset)
            .field(
                "externals",
                &self.externals.as_ref().map(DisplayExternalsProcessedUpto),
            )
            .field(
                "internals",
                &DebugIter(self.internals.iter().map(|(k, v)| (k, DebugDisplay(v)))),
            )
            .finish()
    }
}

pub struct DisplayExternalsProcessedUpto<'a>(pub &'a ExternalsProcessedUpto);
impl std::fmt::Debug for DisplayExternalsProcessedUpto<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for DisplayExternalsProcessedUpto<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "processed_to: {}, read_to: {}",
            DisplayTupleRef(&self.0.processed_to),
            DisplayTupleRef(&self.0.read_to),
        )
    }
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
    pub processed_to_msg: QueueKey,
    /// Needs to read internals to this point (LT, Hash) to reproduce buffer state from prev collation.
    pub read_to_msg: QueueKey,
}

impl std::fmt::Display for InternalsProcessedUptoStuff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "processed_to: {}, read_to: {}",
            self.processed_to_msg, self.read_to_msg
        )
    }
}

impl From<InternalsProcessedUptoStuff> for InternalsProcessedUpto {
    fn from(value: InternalsProcessedUptoStuff) -> Self {
        Self {
            processed_to_msg: value.processed_to_msg.split(),
            read_to_msg: value.read_to_msg.split(),
        }
    }
}

#[derive(Debug)]
pub struct McData {
    pub global_id: i32,
    pub block_id: BlockId,

    pub prev_key_block_seqno: u32,
    pub gen_lt: u64,
    pub gen_chain_time: u64,
    pub libraries: Dict<HashBytes, LibDescr>,

    pub total_validator_fees: CurrencyCollection,

    pub global_balance: CurrencyCollection,
    pub shards: ShardHashes,
    pub config: BlockchainConfig,
    pub validator_info: ValidatorInfo,
    pub consensus_info: ConsensusInfo,

    pub processed_upto: ProcessedUptoInfoStuff,

    pub ref_mc_state_handle: RefMcStateHandle,
}

impl McData {
    pub fn load_from_state(state_stuff: &ShardStateStuff) -> Result<Arc<Self>> {
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

        Ok(Arc::new(Self {
            global_id: state.global_id,
            block_id,

            prev_key_block_seqno,
            gen_lt: state.gen_lt,
            gen_chain_time: state_stuff.get_gen_chain_time(),
            libraries: state.libraries.clone(),
            total_validator_fees: state.total_validator_fees.clone(),

            global_balance: extra.global_balance.clone(),
            shards: extra.shards.clone(),
            config: extra.config.clone(),
            validator_info: extra.validator_info,
            consensus_info: extra.consensus_info,

            processed_upto: state.processed_upto.load()?.try_into()?,

            ref_mc_state_handle: state_stuff.ref_mc_state_handle().clone(),
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
    pub prev_blocks_ids: Vec<BlockId>,
    pub top_shard_blocks_ids: Vec<BlockId>,

    pub consensus_info: ConsensusInfo,
}

/// (`ShardIdent`, seqno)
pub(crate) type CollationSessionId = (ShardIdent, u32);

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
        (self.shard, self.seqno)
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
    pub processed_to_anchor_id: u32,
    pub value_flow: ValueFlow,
    pub proof_funds: ProofFunds,
    #[cfg(feature = "block-creator-stats")]
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

pub(super) struct DisplayTuple<T1, T2>(pub (T1, T2));
impl<T1: std::fmt::Display, T2: std::fmt::Display> std::fmt::Debug for DisplayTuple<T1, T2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl<T1: std::fmt::Display, T2: std::fmt::Display> std::fmt::Display for DisplayTuple<T1, T2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.0 .0, self.0 .1)
    }
}
