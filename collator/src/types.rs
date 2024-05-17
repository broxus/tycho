use std::sync::Arc;

use everscale_crypto::ed25519::KeyPair;
use everscale_types::cell::HashBytes;
use everscale_types::models::{
    Block, BlockId, CurrencyCollection, OwnedMessage, ShardIdent, Signature,
};
use tycho_block_util::block::{BlockStuffAug, ValidatorSubsetInfo};
use tycho_block_util::state::ShardStateStuff;
use tycho_network::{DhtClient, OverlayService, PeerResolver};
use tycho_util::FastHashMap;

pub struct CollationConfig {
    pub key_pair: Arc<KeyPair>,
    pub mc_block_min_interval_ms: u64,
    pub max_mc_block_delta_from_bc_to_await_own: i32,

    pub supported_block_version: u32,
    pub supported_capabilities: u64,

    pub max_collate_threads: u16,

    #[cfg(any(test, feature = "test"))]
    pub test_validators_keypairs: Vec<Arc<KeyPair>>,
}

pub struct BlockCollationResult {
    pub candidate: BlockCandidate,
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

pub(crate) trait MessageExt {
    fn id_hash(&self) -> &HashBytes;
}
impl MessageExt for OwnedMessage {
    fn id_hash(&self) -> &HashBytes {
        self.body.0.repr_hash()
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
