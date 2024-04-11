use everscale_crypto::ed25519::KeyPair;
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, OwnedMessage, ShardIdent, ShardStateUnsplit, Signature};
use std::collections::HashMap;

use tycho_block_util::block::ValidatorSubsetInfo;
use tycho_network::{DhtClient, OverlayService, PeerResolver};

use std::sync::Arc;

use tycho_block_util::block::BlockStuff;

pub struct CollationConfig {
    pub key_pair: KeyPair,
    pub mc_block_min_interval_ms: u64,
}

pub(crate) struct BlockCollationResult {
    pub candidate: BlockCandidate,
    pub new_state: ShardStateUnsplit,
}

#[derive(Clone)]
pub(crate) struct BlockCandidate {
    block_id: BlockId,
    prev_blocks_ids: Vec<BlockId>,
    top_shard_blocks_ids: Vec<BlockId>,
    data: Vec<u8>,
    collated_data: Vec<u8>,
    collated_file_hash: HashBytes,
    chain_time: u64,
}
impl BlockCandidate {
    pub fn new(
        block_id: BlockId,
        prev_blocks_ids: Vec<BlockId>,
        top_shard_blocks_ids: Vec<BlockId>,
        data: Vec<u8>,
        collated_data: Vec<u8>,
        collated_file_hash: HashBytes,
        chain_time: u64,
    ) -> Self {
        Self {
            block_id,
            prev_blocks_ids,
            top_shard_blocks_ids,
            data,
            collated_data,
            collated_file_hash,
            chain_time,
        }
    }
    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }
    pub fn shard_id(&self) -> &ShardIdent {
        &self.block_id.shard
    }
    pub fn chain_time(&self) -> u64 {
        self.chain_time
    }
    pub fn prev_blocks_ids(&self) -> &[BlockId] {
        &self.prev_blocks_ids
    }
    pub fn top_shard_blocks_ids(&self) -> &[BlockId] {
        &self.top_shard_blocks_ids
    }
}

pub(crate) enum OnValidatedBlockEvent {
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
pub(crate) struct BlockSignatures {
    pub signatures: HashMap<HashBytes, Signature>,
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

pub(crate) struct BlockStuffForSync {
    //STUB: will not parse Block because candidate does not contain real block
    //TODO: remove `block_id` and make `block_stuff: BlockStuff` when collator will generate real blocks
    pub block_id: BlockId,
    pub block_stuff: Option<BlockStuff>,
    pub signatures: HashMap<HashBytes, Signature>,
    pub prev_blocks_ids: Vec<BlockId>,
}

/// (`ShardIdent`, seqno)
pub(crate) type CollationSessionId = (ShardIdent, u32);

#[derive(Clone)]
pub(crate) struct CollationSessionInfo {
    /// Sequence number of the collation session
    seqno: u32,
    collators: ValidatorSubsetInfo,
    current_collator_keypair: Option<KeyPair>,
}
impl CollationSessionInfo {
    pub fn new(
        seqno: u32,
        collators: ValidatorSubsetInfo,
        current_collator_keypair: Option<KeyPair>,
    ) -> Self {
        Self {
            seqno,
            collators,
            current_collator_keypair,
        }
    }
    pub fn seqno(&self) -> u32 {
        self.seqno
    }
    pub fn collators(&self) -> &ValidatorSubsetInfo {
        &self.collators
    }

    pub fn current_collator_keypair(&self) -> Option<&KeyPair> {
        self.current_collator_keypair.as_ref()
    }
}

pub(crate) struct MessageContainer {
    id_hash: HashBytes,
    pub message: Arc<OwnedMessage>,
}
impl MessageContainer {
    pub fn from_message(message: Arc<OwnedMessage>) -> Self {
        let id_hash = *message.body.0.repr_hash();
        Self { id_hash, message }
    }
    pub fn id_hash(&self) -> &HashBytes {
        &self.id_hash
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
