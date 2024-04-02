use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, ShardIdent, Signature, ValidatorDescription};
use std::collections::HashMap;

use tycho_block_util::block::ValidatorSubsetInfo;
use tycho_network::{DhtClient, Network, OverlayService, PeerResolver};

use self::ext_types::{Block, BlockProof, BlockSignature, KeyId, ShardStateUnsplit, UInt256};

pub struct CollationConfig {
    pub key_pair: KeyPair,
    pub mc_block_min_interval_ms: u64,
}

pub struct BlockCollationResult {
    pub candidate: BlockCandidate,
    pub new_state: ShardStateUnsplit,
}

#[derive(Clone)]
pub(crate) struct BlockCandidate {
    block_id: BlockId,
    prev_blocks_ids: Vec<BlockId>,
    data: Vec<u8>,
    collated_data: Vec<u8>,
    collated_file_hash: UInt256,
}
impl BlockCandidate {
    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }
    pub fn shard_id(&self) -> &ShardIdent {
        &self.block_id.shard
    }
    pub fn own_signature(&self) -> BlockSignature {
        todo!()
    }
    pub fn chain_time(&self) -> u64 {
        todo!()
    }
}

pub struct BlockSignatures {
    good_sigs: Vec<(KeyId, BlockSignature)>,
    bad_sigs: Vec<(KeyId, BlockSignature)>,
}
impl BlockSignatures {
    pub fn is_valid(&self) -> bool {
        todo!()
    }
}

pub struct ValidatedBlock {
    block: BlockId,
    signatures: Vec<(HashBytes, Signature)>,
    valid: bool,
}

impl ValidatedBlock {
    pub fn new(block: BlockId, signatures: Vec<(HashBytes, Signature)>, valid: bool) -> Self {
        Self {
            block,
            signatures,
            valid,
        }
    }

    pub fn id(&self) -> &BlockId {
        &self.block
    }

    pub fn signatures(&self) -> &Vec<(HashBytes, Signature)> {
        &self.signatures
    }

    pub fn is_valid(&self) -> bool {
        self.valid
    }
}

pub struct BlockStuff {
    id: BlockId,
    block: Option<Block>,
    // other stuff...
}

pub struct BlockProofStuff {
    id: BlockId,
    proof: BlockProof,
    // other stuff...
}

pub struct BlockStuffForSync {
    pub block_stuff: BlockStuff,
    pub signatures: BlockSignatures,
    pub prev_blocks_ids: Vec<BlockId>,
}

/// (ShardIdent, seqno)
pub type CollationSessionId = (ShardIdent, u32);

pub struct CollationSessionInfo {
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

pub(crate) mod ext_types {
    pub use stubs::*;
    pub mod stubs {
        pub struct KeyId([u8; 32]);
        pub struct BlockSignature(pub Vec<u8>);
        #[derive(Clone)]
        pub struct UInt256([u8; 32]);
        #[derive(Clone)]
        pub struct BlockHashId;
        pub struct Block;
        pub struct BlockProof;
        #[derive(Clone)]
        pub struct ShardAccounts;
        pub struct Cell;
        pub struct CurrencyCollection;
        pub struct ShardStateUnsplit;
        pub struct McStateExtra;
        pub struct BlockHandle;

        pub struct ValidatorId;
        pub struct ValidatorDescr;
        impl ValidatorDescr {
            pub fn id(&self) -> ValidatorId {
                todo!()
            }
        }
        pub struct ValidatorSet;
    }
}

#[derive(Clone)]
pub struct ValidatorNetwork {
    pub overlay_service: OverlayService,
    pub peer_resolver: PeerResolver,
    pub dht_client: DhtClient,
}
