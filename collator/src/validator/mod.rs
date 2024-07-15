use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::cell::HashBytes;
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent, Signature, ValidatorDescription};
use tokio::sync::mpsc;
use tycho_network::{Network, OverlayService, PeerId, PeerResolver};
use tycho_util::FastHashMap;

pub mod config;
pub mod network;
pub mod proto;
pub mod rpc;
pub mod state;
pub mod types;

mod impls {

    mod new_impl;
    mod std_impl;
}

// === Validator ===

#[async_trait]
pub trait Validator: Send + Sync {
    /// Returns the key pair used by the validator.
    fn key_pair(&self) -> Arc<KeyPair>;

    /// Adds a new session for the specified shard.
    async fn add_session(
        &self,
        session_id: u32,
        shard_ident: &ShardIdent,
        validators: &[ValidatorDescription],
    ) -> Result<()>;

    /// Collects signatures for the specified block.
    async fn validate(&self, session_id: u32, block_id: &BlockId) -> Result<()>;

    /// Cancels validation before the specified block.
    async fn cancel_validation(&self, before: &BlockIdShort) -> Result<()>;
}

// === Types ===

pub struct NetworkContext {
    pub network: Network,
    pub peer_resolver: PeerResolver,
    pub overlays: OverlayService,
    pub zerostate_id: BlockId,
}

pub type ValidatedBlockTx = mpsc::Sender<ValidatedBlock>;
pub type ValidatedBlockRx = mpsc::Receiver<ValidatedBlock>;

pub struct ValidatedBlock {
    pub block_id: BlockId,
    pub status: ValidationStatus,
}

pub enum ValidationStatus {
    Invalid,
    Skipped,
    Valid(BlockSignatures),
}

pub type BlockSignatures = FastHashMap<HashBytes, Signature>;

pub struct ValidatorInfo {
    pub peer_id: PeerId,
    pub public_key: PublicKey,
    pub weight: u64,
}
