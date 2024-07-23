use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use everscale_crypto::ed25519::{KeyPair, PublicKey};
use everscale_types::models::{BlockId, BlockIdShort, ShardIdent, ValidatorDescription};
use tycho_network::{Network, OverlayService, PeerId, PeerResolver};
use tycho_util::FastHashMap;

pub use self::impls::*;

pub mod proto;
pub mod rpc;

mod impls {
    pub use self::std_impl::{ValidatorStdImpl, ValidatorStdImplConfig};

    mod std_impl;
}

// === Validator ===

#[async_trait]
pub trait Validator: Send + Sync + 'static {
    /// Returns the key pair used by the validator.
    fn key_pair(&self) -> Arc<KeyPair>;

    /// Adds a new session for the specified shard.
    fn add_session(
        &self,
        shard_ident: &ShardIdent,
        session_id: u32,
        validators: &[ValidatorDescription],
    ) -> Result<()>;

    /// Collects signatures for the specified block.
    async fn validate(&self, session_id: u32, block_id: &BlockId) -> Result<ValidationStatus>;

    /// Cancels validation before the specified block.
    fn cancel_validation(&self, before: &BlockIdShort) -> Result<()>;
}

// === Types ===

pub struct ValidatorNetworkContext {
    pub network: Network,
    pub peer_resolver: PeerResolver,
    pub overlays: OverlayService,
    pub zerostate_id: BlockId,
}

#[derive(Debug, Clone)]
pub enum ValidationStatus {
    Skipped,
    Complete(BlockSignatures),
}

pub type BlockSignatures = FastHashMap<PeerId, Arc<[u8; 64]>>;

#[derive(Debug, Clone)]
pub struct BriefValidatorDescr {
    pub peer_id: PeerId,
    pub public_key: PublicKey,
    pub weight: u64,
}

impl TryFrom<&ValidatorDescription> for BriefValidatorDescr {
    type Error = anyhow::Error;

    fn try_from(descr: &ValidatorDescription) -> Result<Self> {
        let Some(public_key) = PublicKey::from_bytes(descr.public_key.0) else {
            anyhow::bail!("invalid validator public key");
        };

        Ok(Self {
            peer_id: PeerId(descr.public_key.0),
            public_key,
            weight: descr.weight,
        })
    }
}
