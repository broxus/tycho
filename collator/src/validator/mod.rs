use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tycho_crypto::ed25519::PublicKey;
use tycho_network::{Network, OverlayService, PeerId, PeerResolver};
use tycho_types::models::{BlockId, BlockIdShort, ShardIdent, ValidatorDescription};
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
    /// Adds a new session for the specified shard.
    fn add_session(&self, info: AddSession<'_>) -> Result<()>;

    /// Collects signatures for the specified block.
    async fn validate(
        &self,
        session_id: ValidationSessionId,
        block_id: &BlockId,
    ) -> Result<ValidationStatus>;

    /// Cancels validation before the specified block.
    ///
    /// TODO: Simplify implementation by somehow passing a corresponding `session_id` as well.
    fn cancel_validation(&self, before: &BlockIdShort) -> Result<()>;
}

// === Types ===

pub struct ValidatorNetworkContext {
    pub network: Network,
    pub peer_resolver: PeerResolver,
    pub overlays: OverlayService,
    pub zerostate_id: BlockId,
}

/// (seqno, subset `short_hash`)
pub(super) type ValidationSessionId = (u32, u32);

pub(super) trait CompositeValidationSessionId {
    fn seqno(&self) -> u32;
}

impl CompositeValidationSessionId for ValidationSessionId {
    fn seqno(&self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AddSession<'a> {
    pub shard_ident: ShardIdent,
    pub start_block_seqno: u32,
    pub session_id: ValidationSessionId,
    pub validators: &'a [ValidatorDescription],
}

#[derive(Debug, Clone)]
pub enum ValidationStatus {
    Skipped,
    Complete(ValidationComplete),
}

#[derive(Debug, Clone)]
pub struct ValidationComplete {
    pub signatures: BlockSignatures,
    pub total_weight: u64,
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
