use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use anyhow::Result;
use arc_swap::ArcSwapOption;
use everscale_types::models::*;
use scc::TreeIndex;
use tokio_util::sync::CancellationToken;
use tycho_network::{OverlayId, PeerId, PrivateOverlay};
use tycho_util::FastHashMap;

use crate::tracing_targets;
use crate::validator::rpc::{
    ExchangeSignatures, ExchangeSignaturesBackoff, ValidatorClient, ValidatorService,
};
use crate::validator::{proto, BriefValidatorDescr, NetworkContext};

/// Validator session is unique for each shard whithin each validator set.
#[derive(Clone)]
#[repr(transparent)]
pub struct ValidatorSession {
    inner: Arc<Inner>,
}

impl ValidatorSession {
    pub fn new(
        shard_ident: &ShardIdent,
        session_id: u32,
        mut validators: FastHashMap<PeerId, BriefValidatorDescr>,
        backoff: &ExchangeSignaturesBackoff,
        net_context: &NetworkContext,
    ) -> Result<Self> {
        let max_weight = validators.values().map(|v| v.weight).sum::<u64>();
        let weight_threshold = max_weight.saturating_mul(2) / 3 + 1;

        let peer_id = net_context.network.peer_id();
        if validators.remove(peer_id).is_none() {
            anyhow::bail!("our node is not in the validator set");
        }

        // NOTE: At this point we are sure that our node is in the validator set

        let state = Arc::new(SessionState {
            peer_id: *net_context.network.peer_id(),
            shard_ident: *shard_ident,
            session_id,
            weight_threshold,
            validators,
            signatures: tokio::sync::RwLock::new(Signatures::default()),
        });

        let overlay_id =
            compute_session_overlay_id(&net_context.zerostate_id, shard_ident, session_id);

        let overlay = PrivateOverlay::builder(overlay_id)
            .named("validator")
            .with_peer_resolver(net_context.peer_resolver.clone())
            .with_entries(validators.values().map(|v| v.peer_id))
            .build(ValidatorService {
                shard_ident: *shard_ident,
                session_id,
                exchanger: Arc::downgrade(&state),
            });

        if !net_context.overlays.add_private_overlay(&overlay) {
            anyhow::bail!("private overlay already exists: {overlay_id:?}");
        }

        let client = ValidatorClient::builder()
            .with_network(net_context.network.clone())
            .with_overlay(overlay)
            .with_backoff(backoff.clone())
            .build();

        Ok(Self {
            inner: Arc::new(Inner { client, state }),
        })
    }

    pub fn shard_ident(&self) -> &ShardIdent {
        &self.inner.state.shard_ident
    }

    pub fn session_id(&self) -> u32 {
        self.inner.state.session_id
    }

    pub async fn cancel_before(&self, block_seqno: u32) {
        let mut signatures = self.inner.state.signatures.write().await;

        // Remove all cached signatures before the specified seqno
        // NOTE: `split_off` returns the part of the map after the specified key (inclusive)
        // so we need to swap it back and drop the part before the key.
        let mut cached = signatures.cached.split_off(&block_seqno);
        std::mem::swap(&mut cached, &mut signatures.cached);

        // Remove all blocks before the specified seqno
        let mut blocks = signatures.blocks.split_off(&block_seqno);
        std::mem::swap(&mut blocks, &mut signatures.blocks);

        // NOTE: Release the lock before dropping the maps
        drop(signatures);
    }
}

struct Inner {
    client: ValidatorClient,
    state: Arc<SessionState>,
}

struct SessionState {
    peer_id: PeerId,
    shard_ident: ShardIdent,
    session_id: u32,
    weight_threshold: u64,
    validators: FastHashMap<PeerId, BriefValidatorDescr>,
    signatures: tokio::sync::RwLock<Signatures>,
    disabled: AtomicBool,
}

#[derive(Default)]
struct Signatures {
    blocks: BTreeMap<u32, Arc<BlockSignatures>>,
    cached: BTreeMap<u32, Arc<SignaturesMap>>,
}

struct BlockSignatures {
    block_id: BlockId,
    own_signature: Arc<[u8; 64]>,
    other_signatures: Arc<SignaturesMap>,
    total_weight: AtomicU64,
    validated: AtomicBool,
    cancellation_token: CancellationToken,
}

impl Drop for BlockSignatures {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

type SignaturesMap = FastHashMap<PeerId, ArcSwapOption<[u8; 64]>>;

impl ExchangeSignatures for SessionState {
    type IntoIter = Vec<proto::PeerSignatureOwned>;

    type Err = ValidationError;

    async fn exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: Arc<[u8; 64]>,
    ) -> Result<Self::IntoIter, Self::Err> {
        if self.disabled.load(Ordering::Relaxed) {
            return Err(ValidationError::Cancelled);
        }

        let signatures = self.signatures.read().await;

        // NOTE: scc's `peek` does not lock the tree
        let mut result;
        let action;
        if let Some(signatures) = signatures.blocks.get(&block_seqno) {
            // Full signature exchange if we know the block
            let Some(saved) = signatures.other_signatures.get(peer_id) else {
                return Err(ValidationError::UnknownPeer);
            };

            'check: {
                match &*saved.load() {
                    Some(saved) if saved == signature => break 'check,
                    Some(_) => return Err(ValidationError::SignatureChanged),
                    // NOTE: Do nothing here to drop the `arc_swap::Guard` before the signature check
                    None => {}
                }

                // TODO: Skip if `validated` is already set?
                //       The reason against it is that we might slash some validators and
                //       we would prefer to have some fallback signatures in this case.

                let data = Block::build_data_for_sign(&signatures.block_id);

                // TODO: Store expanded public key with the signature?
                let validator_info = self.validators.get(peer_id).expect("peer info out of sync");
                if !validator_info
                    .public_key
                    .verify_raw(&data, signature.as_ref())
                {
                    // TODO: Store that the signature is invalid to avoid further checks on retries
                    return Err(ValidationError::InvalidSignature);
                }

                let mut can_notify = false;
                match &*saved.compare_and_swap(None, Some(signature.clone())) {
                    None => {
                        // Update the total weight of the block if the signature is new
                        let total_weight = signatures
                            .total_weight
                            .fetch_add(validator_info.weight, Ordering::Relaxed)
                            + validator_info.weight;

                        can_notify = total_weight >= self.weight_threshold;
                    }
                    Some(saved) => {
                        if saved != signature {
                            // NOTE: Ensure that other query from the same peer does
                            // not have a different signature
                            return Err(ValidationError::SignatureChanged);
                        }
                    }
                }

                if can_notify && !signatures.validated.swap(true, Ordering::Relaxed) {
                    // TODO: Notify the client that the block is validated
                }

                // NOTE: At this point the signature is guaranteed to be valid
                // and is equal to the one from request.
            }

            // TODO: Select random subset of signatures to return
            result = Vec::with_capacity(1 + signatures.other_signatures.len());
            result.push(proto::PeerSignatureOwned {
                peer_id: self.peer_id.0,
                signature: signatures.own_signature.clone(),
            });
            for (peer_id, stored) in &signatures.other_signatures {
                if let Some(signature) = stored.load_full() {
                    result.push(proto::PeerSignatureOwned {
                        peer_id: *peer_id,
                        signature,
                    });
                }
            }

            action = "store_for_block";
        } else {
            // Cache the signature for the block that we don't know yet

            // Find the slot for the specified block seqno.
            let Some::<Arc<SignaturesMap>>(signatures) = signatures.cached.get(&block_seqno) else {
                return Err(ValidationError::NoSlot);
            };

            let Some(saved_signature) = signatures.get(peer_id) else {
                return Err(ValidationError::UnknownPeer);
            };

            // Store the signature in the cache (allow to overwrite the signature)
            saved_signature.store(Some(signature.clone()));

            // Do not return any signatures since we can't verify them yet
            result = Vec::new();

            action = "cache_only";
        }

        drop(signatures);

        tracing::trace!(
            target: tracing_targets::VALIDATOR,
            %peer_id,
            block_seqno,
            action,
            "exchanged signatures"
        );

        Ok(result)
    }
}

fn compute_session_overlay_id(
    zerostate_id: &BlockId,
    shard_ident: &ShardIdent,
    session_id: u32,
) -> OverlayId {
    OverlayId(tl_proto::hash(proto::OverlayIdData {
        zerostate_root_hash: zerostate_id.root_hash.0,
        zerostate_file_hash: zerostate_id.file_hash.0,
        shard_ident: *shard_ident,
        session_id,
    }))
}

#[derive(Debug, thiserror::Error)]
enum ValidationError {
    #[error("session is cancelled")]
    Cancelled,
    #[error("no slot available for the specified seqno")]
    NoSlot,
    #[error("peer is not a known validator")]
    UnknownPeer,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("signature has changed since the last exchange")]
    SignatureChanged,
}
