use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use everscale_types::models::*;
use scc::TreeIndex;
use tycho_network::{OverlayId, PeerId, PrivateOverlay};
use tycho_util::FastHashMap;

use crate::validator::rpc::{
    ExchangeSignatures, ExchangeSignaturesBackoff, ValidatorClient, ValidatorService,
};
use crate::validator::{proto, NetworkContext, ValidatorInfo};

/// Validator session is unique for each shard whithin each validator set.
#[derive(Clone)]
#[repr(transparent)]
pub struct ValidatorSession {
    inner: Arc<Inner>,
}

impl ValidatorSession {
    pub fn new(
        shard_ident: ShardIdent,
        session_id: u32,
        mut validators: FastHashMap<PeerId, ValidatorInfo>,
        backoff: &ExchangeSignaturesBackoff,
        net_context: &NetworkContext,
    ) -> Result<Self> {
        let max_weight = validators.values().map(|v| v.weight).sum();

        let state = Arc::new(SessionState {
            peer_id: *net_context.network.peer_id(),
            shard_ident,
            session_id,
            max_weight,
            validators,
            signatures: tokio::sync::RwLock::new(Signatures::default()),
        });

        let overlay_id =
            compute_session_overlay_id(&net_context.zerostate_id, &shard_ident, session_id);

        let overlay = PrivateOverlay::builder(overlay_id)
            .named("validator")
            .with_peer_resolver(net_context.peer_resolver.clone())
            .with_entries(validators.values().map(|v| v.peer_id))
            .build(ValidatorService {
                shard_ident,
                session_id,
                exchanger: state.clone(),
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
}

struct Inner {
    client: ValidatorClient,
    state: Arc<SessionState>,
}

struct SessionState {
    peer_id: PeerId,
    shard_ident: ShardIdent,
    session_id: u32,
    max_weight: u64,
    validators: FastHashMap<PeerId, ValidatorInfo>,
    signatures: tokio::sync::RwLock<Signatures>,
}

#[derive(Default)]
struct Signatures {
    blocks: TreeIndex<u32, Arc<BlockSignatures>>,
    cached: TreeIndex<u32, Arc<SignaturesMap>>,
}

struct BlockSignatures {
    block_id: BlockId,
    own_signature: Arc<[u8; 64]>,
    other_signatures: Arc<SignaturesMap>,
}

type SignaturesMap = FastHashMap<PeerId, ArcSwapOption<[u8; 64]>>;

impl ExchangeSignatures for Arc<SessionState> {
    type IntoIter = Vec<proto::PeerSignatureOwned>;

    type Err = ValidationError;

    async fn exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: Arc<[u8; 64]>,
    ) -> Result<Self::IntoIter, Self::Err> {
        let signatures = self.signatures.read().await;

        let guard = scc::ebr::Guard::new();

        // NOTE: scc's `peek` does not lock the tree
        if let Some(signatures) = signatures.blocks.peek(&block_seqno, &guard) {
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

                if matches!(
                    &*saved.compare_and_swap(None, Some(signature.clone())),
                    Some(saved) if saved != signature
                ) {
                    // NOTE: Ensure that other query from the same peer does
                    // not have a different signature
                    return Err(ValidationError::SignatureChanged);
                }

                // NOTE: At this point the signature is guaranteed to be valid
                // and is equal to the one from request.
            }

            // TODO: Select random subset of signatures to return
            let mut result = Vec::with_capacity(1 + signatures.other_signatures.len());
            result.push(proto::PeerSignatureOwned {
                peer_id: self.peer_id,
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
            Ok(result)
        } else {
            // Cache the signature for the block that we don't know yet

            // Find the slot for the specified block seqno.
            let Some::<Arc<SignaturesMap>>(signatures) = signatures
                .cached
                .peek(&block_seqno, &scc::ebr::Guard::new())
                .and_then(Arc::clone)
            else {
                return Err(ValidationError::NoSlot);
            };

            let Some(saved_signature) = signatures.get(peer_id) else {
                return Err(ValidationError::UnknownPeer);
            };

            // Store the signature in the cache (allow to overwrite the signature)
            saved_signature.store(Some(signature.clone()));

            // Do not return any signatures since we can't verify them yet
            Ok(Vec::new())
        }
    }
}

fn compute_session_overlay_id(
    zerostate_id: &BlockId,
    shard_ident: &ShardIdent,
    session_id: u32,
) -> OverlayId {
    OverlayId(tl_proto::hash(proto::OverlayIdData {
        zerostate_root_hash: zerostate_id.root_hash,
        zerostate_file_hash: zerostate_id.file_hash,
        shard_ident: *shard_ident,
        session_id,
    }))
}

#[derive(Debug, thiserror::Error)]
enum ValidationError {
    #[error("no slot available for the specified seqno")]
    NoSlot,
    #[error("peer is not a known validator")]
    UnknownPeer,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("signature has changed since the last exchange")]
    SignatureChanged,
}
