use std::future::IntoFuture;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use anyhow::Result;
use arc_swap::ArcSwapOption;
use backon::BackoffBuilder;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::*;
use futures_util::stream::FuturesUnordered;
use futures_util::Future;
use scc::TreeIndex;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tycho_network::{OverlayId, PeerId, PrivateOverlay, Request};
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
        key_pair: Arc<KeyPair>,
        backoff: &ExchangeSignaturesBackoff,
        net_context: &NetworkContext,
    ) -> Result<Self> {
        let max_weight = validators.values().map(|v| v.weight).sum::<u64>();
        let weight_threshold = max_weight.saturating_mul(2) / 3 + 1;

        let peer_id = net_context.network.peer_id();
        let own_weight = match validators.remove(peer_id) {
            Some(info) => info.weight,
            None => anyhow::bail!("our node is not in the validator set"),
        };

        // NOTE: At this point we are sure that our node is in the validator set

        let state = Arc::new(SessionState {
            peer_id: *net_context.network.peer_id(),
            shard_ident: *shard_ident,
            session_id,
            weight_threshold,
            validators: Arc::new(validators),
            block_signatures: TreeIndex::new(),
            cached_signatures: TreeIndex::new(),
            cancelled: AtomicBool::new(false),
            cancelled_signal: Notify::new(),
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

        Ok(Self {
            inner: Arc::new(Inner {
                client: ValidatorClient::new(net_context.network.clone(), overlay),
                backoff: backoff.clone(),
                key_pair,
                own_weight,
                state,
            }),
        })
    }

    pub fn shard_ident(&self) -> &ShardIdent {
        &self.inner.state.shard_ident
    }

    pub fn session_id(&self) -> u32 {
        self.inner.state.session_id
    }

    pub async fn cancel_before(&self, block_seqno: u32) {
        let state = self.inner.state.as_ref();
        state.cached_signatures.remove_range(..block_seqno);
        state.block_signatures.remove_range(..block_seqno);
    }

    pub async fn validate_block(&self, block_id: &BlockId) -> Result<()> {
        const MAX_PARALLEL_REQUESTS: usize = 10;

        debug_assert_eq!(self.inner.state.shard_ident, block_id.shard);

        let state = &self.inner.state;

        // Remove cached slot
        let cached = state
            .cached_signatures
            .peek(&block_id.seqno, &scc::ebr::Guard::new())
            .map(Arc::clone);

        // Prepare block signatures
        let block_signatures = match &cached {
            Some(cached) => self.reuse_signatures(block_id, cached.clone()).await,
            None => self.prepare_new_signatures(block_id),
        }
        .build(block_id, state.weight_threshold);

        // Allow only one validation at a time
        if state
            .block_signatures
            .insert(block_id.seqno, block_signatures.clone())
            .is_err()
        {
            // TODO: Panic here?
            anyhow::bail!("block validation is already in progress");
        }

        // NOTE: To eliminate the gap inside exchange routine, we can remove cached signatures
        // only after we have inserted the block.
        //
        // At this point the following is true:
        // - All new signatures will be stored (and validated) in the block;
        // - There might be some new signatures that were stored in the cache, but we
        //   have not yet processed them. We will use them later.
        state.cached_signatures.remove(&block_id.seqno);

        let mut session_cancelled = pin!(state.cancelled_signal.notified());
        let mut validation_cancelled = pin!(block_signatures.validation.cancelled());

        let mut result = FastHashMap::default();

        let semaphore = Arc::new(Semaphore::new(MAX_PARALLEL_REQUESTS));
        let mut futures = FuturesUnordered::new();
        for (peer_id, signature) in block_signatures.other_signatures.iter() {
            if let Some(valid_signature) = signature.load_full() {
                result.insert(*peer_id, valid_signature);
                continue;
            }

            // if let Some(cached) = cached.as_ref().and_then(|c| c.get(peer_id)) {
            //     if let Some(signature) = cached.load_full() {
            //         // TODO: Validate and store the signature
            //         continue;
            //     }
            // }

            let peer_id = *peer_id;
            let client = self.inner.client.clone();
            let other_signatures = block_signatures.other_signatures.clone();
            let semaphore = semaphore.clone();
            let req = req.clone();
            futures.push(async move {
                let slot = other_signatures.get(&peer_id).expect("peer id out of sync");

                let fetch = async {
                    loop {
                        let _permit = semaphore.acquire().await.unwrap();

                        let result = client.query_with_retry(&peer_id, req).await;
                    }
                };

                tokio::select! {
                    signature = slot.into_future() => signature,
                    segnature = fetch => signature,
                }
            });
        }

        let mut stopped = false;
        while !stopped {
            tokio::select! {
                _ = &mut session_cancelled => {
                    stopped = true;
                }
                _ = &mut validation_cancelled => {
                    stopped = true;
                }
            }
        }

        Ok(())
    }

    fn prepare_new_signatures(&self, block_id: &BlockId) -> BlockSignaturesBuilder {
        let data = Block::build_data_for_sign(block_id);

        // Prepare our own signature
        let own_signature = Arc::new(self.inner.key_pair.sign_raw(&data));

        // Pre-allocate the result map which will contain all validators' signatures (only valid ones)
        let validators = self.inner.state.validators.as_ref();
        let mut result =
            SignaturesMap::with_capacity_and_hasher(validators.len(), Default::default());

        for peer_id in validators.keys() {
            result.insert(*peer_id, Default::default());
        }

        BlockSignaturesBuilder {
            own_signature,
            other_signatures: Arc::new(result),
            total_weight: self.inner.own_weight,
        }
    }

    async fn reuse_signatures(
        &self,
        block_id: &BlockId,
        cached: Arc<SignaturesMap>,
    ) -> BlockSignaturesBuilder {
        let data = Block::build_data_for_sign(block_id);

        let key_pair = self.inner.key_pair.clone();
        let validators = self.inner.state.validators.clone();
        let mut total_weight = self.inner.own_weight;

        tycho_util::sync::rayon_run(move || {
            // Prepare our own signature
            let own_signature = Arc::new(key_pair.sign_raw(&data));

            // Pre-allocate the result map which will contain all validators' signatures (only valid ones)
            let mut result =
                SignaturesMap::with_capacity_and_hasher(cached.len(), Default::default());

            for (peer_id, cached) in cached.iter() {
                let stored = 'stored: {
                    let Some(signature) = cached.load_full() else {
                        break 'stored Default::default();
                    };

                    let validator_info = validators.get(peer_id).expect("peer info out of sync");
                    if !validator_info.public_key.verify_raw(&data, &signature) {
                        // TODO: Somehow mark that this validator sent an invalid signature?
                        break 'stored Default::default();
                    }

                    total_weight += validator_info.weight;
                    Some(signature)
                };

                result.insert(*peer_id, ArcSwapOption::new(stored));
            }

            BlockSignaturesBuilder {
                own_signature,
                other_signatures: Arc::new(result),
                total_weight,
            }
        })
        .await
    }
}

struct Inner {
    client: ValidatorClient,
    backoff: ExchangeSignaturesBackoff,
    key_pair: Arc<KeyPair>,
    own_weight: u64,
    state: Arc<SessionState>,
}

impl Inner {
    async fn receive_signature(
        self: Arc<Self>,
        peer_id: PeerId,
        block_signatures: Arc<BlockSignatures>,
    ) -> Result<Arc<[u8; 64]>, ValidationError> {
        let block_seqno = block_signatures.block_id.seqno;
        let req = Request::from_tl(proto::rpc::ExchangeSignaturesRef {
            block_seqno,
            signature: block_signatures.own_signature.as_ref(),
        });

        let slot = block_signatures
            .other_signatures
            .get(&peer_id)
            .expect("peer id out of sync");

        let mut retry = backon::ExponentialBuilder::default()
            .with_min_delay(self.backoff.min_interval)
            .with_max_delay(self.backoff.max_interval)
            .with_factor(self.backoff.factor)
            .build();

        let signature = loop {
            match self.client.query(&peer_id, req.clone()).await {
                Ok(res) => match res.parse_tl() {
                    Ok(proto::Exchange::Complete(signature)) => break signature,
                    Ok(proto::Exchange::Cached) => {}
                    Err(e) => {
                        tracing::trace!(%peer_id, block_seqno, "failed to parse response: {e:?}");
                    }
                },
                Err(e) => tracing::trace!(%peer_id, block_seqno, "query failed: {e:?}"),
            }

            let delay = retry.next().unwrap();
            tokio::time::sleep(delay).await;
        };

        self.state
            .add_signature(&block_signatures, slot, &peer_id, &signature)?;

        Ok(signature)
    }
}

struct SessionState {
    peer_id: PeerId,
    shard_ident: ShardIdent,
    session_id: u32,
    weight_threshold: u64,
    validators: Arc<FastHashMap<PeerId, BriefValidatorDescr>>,
    block_signatures: TreeIndex<u32, Arc<BlockSignatures>>,
    cached_signatures: TreeIndex<u32, Arc<SignaturesMap>>,
    cancelled: AtomicBool,
    cancelled_signal: Notify,
}

impl SessionState {
    fn add_signature(
        &self,
        block: &BlockSignatures,
        slot: &SignatureSlot,
        peer_id: &PeerId,
        signature: &Arc<[u8; 64]>,
    ) -> Result<(), ValidationError> {
        match &*slot.load() {
            Some(saved) if saved.as_ref() == signature.as_ref() => return Ok(()),
            Some(_) => return Err(ValidationError::SignatureChanged),
            // NOTE: Do nothing here to drop the `arc_swap::Guard` before the signature check
            None => {}
        }

        // TODO: Skip if `validated` is already set?
        //       The reason against it is that we might slash some validators and
        //       we would prefer to have some fallback signatures in this case.

        let data = Block::build_data_for_sign(&block.block_id);

        // TODO: Store expanded public key with the signature?
        let validator_info = self.validators.get(peer_id).expect("peer info out of sync");
        if !validator_info
            .public_key
            .verify_raw(&data, signature.as_ref())
        {
            // TODO: Store that the signature is invalid to avoid further checks on retries
            // TODO: Collect statistics on invalid signatures to slash the malicious validator
            return Err(ValidationError::InvalidSignature);
        }

        let mut can_notify = false;
        match &*slot.compare_and_swap(&None::<Arc<[u8; 64]>>, Some(signature.clone())) {
            None => {
                slot.notify();

                // Update the total weight of the block if the signature is new
                let total_weight = block
                    .total_weight
                    .fetch_add(validator_info.weight, Ordering::Relaxed)
                    + validator_info.weight;

                can_notify = total_weight >= self.weight_threshold;
            }
            Some(saved) => {
                if saved.as_ref() != signature.as_ref() {
                    // NOTE: Ensure that other query from the same peer does
                    // not have a different signature
                    return Err(ValidationError::SignatureChanged);
                }
            }
        }

        if can_notify && !block.validated.swap(true, Ordering::Relaxed) {
            block.validation.cancel();
        }

        Ok(())
    }
}

struct BlockSignaturesBuilder {
    own_signature: Arc<[u8; 64]>,
    other_signatures: Arc<SignaturesMap>,
    total_weight: u64,
}

impl BlockSignaturesBuilder {
    fn build(self, block_id: &BlockId, weight_threshold: u64) -> Arc<BlockSignatures> {
        Arc::new(BlockSignatures {
            block_id: *block_id,
            own_signature: self.own_signature,
            other_signatures: self.other_signatures,
            total_weight: AtomicU64::new(self.total_weight),
            validated: AtomicBool::new(self.total_weight >= weight_threshold),
            validation: CancellationToken::new(),
        })
    }
}

struct BlockSignatures {
    block_id: BlockId,
    own_signature: Arc<[u8; 64]>,
    other_signatures: Arc<SignatureSlotsMap>,
    total_weight: AtomicU64,
    validated: AtomicBool,
    validation: CancellationToken,
}

impl Drop for BlockSignatures {
    fn drop(&mut self) {
        self.validation.cancel();
    }
}

type SignatureSlotsMap = FastHashMap<PeerId, SignatureSlot>;
type SignaturesMap = FastHashMap<PeerId, ArcSwapOption<[u8; 64]>>;

impl ExchangeSignatures for SessionState {
    type Err = ValidationError;

    async fn exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: Arc<[u8; 64]>,
    ) -> Result<proto::Exchange, Self::Err> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(ValidationError::Cancelled);
        }

        let guard = scc::ebr::Guard::new();

        // NOTE: scc's `peek` does not lock the tree
        let result = if let Some(signatures) = self.block_signatures.peek(&block_seqno, &guard) {
            // Full signature exchange if we know the block
            let Some(slot) = signatures.other_signatures.get(peer_id) else {
                return Err(ValidationError::UnknownPeer);
            };

            self.add_signature(&signatures, slot, peer_id, &signature)?;

            proto::Exchange::Complete(signatures.own_signature.clone())
        } else {
            // Cache the signature for the block that we don't know yet

            // Find the slot for the specified block seqno.
            let Some(signatures) = self.cached_signatures.peek(&block_seqno, &guard) else {
                return Err(ValidationError::NoSlot);
            };

            let Some(saved_signature) = signatures.get(peer_id) else {
                return Err(ValidationError::UnknownPeer);
            };

            // Store the signature in the cache (allow to overwrite the signature)
            saved_signature.store(Some(signature.clone()));

            proto::Exchange::Cached
        };

        drop(guard);

        let action = match &result {
            proto::Exchange::Complete(_) => "complete",
            proto::Exchange::Cached => "cached",
        };

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

struct SignatureSlot {
    value: ArcSwapOption<[u8; 64]>,
    waker: parking_lot::Mutex<Option<Waker>>,
}

impl SignatureSlot {
    fn new(value: Option<Arc<[u8; 64]>>) -> Self {
        Self {
            value: ArcSwapOption::new(value),
            waker: parking_lot::Mutex::new(None),
        }
    }

    fn notify(&self) {
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }
}

impl std::ops::Deref for SignatureSlot {
    type Target = ArcSwapOption<[u8; 64]>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a> IntoFuture for &'a SignatureSlot {
    type Output = Arc<[u8; 64]>;
    type IntoFuture = SignatureFuture<'a>;

    #[inline]
    fn into_future(self) -> Self::IntoFuture {
        SignatureFuture(self)
    }
}

struct SignatureFuture<'a>(&'a SignatureSlot);

impl Future for SignatureFuture<'_> {
    type Output = Arc<[u8; 64]>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Fast path
        if let Some(value) = self.0.value.load_full() {
            return Poll::Ready(value);
        }

        let mut waker = self.0.waker.lock();

        // Double check while holding the lock
        if let Some(value) = self.0.value.load_full() {
            return Poll::Ready(value);
        }

        match &mut *waker {
            Some(waker) => {
                waker.clone_from(cx.waker());
            }
            _ => *waker = Some(cx.waker().clone()),
        }

        Poll::Pending
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
