use std::future::IntoFuture;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use anyhow::Result;
use arc_swap::ArcSwapOption;
use backon::BackoffBuilder;
use everscale_crypto::ed25519::KeyPair;
use everscale_types::models::*;
use futures_util::stream::FuturesUnordered;
use futures_util::{Future, StreamExt};
use scc::TreeIndex;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tycho_network::{OverlayId, PeerId, PrivateOverlay, Request};
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use tycho_util::FastHashMap;

use super::ValidatorStdImplConfig;
use crate::tracing_targets;
use crate::validator::rpc::{ExchangeSignatures, ValidatorClient, ValidatorService};
use crate::validator::{
    proto, AddSession, BriefValidatorDescr, ValidationStatus, ValidatorNetworkContext,
};

// Histograms
const METRIC_VALIDATE_BLOCK_TIME: &str = "tycho_validator_validate_block_time";
const METRIC_EXCHANGE_SIGNATURE_TIME: &str = "tycho_validator_exchange_signature_time";
const METRIC_RECEIVE_SIGNATURE_TIME: &str = "tycho_validator_receive_signature_time";

// Counters
const METRIC_BLOCK_EXCHANGES_IN_TOTAL: &str = "tycho_validator_block_exchanges_in_total";
const METRIC_CACHE_EXCHANGES_IN_TOTAL: &str = "tycho_validator_cache_exchanges_in_total";
const METRIC_MISS_EXCHANGES_IN_TOTAL: &str = "tycho_validator_miss_exchanges_in_total";
const METRIC_INVALID_SIGNATURES_IN_TOTAL: &str = "tycho_validator_invalid_signatures_in_total";
const METRIC_INVALID_SIGNATURES_CACHED_TOTAL: &str =
    "tycho_validator_invalid_signatures_cached_total";

// Gauges
const METRIC_SESSIONS_ACTIVE: &str = "tycho_validator_sessions_active";
const METRIC_BLOCK_SLOTS: &str = "tycho_validator_block_slots";
const METRIC_CACHE_SLOTS: &str = "tycho_validator_cache_slots";

/// Validator session is unique for each shard within each validator set.
#[derive(Clone)]
#[repr(transparent)]
pub struct ValidatorSession {
    inner: Arc<Inner>,
}

impl ValidatorSession {
    pub fn new(
        net_context: &ValidatorNetworkContext,
        key_pair: Arc<KeyPair>,
        config: &ValidatorStdImplConfig,
        info: AddSession<'_>,
    ) -> Result<Self> {
        // Prepare a map with other validators
        let mut validators = FastHashMap::default();
        for descr in info.validators {
            // TODO: Skip invalid entries? But what should we do with the total weight?
            let validator_info = BriefValidatorDescr::try_from(descr)?;
            validators.insert(validator_info.peer_id, validator_info);
        }

        let max_weight = validators.values().map(|v| v.weight).sum::<u64>();
        let weight_threshold = max_weight.saturating_mul(2) / 3 + 1;

        let peer_id = net_context.network.peer_id();
        let own_weight = match validators.remove(peer_id) {
            Some(info) => info.weight,
            None => anyhow::bail!("node is not in the validator set"),
        };

        // NOTE: At this point we are sure that our node is in the validator set

        let peer_ids = validators.values().map(|v| v.peer_id).collect::<Vec<_>>();

        // Create the session state
        let state = Arc::new(SessionState {
            shard_ident: info.shard_ident,
            weight_threshold,
            validators: Arc::new(validators),
            block_signatures: TreeIndex::new(),
            cached_signatures: TreeIndex::new(),
            cancelled: AtomicBool::new(false),
            cancelled_signal: Notify::new(),
        });

        // Create the private overlay
        let overlay_id = compute_session_overlay_id(
            &net_context.zerostate_id,
            &info.shard_ident,
            info.session_id,
        );

        let overlay = PrivateOverlay::builder(overlay_id)
            .named("validator")
            .with_peer_resolver(net_context.peer_resolver.clone())
            .with_entries(peer_ids)
            .build(ValidatorService {
                shard_ident: info.shard_ident,
                session_id: info.session_id,
                exchanger: Arc::downgrade(&state),
            });

        if !net_context.overlays.add_private_overlay(&overlay) {
            anyhow::bail!("private overlay already exists: {overlay_id:?}");
        }

        // Create the session
        let session = Self {
            inner: Arc::new(Inner {
                start_block_seqno: info.start_block_seqno,
                session_id: info.session_id,
                config: config.clone(),
                client: ValidatorClient::new(net_context.network.clone(), overlay),
                key_pair,
                own_weight,
                state,
                min_seqno: AtomicU32::new(info.start_block_seqno),
            }),
        };

        metrics::gauge!(METRIC_SESSIONS_ACTIVE).increment(1);

        // Prepare initial cache slots
        session.create_cache_slots(info.start_block_seqno, config.signature_cache_slots);

        // Done
        Ok(session)
    }

    pub fn start_block_seqno(&self) -> u32 {
        self.inner.start_block_seqno
    }

    pub fn cancel(&self) {
        self.inner.state.cancelled.store(true, Ordering::Release);
        self.inner.state.cancelled_signal.notify_waiters();
    }

    pub fn cancel_until(&self, block_seqno: u32) {
        self.inner
            .min_seqno
            .fetch_max(block_seqno, Ordering::Release);

        let state = self.inner.state.as_ref();
        state.cached_signatures.remove_range(..=block_seqno);

        let guard = scc::ebr::Guard::new();
        for (_, validation) in state.block_signatures.range(..=block_seqno, &guard) {
            validation.cancelled.cancel();
        }
        drop(guard);

        // NOTE: Remove only blocks that are old enough.
        let until_seqno = block_seqno.saturating_sub(self.inner.config.old_blocks_to_keep);
        state.block_signatures.remove_range(..=until_seqno);
    }

    #[tracing::instrument(
        skip_all,
        fields(session_id = self.inner.session_id, %block_id)
    )]
    pub async fn validate_block(&self, block_id: &BlockId) -> Result<ValidationStatus> {
        let _histogram = HistogramGuard::begin(METRIC_VALIDATE_BLOCK_TIME);
        tracing::debug!(target: tracing_targets::VALIDATOR, "started");

        debug_assert_eq!(self.inner.state.shard_ident, block_id.shard);

        self.inner
            .min_seqno
            .fetch_max(block_id.seqno, Ordering::Release);

        self.create_cache_slots(block_id.seqno + 1, self.inner.config.signature_cache_slots);

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
            anyhow::bail!(
                "block validation is already in progress. \
                session_id={}, block_id={block_id}",
                self.inner.session_id
            );
        }

        // NOTE: To eliminate the gap inside exchange routine, we can remove cached signatures
        // only after we have inserted the block.
        //
        // At this point the following is true:
        // - All new signatures will be stored (and validated) in the block;
        // - There might be some new signatures that were stored in the cache, but we
        //   have not yet processed them. We will use them later.
        state.cached_signatures.remove(&block_id.seqno);

        // Start collecting signatures from other validators
        let mut result = FastHashMap::default();
        result.insert(
            *self.inner.client.peer_id(),
            block_signatures.own_signature.clone(),
        );
        let mut total_weight = self.inner.own_weight;

        let semaphore = Arc::new(Semaphore::new(self.inner.config.max_parallel_requests));
        let mut futures = FuturesUnordered::new();
        for (peer_id, signature) in block_signatures.other_signatures.iter() {
            if let Some(valid_signature) = signature.load_full() {
                let validator_info = state
                    .validators
                    .get(peer_id)
                    .expect("peer info out of sync");

                result.insert(*peer_id, valid_signature);
                total_weight += validator_info.weight;
                continue;
            }

            let cached = cached
                .as_ref()
                .and_then(|c| c.other_signatures.get(peer_id))
                .and_then(|c| c.load_full());

            let fut = self.inner.clone().receive_signature(
                *peer_id,
                block_signatures.clone(),
                cached,
                semaphore.clone(),
            );
            futures.push(JoinTask::new(fut.instrument(tracing::Span::current())));
        }

        let mut session_cancelled = pin!(state.cancelled_signal.notified());
        if state.cancelled.load(Ordering::Acquire) {
            tracing::trace!(
                target: tracing_targets::VALIDATOR,
                block_seqno = block_id.seqno,
                "session cancelled",
            );
            return Ok(ValidationStatus::Skipped);
        }

        let mut block_cancelled = pin!(block_signatures.cancelled.cancelled());
        while total_weight < state.weight_threshold {
            let res = tokio::select! {
                res = futures.next() => match res {
                    Some(res) => res,
                    None => anyhow::bail!("no more signatures to collect but the threshold is not reached"),
                },
                _ = &mut session_cancelled => {
                    tracing::trace!(
                        target: tracing_targets::VALIDATOR,
                        block_seqno = block_id.seqno,
                        "session cancelled",
                    );
                    return Ok(ValidationStatus::Skipped)
                },
                _ = &mut block_cancelled => {
                    tracing::trace!(
                        target: tracing_targets::VALIDATOR,
                        block_seqno = block_id.seqno,
                        "block cancelled",
                    );
                    return Ok(ValidationStatus::Skipped)
                },
            };

            let validator_info = state
                .validators
                .get(&res.peer_id)
                .expect("peer info out of sync");

            let prev = result.insert(res.peer_id, res.signature);
            debug_assert!(prev.is_none(), "duplicate signature in result");
            total_weight += validator_info.weight;
        }

        tracing::debug!("finished");
        Ok(ValidationStatus::Complete(result))
    }

    fn prepare_new_signatures(&self, block_id: &BlockId) -> BlockSignaturesBuilder {
        let data = Block::build_data_for_sign(block_id);

        // Prepare our own signature
        let own_signature = Arc::new(self.inner.key_pair.sign_raw(&data));

        // Pre-allocate the result map which will contain all validators' signatures (only valid ones)
        let validators = self.inner.state.validators.as_ref();
        let mut other_signatures =
            SignatureSlotsMap::with_capacity_and_hasher(validators.len(), Default::default());

        for peer_id in validators.keys() {
            other_signatures.insert(*peer_id, Default::default());
        }

        BlockSignaturesBuilder {
            own_signature,
            other_signatures,
            total_weight: self.inner.own_weight,
        }
    }

    async fn reuse_signatures(
        &self,
        block_id: &BlockId,
        cached: Arc<CachedSignatures>,
    ) -> BlockSignaturesBuilder {
        let data = Block::build_data_for_sign(block_id);
        let block_seqno = block_id.seqno;

        let key_pair = self.inner.key_pair.clone();
        let validators = self.inner.state.validators.clone();
        let mut total_weight = self.inner.own_weight;

        let span = tracing::Span::current();
        tycho_util::sync::rayon_run(move || {
            let _span = span.enter();

            // Prepare our own signature
            let own_signature = Arc::new(key_pair.sign_raw(&data));

            // Pre-allocate the result map which will contain all validators' signatures (only valid ones)
            let mut other_signatures = SignatureSlotsMap::with_capacity_and_hasher(
                cached.other_signatures.len(),
                Default::default(),
            );

            for (peer_id, cached) in cached.other_signatures.iter() {
                let stored = 'stored: {
                    let Some(signature) = cached.load_full() else {
                        break 'stored Default::default();
                    };

                    let validator_info = validators.get(peer_id).expect("peer info out of sync");
                    if !validator_info.public_key.verify_raw(&data, &signature) {
                        tracing::warn!(
                            target: tracing_targets::VALIDATOR,
                            %peer_id,
                            block_seqno,
                            "cached signature is invalid on reuse: {}",
                            ValidationError::InvalidSignature
                        );

                        metrics::counter!(METRIC_INVALID_SIGNATURES_CACHED_TOTAL).increment(1);

                        // TODO: Somehow mark that this validator sent an invalid signature?
                        break 'stored Default::default();
                    }

                    total_weight += validator_info.weight;
                    Some(signature)
                };

                other_signatures.insert(*peer_id, SignatureSlot::new(stored));
            }

            BlockSignaturesBuilder {
                own_signature,
                other_signatures,
                total_weight,
            }
        })
        .await
    }

    fn create_cache_slots(&self, from: u32, n: u32) {
        let inner = self.inner.as_ref();

        let to = from + n;
        let from = inner.min_seqno.load(Ordering::Acquire).max(from);

        if from >= to {
            return;
        }

        for seqno in from..to {
            let signatures = CachedSignatures::new(&inner.state.validators);
            _ = inner.state.cached_signatures.insert(seqno, signatures);
        }
    }
}

struct Inner {
    start_block_seqno: u32,
    session_id: u32,
    config: ValidatorStdImplConfig,
    client: ValidatorClient,
    key_pair: Arc<KeyPair>,
    own_weight: u64,
    state: Arc<SessionState>,
    min_seqno: AtomicU32,
}

impl Inner {
    async fn receive_signature(
        self: Arc<Self>,
        peer_id: PeerId,
        block_signatures: Arc<BlockSignatures>,
        cached: Option<Arc<[u8; 64]>>,
        semaphore: Arc<Semaphore>,
    ) -> ValidSignature {
        let _histogram = HistogramGuard::begin(METRIC_RECEIVE_SIGNATURE_TIME);

        let block_seqno = block_signatures.block_id.seqno;
        let req = Request::from_tl(proto::rpc::ExchangeSignaturesRef {
            block_seqno,
            signature: block_signatures.own_signature.as_ref(),
        });

        let slot = block_signatures
            .other_signatures
            .get(&peer_id)
            .expect("peer info out of sync");

        let state = self.state.as_ref();

        // Try to use the cached signature first
        if let Some(signature) = cached {
            match state.add_signature(&block_signatures, slot, &peer_id, &signature) {
                Ok(()) => return ValidSignature { peer_id, signature },
                Err(ValidationError::SignatureChanged) => {
                    if let Some(signature) = slot.load_full() {
                        return ValidSignature { peer_id, signature };
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: tracing_targets::VALIDATOR,
                        %peer_id,
                        "cached signature is invalid on add_signature: {e:?}",
                    );
                }
            }
        }

        let backoff = &self.config.exchange_signatures_backoff;
        let signature = loop {
            let mut retry = backon::ExponentialBuilder::default()
                .with_min_delay(backoff.min_interval)
                .with_max_delay(backoff.max_interval)
                .with_factor(backoff.factor)
                .with_max_times(usize::MAX)
                .build();

            let signature_fut = slot.into_future();
            let recv_fut = async {
                loop {
                    let permit = semaphore.acquire().await.unwrap();

                    let timeout = self.config.exchange_signatures_timeout;
                    let query = {
                        let _histogram = HistogramGuard::begin(METRIC_EXCHANGE_SIGNATURE_TIME);
                        self.client.query(&peer_id, req.clone())
                    };

                    match tokio::time::timeout(timeout, query).await {
                        Ok(Ok(res)) => match res.parse_tl() {
                            Ok(proto::Exchange::Complete(signature)) => break signature,
                            Ok(proto::Exchange::Cached) => {
                                tracing::trace!(
                                    target: tracing_targets::VALIDATOR,
                                    "partial signature exchange",
                                );
                            }
                            Err(e) => {
                                tracing::trace!(
                                    target: tracing_targets::VALIDATOR,
                                    "failed to parse response: {e:?}",
                                );
                            }
                        },
                        Ok(Err(e)) => tracing::trace!(
                            target: tracing_targets::VALIDATOR,
                            "query failed: {e:?}",
                        ),
                        Err(_) => tracing::trace!(
                            target: tracing_targets::VALIDATOR,
                            "query timed out",
                        ),
                    }
                    drop(permit);

                    let delay = retry.next().unwrap();
                    tokio::time::sleep(delay).await;
                }
            };

            // TODO: Check if there is any contention on the `signature_fut`
            let signature = tokio::select! {
                signature = signature_fut => signature,
                signature = recv_fut => signature,
            };

            match state.add_signature(&block_signatures, slot, &peer_id, &signature) {
                Ok(()) => break signature,
                Err(ValidationError::SignatureChanged) => {
                    if let Some(signature) = slot.load_full() {
                        break signature;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: tracing_targets::VALIDATOR,
                        "fetched signature is invalid: {e:?}",
                    );
                }
            }

            // NOTE: Outer interval is used for retries in case of invalid signatures.
            // It will keep this future alive until we either receive enough signatures
            // from other validators or this one finally return a valid signature.
            tokio::time::sleep(self.config.failed_exchange_interval).await;
        };

        ValidSignature { peer_id, signature }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        metrics::gauge!(METRIC_SESSIONS_ACTIVE).decrement(1);

        tracing::debug!(
            target: tracing_targets::VALIDATOR,
            shard_ident = %self.state.shard_ident,
            session_id = self.session_id,
            "validator session dropped"
        );
    }
}

struct SessionState {
    shard_ident: ShardIdent,
    weight_threshold: u64,
    validators: Arc<FastHashMap<PeerId, BriefValidatorDescr>>,
    block_signatures: TreeIndex<u32, Arc<BlockSignatures>>,
    cached_signatures: TreeIndex<u32, Arc<CachedSignatures>>,
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

        let data = Block::build_data_for_sign(&block.block_id);

        // TODO: Store expanded public key with the signature?
        let validator_info = self.validators.get(peer_id).expect("peer info out of sync");
        if !validator_info
            .public_key
            .verify_raw(&data, signature.as_ref())
        {
            // TODO: Store that the signature is invalid to avoid further checks on retries
            // TODO: Collect statistics on invalid signatures to slash the malicious validator
            metrics::counter!(METRIC_INVALID_SIGNATURES_IN_TOTAL).increment(1);
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

        if can_notify {
            block.validated.store(true, Ordering::Release);
        }

        Ok(())
    }
}

struct ValidSignature {
    peer_id: PeerId,
    signature: Arc<[u8; 64]>,
}

struct BlockSignaturesBuilder {
    own_signature: Arc<[u8; 64]>,
    other_signatures: SignatureSlotsMap,
    total_weight: u64,
}

impl BlockSignaturesBuilder {
    fn build(self, block_id: &BlockId, weight_threshold: u64) -> Arc<BlockSignatures> {
        metrics::gauge!(METRIC_BLOCK_SLOTS).increment(1);

        Arc::new(BlockSignatures {
            block_id: *block_id,
            own_signature: self.own_signature,
            other_signatures: self.other_signatures,
            total_weight: AtomicU64::new(self.total_weight),
            validated: AtomicBool::new(self.total_weight >= weight_threshold),
            cancelled: CancellationToken::new(),
        })
    }
}

struct BlockSignatures {
    block_id: BlockId,
    own_signature: Arc<[u8; 64]>,
    other_signatures: SignatureSlotsMap,
    total_weight: AtomicU64,
    validated: AtomicBool,
    cancelled: CancellationToken,
}

impl Drop for BlockSignatures {
    fn drop(&mut self) {
        metrics::gauge!(METRIC_BLOCK_SLOTS).decrement(1);
    }
}

struct CachedSignatures {
    other_signatures: FastHashMap<PeerId, ArcSwapOption<[u8; 64]>>,
}

impl CachedSignatures {
    fn new(validators: &FastHashMap<PeerId, BriefValidatorDescr>) -> Arc<Self> {
        let signatures = validators
            .keys()
            .map(|peer_id| (*peer_id, Default::default()))
            .collect();

        metrics::gauge!(METRIC_CACHE_SLOTS).increment(1);

        Arc::new(Self {
            other_signatures: signatures,
        })
    }
}

impl Drop for CachedSignatures {
    fn drop(&mut self) {
        metrics::gauge!(METRIC_CACHE_SLOTS).decrement(1);
    }
}

type SignatureSlotsMap = FastHashMap<PeerId, SignatureSlot>;

impl ExchangeSignatures for SessionState {
    type Err = ValidationError;

    async fn exchange_signatures(
        &self,
        peer_id: &PeerId,
        block_seqno: u32,
        signature: Arc<[u8; 64]>,
    ) -> Result<proto::Exchange, Self::Err> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(ValidationError::Cancelled);
        }

        let guard = scc::ebr::Guard::new();

        // Full signature exchange if we know the block.
        // Otherwise, cache the signature for the block to use it later.
        //
        // NOTE: scc's `peek` does not lock the tree
        let result = if let Some(signatures) = self.block_signatures.peek(&block_seqno, &guard) {
            metrics::counter!(METRIC_BLOCK_EXCHANGES_IN_TOTAL).increment(1);

            let Some(slot) = signatures.other_signatures.get(peer_id) else {
                return Err(ValidationError::UnknownPeer);
            };

            // If more signatures are still needed, validate and store new to the block
            if !signatures.validated.load(Ordering::Acquire) {
                self.add_signature(signatures, slot, peer_id, &signature)?;
            }

            proto::Exchange::Complete(signatures.own_signature.clone())
        } else {
            // Find the slot for the specified block seqno.
            let Some(slot) = self.cached_signatures.peek(&block_seqno, &guard) else {
                metrics::counter!(METRIC_MISS_EXCHANGES_IN_TOTAL).increment(1);
                return Err(ValidationError::NoSlot);
            };
            metrics::counter!(METRIC_CACHE_EXCHANGES_IN_TOTAL).increment(1);

            let Some(saved_signature) = slot.other_signatures.get(peer_id) else {
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

#[derive(Default)]
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
