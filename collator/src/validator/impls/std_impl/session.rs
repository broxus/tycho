use std::fmt;
use std::future::IntoFuture;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use anyhow::Result;
use arc_swap::ArcSwapOption;
use backon::BackoffBuilder;
use futures_util::stream::FuturesUnordered;
use futures_util::{Future, StreamExt};
use scc::TreeIndex;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tycho_crypto::ed25519::KeyPair;
use tycho_network::{OverlayId, PeerId, PrivateOverlay, Request};
use tycho_types::models::*;
use tycho_util::FastHashMap;
use tycho_util::futures::JoinTask;
use tycho_util::metrics::HistogramGuard;
use super::ValidatorStdImplConfig;
use crate::tracing_targets;
use crate::validator::rpc::{ExchangeSignatures, ValidatorClient, ValidatorService};
use crate::validator::{
    AddSession, BriefValidatorDescr, CompositeValidationSessionId, ValidationComplete,
    ValidationSessionId, ValidationStatus, ValidatorNetworkContext, proto,
};
const METRIC_VALIDATE_BLOCK_TIME: &str = "tycho_validator_validate_block_time";
const METRIC_EXCHANGE_SIGNATURE_TIME: &str = "tycho_validator_exchange_signature_time";
const METRIC_RECEIVE_SIGNATURE_TIME: &str = "tycho_validator_receive_signature_time";
const METRIC_BLOCK_EXCHANGES_IN_TOTAL: &str = "tycho_validator_block_exchanges_in_total";
const METRIC_CACHE_EXCHANGES_IN_TOTAL: &str = "tycho_validator_cache_exchanges_in_total";
const METRIC_MISS_EXCHANGES_IN_TOTAL: &str = "tycho_validator_miss_exchanges_in_total";
const METRIC_INVALID_SIGNATURES_IN_TOTAL: &str = "tycho_validator_invalid_signatures_in_total";
const METRIC_INVALID_SIGNATURES_CACHED_TOTAL: &str = "tycho_validator_invalid_signatures_cached_total";
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
        let mut validators = FastHashMap::default();
        for descr in info.validators {
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
        let peer_ids = validators.values().map(|v| v.peer_id).collect::<Vec<_>>();
        let state = Arc::new(SessionState {
            shard_ident: info.shard_ident,
            weight_threshold,
            validators: Arc::new(validators),
            block_signatures: TreeIndex::new(),
            cached_signatures: TreeIndex::new(),
            cancelled: AtomicBool::new(false),
            cancelled_signal: Notify::new(),
        });
        let overlay_id = compute_session_overlay_id(
            &net_context.zerostate_id,
            &info.shard_ident,
            &info.session_id,
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
        let session = Self {
            inner: Arc::new(Inner {
                start_block_seqno: info.start_block_seqno,
                session_id: info.session_id,
                config: config.clone(),
                client: ValidatorClient::new(net_context.network.clone(), overlay),
                key_pair,
                peer_id: *peer_id,
                own_weight,
                state,
                min_seqno: AtomicU32::new(info.start_block_seqno),
            }),
        };
        metrics::gauge!(METRIC_SESSIONS_ACTIVE).increment(1);
        session.create_cache_slots(info.start_block_seqno, config.signature_cache_slots);
        Ok(session)
    }
    pub fn id(&self) -> ValidationSessionId {
        self.inner.session_id
    }
    pub fn start_block_seqno(&self) -> u32 {
        self.inner.start_block_seqno
    }
    pub fn cancel(&self) {
        self.inner.state.cancelled.store(true, Ordering::Release);
        self.inner.state.cancelled_signal.notify_waiters();
    }
    pub fn cancel_until(&self, block_seqno: u32) {
        self.inner.min_seqno.fetch_max(block_seqno, Ordering::Release);
        let state = self.inner.state.as_ref();
        state.cached_signatures.remove_range(..=block_seqno);
        let guard = scc::ebr::Guard::new();
        for (_, validation) in state.block_signatures.range(..=block_seqno, &guard) {
            validation.cancelled.cancel();
        }
        drop(guard);
        let until_seqno = block_seqno
            .saturating_sub(self.inner.config.old_blocks_to_keep);
        state.block_signatures.remove_range(..=until_seqno);
    }
    #[tracing::instrument(
        skip_all,
        fields(session_id = ?self.inner.session_id, %block_id)
    )]
    pub async fn validate_block(&self, block_id: &BlockId) -> Result<ValidationStatus> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(validate_block)),
            file!(),
            177u32,
        );
        let block_id = block_id;
        let _histogram = HistogramGuard::begin(METRIC_VALIDATE_BLOCK_TIME);
        tracing::info!(target : tracing_targets::VALIDATOR, "started");
        debug_assert_eq!(self.inner.state.shard_ident, block_id.shard);
        self.inner.min_seqno.fetch_max(block_id.seqno, Ordering::Release);
        self.create_cache_slots(
            block_id.seqno + 1,
            self.inner.config.signature_cache_slots,
        );
        let state = &self.inner.state;
        let cached = state
            .cached_signatures
            .peek(&block_id.seqno, &scc::ebr::Guard::new())
            .map(Arc::clone);
        let block_signatures = match &cached {
            Some(cached) => {
                __guard.end_section(199u32);
                let __result = self.reuse_signatures(block_id, cached.clone()).await;
                __guard.start_section(199u32);
                __result
            }
            None => self.prepare_new_signatures(block_id),
        }
            .build(block_id, state.weight_threshold);
        if state
            .block_signatures
            .insert(block_id.seqno, block_signatures.clone())
            .is_err()
        {
            anyhow::bail!(
                "block validation is already in progress. \
                session_id={:?}, block_id={}",
                self.inner.session_id, block_id,
            );
        }
        state.cached_signatures.remove(&block_id.seqno);
        let mut result = FastHashMap::default();
        result
            .insert(
                *self.inner.client.peer_id(),
                block_signatures.own_signature.clone(),
            );
        let mut total_weight = self.inner.own_weight;
        let semaphore = Arc::new(
            Semaphore::new(self.inner.config.max_parallel_requests),
        );
        let mut futures = FuturesUnordered::new();
        for (peer_id, signature) in block_signatures.other_signatures.iter() {
            __guard.checkpoint(238u32);
            if let Some(valid_signature) = signature.load_full() {
                let validator_info = state
                    .validators
                    .get(peer_id)
                    .expect("peer info out of sync");
                result.insert(*peer_id, valid_signature);
                total_weight += validator_info.weight;
                {
                    __guard.end_section(247u32);
                    __guard.start_section(247u32);
                    continue;
                };
            }
            let cached = cached
                .as_ref()
                .and_then(|c| c.other_signatures.get(peer_id))
                .and_then(|c| c.load_full());
            let fut = self
                .inner
                .clone()
                .receive_signature(
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
                target : tracing_targets::VALIDATOR, block_seqno = block_id.seqno,
                "session cancelled",
            );
            {
                __guard.end_section(271u32);
                return Ok(ValidationStatus::Skipped);
            };
        }
        let mut block_cancelled = pin!(block_signatures.cancelled.cancelled());
        while total_weight < state.weight_threshold {
            __guard.checkpoint(275u32);
            let res = {
                __guard.end_section(276u32);
                let __result = tokio::select! {
                    res = futures.next() => match res { Some(res) => res, None =>
                    anyhow::bail!("no more signatures to collect but the threshold is not reached"),
                    }, _ = & mut session_cancelled => { tracing::trace!(target :
                    tracing_targets::VALIDATOR, block_seqno = block_id.seqno,
                    "session cancelled",); return Ok(ValidationStatus::Skipped) }, _ = &
                    mut block_cancelled => { tracing::trace!(target :
                    tracing_targets::VALIDATOR, block_seqno = block_id.seqno,
                    "block cancelled",); return Ok(ValidationStatus::Skipped) },
                };
                __guard.start_section(276u32);
                __result
            };
            let validator_info = state
                .validators
                .get(&res.peer_id)
                .expect("peer info out of sync");
            let prev = result.insert(res.peer_id, res.signature);
            debug_assert!(prev.is_none(), "duplicate signature in result");
            total_weight += validator_info.weight;
        }
        tracing::info!(target : tracing_targets::VALIDATOR, "finished");
        Ok(
            ValidationStatus::Complete(ValidationComplete {
                signatures: result,
                total_weight,
            }),
        )
    }
    fn prepare_new_signatures(&self, block_id: &BlockId) -> BlockSignaturesBuilder {
        let data = Block::build_data_for_sign(block_id);
        let own_signature = Arc::new(self.inner.key_pair.sign_raw(&data));
        tracing::debug!(
            target : tracing_targets::VALIDATOR, % block_id, my_peer_id = % self.inner
            .peer_id, ? data, ? own_signature, "own signature created",
        );
        let validators = self.inner.state.validators.as_ref();
        let mut other_signatures = SignatureSlotsMap::with_capacity_and_hasher(
            validators.len(),
            Default::default(),
        );
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(reuse_signatures)),
            file!(),
            351u32,
        );
        let block_id = block_id;
        let cached = cached;
        let data = Block::build_data_for_sign(block_id);
        let block_id = *block_id;
        let key_pair = self.inner.key_pair.clone();
        let my_peer_id = self.inner.peer_id;
        let validators = self.inner.state.validators.clone();
        let mut total_weight = self.inner.own_weight;
        let span = tracing::Span::current();
        {
            __guard.end_section(421u32);
            let __result = tycho_util::sync::rayon_run(move || {
                    let _span = span.enter();
                    let own_signature = Arc::new(key_pair.sign_raw(&data));
                    tracing::debug!(
                        target : tracing_targets::VALIDATOR, % my_peer_id, % block_id, ?
                        data, ? own_signature, "own signature created",
                    );
                    let mut other_signatures = SignatureSlotsMap::with_capacity_and_hasher(
                        cached.other_signatures.len(),
                        Default::default(),
                    );
                    for (peer_id, cached) in cached.other_signatures.iter() {
                        let stored = 'stored: {
                            let Some(signature) = cached.load_full() else {
                                break 'stored Default::default();
                            };
                            let validator_info = validators
                                .get(peer_id)
                                .expect("peer info out of sync");
                            if !validator_info.public_key.verify_raw(&data, &signature) {
                                tracing::warn!(
                                    target : tracing_targets::VALIDATOR, % my_peer_id, %
                                    peer_id, public_key = % validator_info.public_key, %
                                    block_id, ? data, cached_signature = ? signature,
                                    "cached signature is invalid on reuse: {}",
                                    ValidationError::InvalidSignature
                                );
                                metrics::counter!(METRIC_INVALID_SIGNATURES_CACHED_TOTAL)
                                    .increment(1);
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
                .await;
            __guard.start_section(421u32);
            __result
        }
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
pub struct DebugLogValidatorSesssion<'a>(pub &'a ValidatorSession);
impl fmt::Debug for DebugLogValidatorSesssion<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidatorSession")
            .field("shard", &self.0.inner.state.shard_ident)
            .field("session_id", &self.0.inner.session_id)
            .field("public_key", &self.0.inner.key_pair.public_key)
            .field("peer_id", &self.0.inner.peer_id)
            .field("own_weight", &self.0.inner.own_weight)
            .field("weight_threshold", &self.0.inner.state.weight_threshold)
            .field("start_block_seqno", &self.0.inner.start_block_seqno)
            .field("min_seqno", &self.0.inner.min_seqno)
            .field("validators", &self.0.inner.state.validators)
            .finish()
    }
}
struct Inner {
    start_block_seqno: u32,
    session_id: ValidationSessionId,
    config: ValidatorStdImplConfig,
    client: ValidatorClient,
    key_pair: Arc<KeyPair>,
    peer_id: PeerId,
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(receive_signature)),
            file!(),
            477u32,
        );
        let peer_id = peer_id;
        let block_signatures = block_signatures;
        let cached = cached;
        let semaphore = semaphore;
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
        if let Some(signature) = cached {
            match state.add_signature(&block_signatures, slot, &peer_id, &signature) {
                Ok(()) => {
                    __guard.end_section(496u32);
                    return ValidSignature {
                        peer_id,
                        signature,
                    };
                }
                Err(ValidationError::SignatureChanged) => {
                    if let Some(signature) = slot.load_full() {
                        {
                            __guard.end_section(499u32);
                            return ValidSignature {
                                peer_id,
                                signature,
                            };
                        };
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target : tracing_targets::VALIDATOR, % peer_id,
                        "cached signature is invalid on add_signature: {e:?}",
                    );
                }
            }
        }
        let backoff = &self.config.exchange_signatures_backoff;
        let signature = loop {
            __guard.checkpoint(513u32);
            let mut retry = backon::ExponentialBuilder::default()
                .with_min_delay(backoff.min_interval)
                .with_max_delay(backoff.max_interval)
                .with_factor(backoff.factor)
                .with_max_times(usize::MAX)
                .build();
            let signature_fut = slot.into_future();
            let recv_fut = async {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    522u32,
                );
                loop {
                    __guard.checkpoint(523u32);
                    let permit = {
                        __guard.end_section(524u32);
                        let __result = semaphore.acquire().await;
                        __guard.start_section(524u32);
                        __result
                    }
                        .unwrap();
                    let timeout = self.config.exchange_signatures_timeout;
                    let query = {
                        let _histogram = HistogramGuard::begin(
                            METRIC_EXCHANGE_SIGNATURE_TIME,
                        );
                        self.client.query(&peer_id, req.clone())
                    };
                    match {
                        __guard.end_section(532u32);
                        let __result = tokio::time::timeout(timeout, query).await;
                        __guard.start_section(532u32);
                        __result
                    } {
                        Ok(Ok(res)) => {
                            match res.parse_tl() {
                                Ok(proto::Exchange::Complete(signature)) => {
                                    __guard.end_section(534u32);
                                    __guard.start_section(534u32);
                                    break signature;
                                }
                                Ok(proto::Exchange::Cached) => {
                                    tracing::trace!(
                                        target : tracing_targets::VALIDATOR,
                                        "partial signature exchange",
                                    );
                                }
                                Err(e) => {
                                    tracing::trace!(
                                        target : tracing_targets::VALIDATOR,
                                        "failed to parse response: {e:?}",
                                    );
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            tracing::trace!(
                                target : tracing_targets::VALIDATOR, "query failed: {e:?}",
                            )
                        }
                        Err(_) => {
                            tracing::trace!(
                                target : tracing_targets::VALIDATOR, "query timed out",
                            )
                        }
                    }
                    drop(permit);
                    let delay = retry.next().unwrap();
                    {
                        __guard.end_section(560u32);
                        let __result = tokio::time::sleep(delay).await;
                        __guard.start_section(560u32);
                        __result
                    };
                }
            };
            let signature = {
                __guard.end_section(564u32);
                let __result = tokio::select! {
                    signature = signature_fut => signature, signature = recv_fut =>
                    signature,
                };
                __guard.start_section(564u32);
                __result
            };
            match state.add_signature(&block_signatures, slot, &peer_id, &signature) {
                Ok(()) => {
                    __guard.end_section(570u32);
                    __guard.start_section(570u32);
                    break signature;
                }
                Err(ValidationError::SignatureChanged) => {
                    if let Some(signature) = slot.load_full() {
                        {
                            __guard.end_section(573u32);
                            __guard.start_section(573u32);
                            break signature;
                        };
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target : tracing_targets::VALIDATOR, % peer_id,
                        "fetched signature is invalid: {e:?}",
                    );
                }
            }
            {
                __guard.end_section(588u32);
                let __result = tokio::time::sleep(self.config.failed_exchange_interval)
                    .await;
                __guard.start_section(588u32);
                __result
            };
        };
        ValidSignature {
            peer_id,
            signature,
        }
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        metrics::gauge!(METRIC_SESSIONS_ACTIVE).decrement(1);
        tracing::debug!(
            target : tracing_targets::VALIDATOR, shard_ident = % self.state.shard_ident,
            session_id = ? self.session_id, "validator session dropped"
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
            None => {}
        }
        let data = Block::build_data_for_sign(&block.block_id);
        let validator_info = self
            .validators
            .get(peer_id)
            .expect("peer info out of sync");
        if !validator_info.public_key.verify_raw(&data, signature.as_ref()) {
            tracing::warn!(
                target : tracing_targets::VALIDATOR, % peer_id, public_key = %
                validator_info.public_key, block_id = % block.block_id, ? data, ?
                signature, "signature is invalid from peer {}",
                ValidationError::InvalidSignature
            );
            metrics::counter!(METRIC_INVALID_SIGNATURES_IN_TOTAL).increment(1);
            return Err(ValidationError::InvalidSignature);
        }
        let mut can_notify = false;
        match &*slot.compare_and_swap(&None::<Arc<[u8; 64]>>, Some(signature.clone())) {
            None => {
                slot.notify();
                let total_weight = block
                    .total_weight
                    .fetch_add(validator_info.weight, Ordering::Relaxed)
                    + validator_info.weight;
                can_notify = total_weight >= self.weight_threshold;
            }
            Some(saved) => {
                if saved.as_ref() != signature.as_ref() {
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(exchange_signatures)),
            file!(),
            762u32,
        );
        let peer_id = peer_id;
        let block_seqno = block_seqno;
        let signature = signature;
        if self.cancelled.load(Ordering::Acquire) {
            {
                __guard.end_section(764u32);
                return Err(ValidationError::Cancelled);
            };
        }
        let guard = scc::ebr::Guard::new();
        let result = if let Some(signatures) = self
            .block_signatures
            .peek(&block_seqno, &guard)
        {
            metrics::counter!(METRIC_BLOCK_EXCHANGES_IN_TOTAL).increment(1);
            let Some(slot) = signatures.other_signatures.get(peer_id) else {
                {
                    __guard.end_section(777u32);
                    return Err(ValidationError::UnknownPeer);
                };
            };
            if !signatures.validated.load(Ordering::Acquire) {
                self.add_signature(signatures, slot, peer_id, &signature)?;
            }
            proto::Exchange::Complete(signatures.own_signature.clone())
        } else {
            let Some(slot) = self.cached_signatures.peek(&block_seqno, &guard) else {
                metrics::counter!(METRIC_MISS_EXCHANGES_IN_TOTAL).increment(1);
                {
                    __guard.end_section(790u32);
                    return Err(ValidationError::NoSlot);
                };
            };
            metrics::counter!(METRIC_CACHE_EXCHANGES_IN_TOTAL).increment(1);
            let Some(saved_signature) = slot.other_signatures.get(peer_id) else {
                {
                    __guard.end_section(795u32);
                    return Err(ValidationError::UnknownPeer);
                };
            };
            saved_signature.store(Some(signature.clone()));
            proto::Exchange::Cached
        };
        drop(guard);
        let action = match &result {
            proto::Exchange::Complete(_) => "complete",
            proto::Exchange::Cached => "cached",
        };
        tracing::trace!(
            target : tracing_targets::VALIDATOR, % peer_id, block_seqno, action,
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
        if let Some(value) = self.0.value.load_full() {
            return Poll::Ready(value);
        }
        let mut waker = self.0.waker.lock();
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
    session_id: &ValidationSessionId,
) -> OverlayId {
    OverlayId(
        tl_proto::hash(proto::OverlayIdData {
            zerostate_root_hash: zerostate_id.root_hash.0,
            zerostate_file_hash: zerostate_id.file_hash.0,
            shard_ident: *shard_ident,
            session_seqno: session_id.seqno(),
        }),
    )
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
