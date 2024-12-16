use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use everscale_crypto::ed25519::KeyPair;
use futures_util::FutureExt;

use crate::dag::dag_point_future::DagPointFuture;
use crate::dag::WeakDagRound;
use crate::dyn_event;
use crate::effects::{AltFmt, AltFormat, ValidateCtx};
use crate::engine::CachedConfig;
use crate::models::{DagPoint, Digest, Round, Signature, UnixTime};

#[cfg_attr(feature = "test", derive(Clone))]
pub struct DagLocation {
    // one of the points at current location
    // was proven by the next point of a node;
    // even if we marked this point as invalid, consensus may override our decision
    // and we will have to sync
    // vertex: Option<Digest>,
    /// We can sign or reject just a single (e.g. first validated) point at the current location;
    /// other (equivocated) points may be received as includes, witnesses or a proven vertex;
    /// we have to include signed points @ r+0 & @ r-1 as dependencies in our point @ r+1.
    /// Not needed for transitive dependencies.
    pub state: InclusionState,
    /// only one of the point versions at current location
    /// may become proven by the next round point(s) of a node;
    /// even if we marked a proven point as invalid, consensus may ignore our decision
    pub versions: BTreeMap<Digest, DagPointFuture>,
    /// If author produced and sent a point with invalid sig, that only affects equivocation,
    /// but that point cannot be used in any other way.
    pub bad_sig_in_broadcast: bool,
}

impl DagLocation {
    pub fn new(parent: WeakDagRound) -> Self {
        DagLocation {
            state: InclusionState::new(parent),
            versions: BTreeMap::new(),
            bad_sig_in_broadcast: false,
        }
    }
}

#[derive(Clone)]
pub struct InclusionState(Arc<InclusionStateInner>);
struct InclusionStateInner {
    parent: WeakDagRound,
    signable: OnceLock<Signable>,
}

impl InclusionState {
    fn new(parent: WeakDagRound) -> Self {
        Self(Arc::new(InclusionStateInner {
            parent,
            signable: OnceLock::new(),
        }))
    }

    /// Must not be used for downloaded dependencies
    pub fn init(&self, resolved: &DagPoint) -> &Signable {
        let mut is_first = false;
        let signable = self.0.signable.get_or_init(|| {
            let signable = Signable {
                first_resolved: resolved.clone(),
                signed: OnceLock::new(),
            };
            is_first = true;
            signable
        });
        let mut log_level = tracing::Level::DEBUG;
        if is_first {
            ValidateCtx::first_resolved(resolved);
            // FIXME certified is a flavor of valid, as verified by signature chain
            if let Some(valid) = resolved.valid() {
                if let Some(dag_round) = self.0.parent.upgrade() {
                    dag_round.threshold().add(valid);
                }
            } else {
                log_level = log_level.max(tracing::Level::WARN);
                signable.reject();
            };
        } else {
            ValidateCtx::alt_resolved(resolved);
            log_level = log_level.max(tracing::Level::WARN);
        }
        dyn_event!(
            log_level,
            result = display(resolved.alt()),
            author = display(resolved.author().alt()),
            round = resolved.round().0,
            digest = display(resolved.digest().alt()),
            signed = signable.signed().map(|a| a.is_ok()),
            rejected = signable.signed().map(|a| a.is_err()),
            first_digest = Some(!is_first)
                .filter(|x| *x)
                .map(|_| display(signable.first_resolved.digest().alt())),
            "resolved dag point",
        );
        signable
    }
    pub fn signable(&self) -> Option<&'_ Signable> {
        self.0.signable.get()
    }
}

// Actually we are not interested in the round of making a signature,
// but a round of the first (and only) reference.
// Since the single version of a point is decided as eligible for signature (trusted),
// it may be included immediately; no need to include it twice.
// One cannot include point with the time lesser than the proven anchor candidate -
// and that's all for the global time sequence, i.e. any node with a big enough time skew
// can produce points to be included and committed, but cannot accomplish leader's requirements.
#[derive(Debug)]
pub struct Signable {
    pub first_resolved: DagPoint,
    // signature cannot be rolled back, the point must be included as next point dependency
    signed: OnceLock<Result<Signature, ()>>,
}

impl Signable {
    pub fn sign(
        &self,
        at: Round,
        key_pair: Option<&KeyPair>, // same round for own point and next round for other's
    ) -> bool {
        let mut this_call_signed = false;
        if let Some((valid, key_pair)) = self.first_resolved.trusted().zip(key_pair) {
            if valid.info.data().time - UnixTime::now()
                < UnixTime::from_millis(CachedConfig::get().consensus.clock_skew_millis as _)
            {
                _ = self.signed.get_or_init(|| {
                    this_call_signed = true;
                    Ok(Signature::new(key_pair, valid.info.digest()))
                });
                if this_call_signed {
                    if valid.info.round() > at {
                        panic!(
                            "cannot sign point of future round {:?} at round {}",
                            valid.info.id(),
                            at.0
                        );
                    } else if valid.info.round() == at {
                        metrics::counter!("tycho_mempool_signing_current_round_count").increment(1);
                    } else if valid.info.round() == at.prev() {
                        metrics::counter!("tycho_mempool_signing_prev_round_count").increment(1);
                    } else {
                        panic!(
                            "cannot sign point of old round {:?} at round {}",
                            valid.info.id(),
                            at.0
                        );
                    }
                } // else: just retry of sig request
            } else {
                // decide later
                metrics::counter!("tycho_mempool_signing_postponed").increment(1);
            }
        } else {
            self.reject();
        }
        this_call_signed
    }
    fn reject(&self) {
        metrics::counter!("tycho_mempool_signing_rejected").increment(1);
        _ = self.signed.set(Err(()));
    }
    pub fn signed(&self) -> Option<&Result<Signature, ()>> {
        self.signed.get()
    }
}

impl AltFormat for DagLocation {}
impl std::fmt::Debug for AltFmt<'_, DagLocation> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let loc = AltFormat::unpack(self);
        for (digest, promise) in &loc.versions {
            write!(f, "#{}-", digest.alt())?;
            match promise.clone().now_or_never() {
                None => write!(f, "None, ")?,
                Some(point) => write!(f, "{}, ", point.alt())?,
            };
        }
        Ok(())
    }
}
