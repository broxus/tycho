use std::collections::{btree_map, BTreeMap};
use std::sync::{Arc, OnceLock};

use everscale_crypto::ed25519::KeyPair;
use futures_util::FutureExt;

use crate::dag::dag_point_future::DagPointFuture;
use crate::effects::{AltFmt, AltFormat};
use crate::engine::CachedConfig;
use crate::models::{DagPoint, Digest, Round, Signature, UnixTime, ValidPoint};

/// If DAG location exists, it must have non-empty `versions` map;
///
/// Inclusion state is filled if it belongs to the 2 latest dag rounds
/// and will be used for own point production
///
/// Note methods encapsulate mutability to preserve this invariant, a bit less panics
#[derive(Default)]
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
    state: InclusionState,
    /// only one of the point versions at current location
    /// may become proven by the next round point(s) of a node;
    /// even if we marked a proven point as invalid, consensus may ignore our decision
    versions: BTreeMap<Digest, DagPointFuture>,
    /// If author produced and sent a point with invalid sig, that only affects equivocation,
    /// but that point cannot be used in any other way.
    pub bad_sig_in_broadcast: bool,
}

impl DagLocation {
    // point that is validated depends on other equivocated points futures (if any)
    // in the same location, so need to keep order of futures' completion;
    // to make signature we are interested in the single validated point only
    // (others are at least suspicious and cannot be signed)
    pub fn get_or_init<F>(&mut self, digest: &Digest, init: F) -> &DagPointFuture
    where
        F: FnOnce(&InclusionState) -> DagPointFuture,
    {
        self.versions
            .entry(*digest)
            .or_insert_with(|| init(&self.state))
    }
    pub fn init_or_modify<F, U>(
        &mut self,
        digest: &Digest,
        init: F,
        modify: U,
    ) -> Option<&DagPointFuture>
    where
        F: FnOnce(&InclusionState) -> DagPointFuture,
        U: FnOnce(&DagPointFuture),
    {
        match self.versions.entry(*digest) {
            btree_map::Entry::Occupied(entry) => {
                modify(entry.get());
                None
            }
            btree_map::Entry::Vacant(entry) => Some(entry.insert(init(&self.state))),
        }
    }
    pub fn versions(&self) -> &'_ BTreeMap<Digest, DagPointFuture> {
        &self.versions
    }
    pub fn state(&self) -> &'_ InclusionState {
        &self.state
    }
}

// Todo remove inner locks and introduce global current dag round watch simultaneously, see Collector
#[derive(Default, Clone)]
pub struct InclusionState(Arc<OnceLock<Signable>>);

impl InclusionState {
    /// Must not be used for downloaded dependencies
    pub(super) fn init(&self, first_completed: &DagPoint) {
        _ = self.0.get_or_init(|| {
            let signable = Signable {
                first_completed: first_completed.clone(),
                signed: OnceLock::new(),
            };
            if first_completed.trusted().is_none() {
                tracing::warn!(
                    result = display(first_completed.alt()),
                    author = display(first_completed.author().alt()),
                    round = first_completed.round().0,
                    digest = display(first_completed.digest().alt()),
                    "resolved dag point",
                );
                signable.reject();
            }
            signable
        });
    }
    pub fn signable(&self) -> Option<&'_ Signable> {
        self.0.get()
    }
    pub fn signed(&self) -> Option<&'_ Result<Signed, ()>> {
        self.0.get()?.signed.get()
    }
    pub fn signed_point(&self, at: Round) -> Option<&'_ ValidPoint> {
        let signable = self.0.get()?;
        if signable.signed.get()?.as_ref().ok()?.at == at {
            signable.first_completed.valid()
        } else {
            None
        }
    }
    pub fn point(&self) -> Option<&DagPoint> {
        self.0.get().map(|signable| &signable.first_completed)
    }
}

// Todo actually we are not interested in the round of making a signature,
//   but a round of the first (and only) reference.
//   Since the single version of a point is decided as eligible for signature (trusted),
//   it may be included immediately; no need to include it twice.
//   One cannot include point with the time lesser than the proven anchor candidate -
//   and that's all for the global time sequence, i.e. any node with a great time skew
//   can produce points to be included and committed, but cannot accomplish leader's requirements.
#[derive(Debug)]
pub struct Signable {
    first_completed: DagPoint,
    // signature cannot be rolled back, the point must be included as next point dependency
    signed: OnceLock<Result<Signed, ()>>,
}

#[derive(Debug)]
pub struct Signed {
    pub at: Round,
    pub with: Signature,
}

impl Signable {
    pub fn sign(
        &self,
        at: Round,
        key_pair: Option<&KeyPair>, // same round for own point and next round for other's
    ) -> bool {
        let mut this_call_signed = false;
        if let Some((valid, key_pair)) = self.first_completed.trusted().zip(key_pair) {
            if valid.info.data().time < (UnixTime::now() + CachedConfig::clock_skew()) {
                _ = self.signed.get_or_init(|| {
                    this_call_signed = true;
                    Ok(Signed {
                        at,
                        with: Signature::new(key_pair, valid.info.digest()),
                    })
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
}

impl AltFormat for DagLocation {}
impl std::fmt::Debug for AltFmt<'_, DagLocation> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let loc = AltFormat::unpack(self);
        for (digest, promise) in loc.versions() {
            write!(f, "#{}-", digest.alt())?;
            match promise.clone().now_or_never() {
                None => write!(f, "None, ")?,
                Some(point) => write!(f, "{}, ", point.alt())?,
            };
        }
        Ok(())
    }
}
