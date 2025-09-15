use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock};

use futures_util::FutureExt;
use tycho_crypto::ed25519::KeyPair;

use crate::dag::WeakDagRound;
use crate::dag::dag_point_future::DagPointFuture;
use crate::dyn_event;
use crate::effects::{AltFmt, AltFormat, ValidateCtx};
use crate::engine::MempoolConfig;
use crate::models::{
    DagPoint, Digest, PointId, PointStatus, Round, Signature, UnixTime, ValidPoint,
};

#[cfg_attr(any(feature = "test", test), derive(Clone))]
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
}

impl DagLocation {
    pub fn new(parent: WeakDagRound) -> Self {
        DagLocation {
            state: InclusionState::new(parent),
            versions: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct InclusionState(Arc<InclusionStateInner>);
// NOTE `valid` init lock must be acquired inside `resolved` or separately,
//   never acquire `resolved` inside `valid`
struct InclusionStateInner {
    parent: WeakDagRound,
    /// in general case is both first and valid,
    /// but may be closed for signing before first valid is downloaded;
    /// if some point was chosen to be the `first resolved` by acquiring location and
    /// owning the flag, any other point may resolve in DAG earlier and must be ignored
    resolved: OnceLock<FirstResolved>,
    /// may be resolved after invalid
    valid: FirstValid,
}

enum FirstResolved {
    Valid(Digest, OnceLock<Result<Signable, ()>>),
    NotValid(Digest),
    /// Closes the ability for location to have signature when Engine moved forward.
    /// This makes not possible for other nodes to create a point with a local sig among proofs
    /// when local node cannot reference that signed point in its own points.
    Closed,
}

struct FirstValid(OnceLock<Digest>);

struct Signable {
    valid: ValidPoint,
    signature: OnceLock<Result<Signature, ()>>,
}

impl InclusionState {
    fn new(parent: WeakDagRound) -> Self {
        Self(Arc::new(InclusionStateInner {
            parent,
            resolved: OnceLock::new(),
            valid: FirstValid(OnceLock::new()),
        }))
    }

    pub fn acquire<T: PointStatus>(&self, id: &PointId, status: &mut T) {
        assert!(
            !status.is_first_resolved(),
            "{:?} {status} must not have first_resolved flag",
            id.alt()
        );
        assert!(
            !status.is_first_valid(),
            "{:?} {status} must not have first_valid flag",
            id.alt()
        );
        // Note: concurrent callers must block until `resolved` is initialized, so that holds:
        //  * if the first resolved is valid, then it is also the first valid
        //  * if the first resolved is not valid, then some next valid will be the first valid
        let resolved = self.0.resolved.get_or_init(|| {
            status.set_first_resolved();
            if status.is_valid() {
                // the only place with nested lock - to resolve race during general work
                let valid = self.0.valid.0.get_or_init(|| {
                    status.set_first_valid();
                    id.digest
                });
                assert_eq!(
                    *valid,
                    id.digest,
                    "first valid must not be init earlier than first resolved, {:?} {status}",
                    id.alt()
                );
                FirstResolved::Valid(id.digest, OnceLock::new())
            } else {
                FirstResolved::NotValid(id.digest)
            }
        });
        if !status.is_first_resolved() && status.is_valid() {
            self.0.valid.0.get_or_init(|| {
                status.set_first_valid();
                id.digest
            });
        }
        let log_level = if status.is_first_resolved() && status.is_first_valid() {
            tracing::Level::TRACE
        } else {
            tracing::Level::WARN
        };
        dyn_event!(
            log_level,
            author = display(id.author.alt()),
            round = id.round.0,
            digest = display(id.digest.alt()),
            status = display(status),
            first_reolved = debug(resolved.alt()),
            first_valid = debug(self.0.valid.alt()),
            "dag location acquired",
        );
    }

    /// NOTE must be called sequentially: races are not permitted during restore
    pub fn acquire_restore<T: PointStatus>(&self, id: &PointId, status: &T) {
        if status.is_first_resolved() {
            let resolved = self.0.resolved.get_or_init(|| {
                if status.is_valid() {
                    FirstResolved::Valid(id.digest, OnceLock::new())
                } else {
                    FirstResolved::NotValid(id.digest)
                }
            });
            match resolved {
                FirstResolved::Valid(first, _) | FirstResolved::NotValid(first)
                    if id.digest == *first => {}
                _ => panic!(
                    "{:?} {status} was not restored as first_resolved: already acquired {:?}",
                    id.alt(),
                    resolved.alt(),
                ),
            }
        }
        if status.is_first_valid() {
            assert!(
                status.is_valid(),
                "{:?} {status} is not valid but has first_valid flag",
                id.alt()
            );
            let first = self.0.valid.0.get_or_init(|| id.digest);
            assert_eq!(
                id.digest,
                *first,
                "{:?} {status} was not restored as first_valid: already acquired by {}",
                id.alt(),
                first.alt()
            );
        }

        let log_level = if status.is_first_resolved() && status.is_first_valid() {
            tracing::Level::TRACE
        } else {
            tracing::Level::WARN
        };
        dyn_event!(
            log_level,
            author = display(id.author.alt()),
            round = id.round.0,
            digest = display(id.digest.alt()),
            status = display(status),
            first_resolved = debug(self.0.resolved.get().map(|first| first.alt())),
            first_valid = debug(self.0.valid.alt()),
            "dag location acquire restored",
        );
    }

    pub fn resolve(&self, dag_point: &DagPoint) {
        // first resolved

        if dag_point.is_first_resolved() {
            let Some(resolved) = self.0.resolved.get() else {
                panic!(
                    "{:?} {} has first_resolved flag but state was not acquired",
                    dag_point.id().alt(),
                    dag_point.alt(),
                );
            };
            match resolved {
                FirstResolved::Valid(digest, signable) if digest == dag_point.digest() => {
                    let Some(valid) = dag_point.valid() else {
                        panic!(
                            "{:?} {} is not valid to resolve acquired state {:?}",
                            dag_point.id().alt(),
                            dag_point.alt(),
                            resolved.alt(),
                        );
                    };
                    // may be concurrently rejected because Engine advanced, so no check
                    signable.get_or_init(|| {
                        Ok(Signable {
                            valid: valid.clone(),
                            signature: OnceLock::new(),
                        })
                    });
                }
                FirstResolved::NotValid(digest)
                    if digest == dag_point.digest() && dag_point.valid().is_none() => {}
                FirstResolved::Closed => {} // concurrently closed during local point creation
                _ => panic!(
                    "{:?} {} has first_resolved flag, but state is acquired by another {:?}",
                    dag_point.id().alt(),
                    dag_point.alt(),
                    resolved.alt(),
                ),
            }
        } else if let Some(resolved) = self.0.resolved.get()
            && let FirstResolved::Valid(digest, _) | FirstResolved::NotValid(digest) = resolved
            && digest == dag_point.digest()
        {
            panic!(
                "{:?} {} has no first_resolved flag, but acquired its state {:?}",
                dag_point.id().alt(),
                dag_point.alt(),
                resolved.alt(),
            )
        }
        ValidateCtx::resolved(dag_point); // meter after checks

        // first valid

        if let Some(valid) = dag_point.valid().filter(|v| v.is_first_valid()) {
            let Some(first_valid) = self.0.valid.0.get() else {
                panic!(
                    "{:?} {} has first_valid flag, but its state was not acquired",
                    dag_point.id().alt(),
                    dag_point.alt(),
                )
            };
            if valid.info().digest() == first_valid {
                if let Some(dag_round) = self.0.parent.upgrade() {
                    dag_round.threshold().add(valid);
                }
            } else {
                panic!(
                    "{:?} {} has first_valid flag, but its state is acquired by other # {}",
                    dag_point.id().alt(),
                    dag_point.alt(),
                    first_valid.alt()
                )
            }
        } else if let Some(first_valid) = self.0.valid.0.get() {
            assert_ne!(
                dag_point.digest(),
                first_valid,
                "{:?} {} acquired first valid state, but has no flag or is not valid",
                dag_point.id().alt(),
                dag_point.alt(),
            );
        }

        tracing::debug!(
            author = display(dag_point.author()),
            round = dag_point.round().0,
            digest = display(dag_point.digest().alt()),
            status = display(dag_point.alt()),
            first_resolved = debug(self.0.resolved.get().map(|a| a.alt())),
            first_valid = debug(self.0.valid.alt()),
            "point resolved",
        );
    }

    /// `None` if not yet defined, `Some::<Result>` is irreversible
    pub fn sign(
        &self,
        at: Round,
        key_pair: Option<&KeyPair>,
        conf: &MempoolConfig,
    ) -> Option<Result<Signed<'_>, ()>> {
        let maybe_signable = match &self.0.resolved.get()? {
            FirstResolved::Valid(_, once) => once.get()?,
            FirstResolved::NotValid(_) | FirstResolved::Closed => return Some(Err(())),
        };
        match maybe_signable {
            Ok(signable) => signable.sign(at, key_pair, conf),
            Err(()) => Some(Err(())),
        }
    }

    /// irreversible
    pub fn get_or_reject(&self) -> Result<Signed<'_>, ()> {
        let resolved = self.0.resolved.get_or_init(|| FirstResolved::Closed);
        let maybe_signable = match resolved {
            FirstResolved::Valid(_, once) => once.get_or_init(|| Err(())),
            FirstResolved::NotValid(_) | FirstResolved::Closed => return Err(()),
        };
        let signable = maybe_signable.as_ref().map_err(|_ignore| ())?;
        // intentionally no metrics for rejection at last chance: no sig request was received
        match signable.signature.get_or_init(|| Err(())) {
            Ok(signature) => Ok(Signed {
                first_resolved: &signable.valid,
                signature,
            }),
            Err(()) => Err(()),
        }
    }
}

pub struct Signed<'a> {
    pub first_resolved: &'a ValidPoint,
    pub signature: &'a Signature,
}

// Actually we are not interested in the round of making a signature,
// but a round of the first (and only) reference.
// Since the first version of a point is decided as eligible for signature,
// it may be included immediately; no need to include it twice.
// One cannot include point with the time lesser than the proven anchor candidate -
// and that's all for the global time sequence, i.e. any node with a big enough time skew
// can produce points to be included and committed, but cannot accomplish leader's requirements.

impl Signable {
    /// `None` if not yet defined, `Some::<Result>` is irreversible
    fn sign(
        &self,
        at: Round,
        key_pair: Option<&KeyPair>,
        conf: &MempoolConfig,
    ) -> Option<Result<Signed<'_>, ()>> {
        let result = match self.signature.get() {
            Some(ready) => ready,
            None => {
                const POSTPONED: &str = "tycho_mempool_signing_postponed";
                const REJECTED: &str = "tycho_mempool_signing_rejected";

                if let Some(key_pair) = key_pair {
                    if self.valid.info().round() > at {
                        metrics::counter!(POSTPONED, "kind" => "round").increment(1);
                        return None; // retry later
                    }
                    if self.valid.info().time() - UnixTime::now()
                        >= UnixTime::from_millis(conf.consensus.clock_skew_millis.get() as _)
                    {
                        metrics::counter!(POSTPONED, "kind" => "time").increment(1);
                        return None; // retry later
                    }
                    self.signature.get_or_init(|| {
                        if self.valid.info().round() >= at.prev() {
                            if self.valid.info().round() == at {
                                metrics::counter!("tycho_mempool_signing_current_round_count")
                                    .increment(1);
                            } else {
                                metrics::counter!("tycho_mempool_signing_prev_round_count")
                                    .increment(1);
                            }
                            Ok(Signature::new(key_pair, self.valid.info().digest()))
                        } else {
                            metrics::counter!(REJECTED, "kind" => "round").increment(1);
                            Err(()) // too old round
                        }
                    })
                } else if self.valid.info().round() >= at {
                    // metrics::counter!(POSTPONED, "kind" => "round").increment(1);
                    return None; // wait for retry at next round, maybe keys will be defined
                } else {
                    self.signature.get_or_init(|| {
                        metrics::counter!(REJECTED, "kind" => "v_set").increment(1);
                        Err(()) // we are not in v_set and cannot sign, now for sure
                    })
                }
            }
        };
        Some(match result {
            Ok(signature) => Ok(Signed {
                first_resolved: &self.valid,
                signature,
            }),
            Err(()) => Err(()),
        })
    }
}

impl AltFormat for FirstResolved {}
impl Debug for AltFmt<'_, FirstResolved> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self) {
            FirstResolved::Valid(digest, signable) => match signable.get() {
                None => write!(f, "Valid(# {}, Signable(NotResolved))", digest.alt()),
                Some(result) => {
                    let signable_alt = result.as_ref().map(|signable| signable.alt());
                    write!(f, "Valid(# {}, {signable_alt:?})", digest.alt())
                }
            },
            FirstResolved::NotValid(digest) => {
                write!(f, "NotValid(# {})", digest.alt())
            }
            FirstResolved::Closed => f.write_str("Closed"),
        }
    }
}

impl AltFormat for FirstValid {}
impl Debug for AltFmt<'_, FirstValid> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match AltFormat::unpack(self).0.get() {
            Some(digest) => write!(f, "{}", digest.alt()),
            None => f.write_str("Empty"),
        }
    }
}

impl AltFormat for Signable {}
impl Debug for AltFmt<'_, Signable> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        let digest = inner.valid.info().digest().alt();
        match inner.signature.get() {
            None => write!(f, "Signable(# {digest})"),
            Some(Ok(_sig)) => write!(f, "Signed(# {digest})"),
            Some(Err(())) => write!(f, "Rejected(# {digest})"),
        }
    }
}

impl AltFormat for Signed<'_> {}
impl Debug for AltFmt<'_, Signed<'_>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = AltFormat::unpack(self);
        write!(
            f,
            "Signed {{ {:?} {} }}",
            &inner.first_resolved.info().id().alt(),
            &inner.first_resolved.alt()
        )
    }
}

impl AltFormat for DagLocation {}
impl Debug for AltFmt<'_, DagLocation> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let loc = AltFormat::unpack(self);
        for (digest, promise) in &loc.versions {
            write!(f, "#{}-", digest.alt())?;
            match promise.clone().now_or_never() {
                None => write!(f, "None, ")?,
                Some(Ok(point)) => write!(f, "{}, ", point.alt())?,
                Some(Err(e)) => write!(f, "{e}, ")?,
            };
        }
        Ok(())
    }
}
