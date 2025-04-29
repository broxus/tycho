use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::{atomic, Arc, Weak};

use ahash::HashMapExt;
use parking_lot::Mutex;
use tycho_util::FastHashMap;

use crate::models::Digest;

#[derive(Clone, Default)]
pub struct Cert(Arc<CertInner>);
pub struct WeakCert(Weak<CertInner>);

#[derive(Default)]
struct CertInner {
    is_certified: AtomicBool,
    has_deps: AtomicBool,
    /// set at most once; taken at most once only if certified
    direct_deps: Mutex<DepsLifeCycle>,
}

#[derive(Default)]
enum DepsLifeCycle {
    #[default]
    Unset,
    Set(CertDirectDeps),
    Taken,
}

pub struct CertDirectDeps {
    pub includes: FastHashMap<Digest, WeakCert>,
    pub witness: FastHashMap<Digest, WeakCert>,
}

impl Cert {
    pub fn is_certified(&self) -> bool {
        self.0.is_certified.load(atomic::Ordering::Acquire)
    }

    /// must be used at most once
    pub fn set_deps(&self, direct_deps: CertDirectDeps) {
        if self.is_certified() {
            // Note: leaves lock in `DepsLifeCycle::Unset` state, but atomic flag is still correct
            Self::traverse_certify(direct_deps);
        } else {
            self.0.set_deps(direct_deps);
            // `certify()` call may set `is_certified` but miss `has_deps`, so have to re-check
            if self.is_certified() {
                if let Some(direct_deps) = self.0.try_take_deps() {
                    Self::traverse_certify(direct_deps);
                }
            }
        }
    }

    pub fn certify(&self) {
        self.0.try_certify();
        if let Some(direct_deps) = self.0.try_take_deps() {
            Self::traverse_certify(direct_deps);
        }
    }

    fn certify_as_dep(&self) -> Option<CertDirectDeps> {
        self.0.try_certify();
        self.0.try_take_deps()
    }

    fn traverse_certify(direct_deps: CertDirectDeps) {
        // very much like commit: current @ r+0, includes @ r-1, witness @ r-2
        let mut deps = {
            let estimated_capacity =
                (((direct_deps.includes.len() + direct_deps.witness.len()) * 3) / 2) + 1;
            [
                direct_deps.includes,
                direct_deps.witness,
                FastHashMap::with_capacity(estimated_capacity),
            ]
        };

        while !deps.iter().all(|hm| hm.is_empty()) {
            let [current, includes, witness] = &mut deps;
            for (_digest, parent) in current.drain() {
                if let Some(direct_deps) = parent.upgrade().and_then(|cert| cert.certify_as_dep()) {
                    for (digest, weak) in direct_deps.includes {
                        includes.insert(digest, weak);
                    }
                    for (digest, weak) in direct_deps.witness {
                        witness.insert(digest, weak);
                    }
                }
            }
            deps.rotate_left(1);
        }
    }

    pub fn downgrade(&self) -> WeakCert {
        WeakCert(Arc::downgrade(&self.0))
    }
}

impl WeakCert {
    pub fn upgrade(&self) -> Option<Cert> {
        Some(Cert(self.0.upgrade()?))
    }
}

impl CertInner {
    fn try_certify(&self) {
        let _ = (self.is_certified)
            .compare_exchange(
                false,
                true,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Relaxed,
            )
            .is_ok();
    }

    fn try_take_deps(&self) -> Option<CertDirectDeps> {
        let is_first = (self.has_deps)
            .compare_exchange(
                true,
                false,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Relaxed,
            )
            .is_ok();
        if !is_first {
            return None;
        }
        let mut guard = self.direct_deps.lock();
        match std::mem::replace(&mut *guard, DepsLifeCycle::Taken) {
            DepsLifeCycle::Set(deps) => Some(deps),
            forbidden @ (DepsLifeCycle::Unset | DepsLifeCycle::Taken) => {
                panic!("cert synchronisation is broken: take {forbidden:?}")
            }
        }
    }

    fn set_deps(&self, direct_deps: CertDirectDeps) {
        {
            let mut guard = self.direct_deps.lock();
            match std::mem::replace(&mut *guard, DepsLifeCycle::Set(direct_deps)) {
                DepsLifeCycle::Unset => {}
                ref forbidden @ (DepsLifeCycle::Set(_) | DepsLifeCycle::Taken) => {
                    panic!("cert deps must be set only once, prev value: {forbidden:?}");
                }
            }
        }
        self.has_deps.store(true, atomic::Ordering::Release);
    }
}

impl Debug for Cert {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_struct("Cert");
        builder.field("is_certified", &self.is_certified());
        builder.field("has_deps", &self.0.has_deps.load(atomic::Ordering::Acquire));
        {
            let guard = self.0.direct_deps.lock();
            let life_cycle: &DepsLifeCycle = &guard;
            builder.field("deps", life_cycle);
        };
        builder.finish()
    }
}
impl Debug for DepsLifeCycle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DepsLifeCycle::Unset => f.write_str("unset"),
            DepsLifeCycle::Set(deps) => write!(f, "set {:?}", deps),
            DepsLifeCycle::Taken => f.write_str("taken"),
        }
    }
}
impl Debug for CertDirectDeps {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}+{}", self.includes.len(), self.witness.len())
    }
}
