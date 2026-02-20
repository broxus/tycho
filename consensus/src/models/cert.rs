use std::cell::RefCell;
use std::collections::hash_map;
use std::sync::{Arc, Weak};

use tycho_util::FastHashMap;

use self::inner::CertInner;
use crate::engine::MempoolConfig;
use crate::models::Digest;

#[derive(Clone, Default)]
pub struct Cert(Arc<CertInner>);
pub struct WeakCert(Weak<CertInner>);

thread_local! {
    // each map will have nearly the current vset size
    static DEPS_QUEUE: RefCell<[FastHashMap<Digest, Cert>; 3]> = Default::default();
}

impl Cert {
    /// `commit` is an action on certified points only
    pub fn set_committed(&self) {
        self.0.set_committed();
    }

    pub fn is_committed(&self) -> bool {
        self.0.is_committed()
    }

    pub fn is_certified(&self) -> bool {
        self.0.certified().is_some()
    }

    pub fn has_proof(&self) -> bool {
        self.0.certified().is_some_and(|directly| directly)
    }

    pub fn set_deps(&self, direct_deps: CertDirectDeps) {
        if let Some((direct_deps, height)) = self.0.set_deps(direct_deps) {
            direct_deps.traverse_certify(height.saturating_sub(1));
        };
    }

    pub fn certify(&self, conf: &MempoolConfig) {
        // config value leaves enough room for +1 and other stuff (hidden flags)
        let max_depth = <u16 as From<u8>>::from(conf.consensus.commit_history_rounds.get());
        // current height == commit length + 1 round for current point (same as for anchor)
        if let Some(direct_deps) = self.0.certify_deeper(max_depth + 1, true) {
            direct_deps.traverse_certify(max_depth);
        };
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

/// Should be filled with unique digests
pub struct CertDirectDeps {
    pub includes: Vec<(Digest, WeakCert)>,
    pub witness: Vec<(Digest, WeakCert)>,
}

struct CertDirectRefs<'a> {
    includes: &'a [(Digest, WeakCert)],
    witness: Option<&'a [(Digest, WeakCert)]>,
}

impl CertDirectRefs<'_> {
    fn traverse_certify(self, current_deps_height: u16) {
        if current_deps_height == 0 {
            return;
        }
        DEPS_QUEUE.with_borrow_mut(|deps_queue| {
            let [current, includes, _] = deps_queue;

            // process direct `includes` and enqueue deps into maps without shift

            for (_, weak) in self.includes {
                let Some(child) = weak.upgrade() else {
                    continue;
                };
                let Some(child_deps) = child.0.certify_deeper(current_deps_height, false) else {
                    continue;
                };
                Self::extend_next(current, child_deps.includes);
                if let Some(child_witness) = child_deps.witness {
                    Self::extend_next(includes, child_witness);
                }
            }
            if let Some(direct_witness) = self.witness {
                Self::extend_next(current, direct_witness);
            }

            // process top map from queue, load refs into other two maps, shift, repeat

            for height in (1..current_deps_height).rev() {
                if deps_queue.iter().all(|map| map.is_empty()) {
                    break;
                }
                let [current, includes, witness] = deps_queue;
                for (_, child) in current.drain() {
                    if let Some(child_deps) = child.0.certify_deeper(height, false) {
                        Self::extend_next(includes, child_deps.includes);
                        if let Some(child_witness) = child_deps.witness {
                            Self::extend_next(witness, child_witness);
                        }
                    };
                }
                deps_queue.rotate_left(1);
            }

            let max_len = self.includes.len() * 4;
            for map in deps_queue {
                map.clear();
                if map.capacity() > max_len {
                    map.shrink_to(max_len / 2);
                }
            }
        });
    }

    fn extend_next(dst: &mut FastHashMap<Digest, Cert>, source: &[(Digest, WeakCert)]) {
        for (digest, weak) in source {
            let hash_map::Entry::Vacant(vacant) = dst.entry(*digest) else {
                continue;
            };
            let Some(cert) = weak.upgrade() else {
                continue;
            };
            vacant.insert(cert);
        }
    }
}

mod inner {
    use std::cell::UnsafeCell;
    use std::mem::MaybeUninit;
    use std::sync::atomic::{self, AtomicU16};

    use super::*;

    // NOTE: flag related stuff is private, height in super module always has upper bits unset
    const TOP: u16 = 0b_1000 << 12;
    const INIT_STARTED: u16 = 0b_0100 << 12;
    const INIT_COMPLETED: u16 = INIT_STARTED | (0b_0010 << 12);
    const COMMITTED: u16 = 0b_0001 << 12;
    const ALL_FLAGS: u16 = TOP | INIT_COMPLETED | COMMITTED;

    pub struct CertInner {
        /// * `0`: current point is not certified (default)
        /// * `1`: current point belongs to a certified subtree, but its direct deps aren't
        /// * `2`: current point and its direct `includes` are certified
        /// * `3`: direct `includes` and `witness` are certified, and also `includes` of `includes`
        /// * ... (_happy cycling_)
        /// * `N+1`: topmost certified point, with 1..=N rounds certified, with proving point at N+2
        height: AtomicU16,
        direct_deps: UnsafeCell<MaybeUninit<CertDirectDeps>>,
    }

    unsafe impl Sync for CertInner {}

    impl Default for CertInner {
        fn default() -> Self {
            Self {
                height: AtomicU16::default(),
                direct_deps: UnsafeCell::new(MaybeUninit::uninit()),
            }
        }
    }

    impl CertInner {
        pub fn set_committed(&self) {
            let height = (self.height).fetch_or(COMMITTED, atomic::Ordering::Relaxed);
            assert_ne!(height & !ALL_FLAGS, 0, "not certified by anchor: {height}");
            assert_eq!(height & COMMITTED, 0, "committed twice: {height}");
        }

        pub fn is_committed(&self) -> bool {
            let height = self.height.load(atomic::Ordering::Relaxed);
            height & COMMITTED == COMMITTED
        }

        /// Option contains `true` only if the carrier was directly certified
        pub fn certified(&self) -> Option<bool> {
            let height = self.height.load(atomic::Ordering::Relaxed);
            (height & !ALL_FLAGS != 0).then_some(height & TOP == TOP)
        }

        pub fn set_deps(&self, direct_deps: CertDirectDeps) -> Option<(CertDirectRefs<'_>, u16)> {
            let mut height = (self.height).fetch_or(INIT_STARTED, atomic::Ordering::Relaxed);
            assert_eq!(height & INIT_STARTED, 0, "must set deps only once");

            // SAFETY: we write deps only once, otherwise we panic; then we set INIT_COMPLETED
            let deps = unsafe {
                let was_uninit = &mut *self.direct_deps.get();
                was_uninit.write(direct_deps);
                was_uninit.assume_init_ref()
            };

            height = (self.height).fetch_or(INIT_COMPLETED, atomic::Ordering::Release);
            height &= !ALL_FLAGS; // unmask all because of max height bit

            if height > 1 {
                let refs = CertDirectRefs {
                    includes: &deps.includes,
                    witness: (height > 2).then_some(&deps.witness),
                };
                Some((refs, height))
            } else {
                None
            }
        }

        /// `top` is set for max possible height by config (point has direct proof by next one)
        ///
        /// returns `Some` if the new height is greater (new proof is closer), so must continue traverse
        pub fn certify_deeper(&self, height: u16, is_top: bool) -> Option<CertDirectRefs<'_>> {
            let top_mask = if is_top { TOP } else { 0 };
            if self.max_height_published(height, top_mask) && height > 1 {
                // SAFETY: INIT_COMPLETED is published after deps are init
                let deps = unsafe { (&*self.direct_deps.get()).assume_init_ref() };
                Some(CertDirectRefs {
                    includes: &deps.includes,
                    witness: (height > 2).then_some(&deps.witness),
                })
            } else {
                None
            }
        }

        fn max_height_published(&self, mut height: u16, top_mask: u16) -> bool {
            self.height
                .fetch_update(
                    atomic::Ordering::Relaxed, // store: flags remain unchanged
                    atomic::Ordering::Acquire, // load: synchronizes-with `set_deps()`
                    |stored| {
                        height |= stored & ALL_FLAGS; // inherit flags
                        // top bit is set only once, when height is greater than any other
                        (height > stored).then_some(height | top_mask) // keep max
                    },
                )
                // is_ok: traverse only if current height is greater, don't repeat work already done
                .is_ok_and(|prev| prev & INIT_COMPLETED == INIT_COMPLETED) // SAFETY: read only init
        }
    }

    impl Drop for CertInner {
        fn drop(&mut self) {
            if self.height.load(atomic::Ordering::Acquire) & INIT_COMPLETED == INIT_COMPLETED {
                // SAFETY: INIT is completed, contents are published; we're the only owner
                unsafe { self.direct_deps.get_mut().assume_init_drop() }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::default_test_config;

    const PEERS: usize = 3;

    const ROUNDS: usize = 7;

    type ArrType = [[(Digest, Cert); PEERS]; ROUNDS];

    #[test]
    fn cert_after_set_deps() {
        let conf = conf();
        let cert_rounds = conf.consensus.commit_history_rounds.get() as usize;

        let arr = make_arr();
        // all certs have all dependencies prefilled
        for r in 0..ROUNDS {
            for (_, cert) in &arr[r] {
                cert.set_deps(deps_from(&arr[..r]));
            }
        }

        // certify one from top round
        arr[ROUNDS - 1][0].1.certify(&conf);
        assert_eq!(any_certified(&arr).len(), cert_rounds + 1);
        assert_eq!(all_certified(&arr).len(), cert_rounds);

        // certify one round deeper
        arr[ROUNDS - 2][0].1.certify(&conf);
        assert_eq!(any_certified(&arr).len(), cert_rounds + 2);
        assert_eq!(all_certified(&arr).len(), cert_rounds + 1);
    }

    #[test]
    fn set_deps_after_cert() {
        let conf = conf();
        let cert_rounds = conf.consensus.commit_history_rounds.get() as usize;

        let arr = make_arr();

        // certify one not at the top
        arr[ROUNDS - 2][0].1.certify(&conf);

        // fill deps up to certified one (inclusive)
        for r in 0..=ROUNDS - 2 {
            for (_, cert) in &arr[r] {
                cert.set_deps(deps_from(&arr[..r]));
            }
        }

        // the same as if deps were set before certified, like in the other test
        assert_eq!(any_certified(&arr).len(), cert_rounds + 1);
        assert_eq!(all_certified(&arr).len(), cert_rounds);

        // certify one a round above
        let (_, one_cert_above) = &arr[ROUNDS - 1][0];
        one_cert_above.certify(&conf);

        assert_eq!(any_certified(&arr).len(), cert_rounds + 2); // one new certified
        assert_eq!(all_certified(&arr).len(), cert_rounds); // unchanged: no deps yet

        one_cert_above.set_deps(deps_from(&arr[..ROUNDS - 1]));

        // the same as if we certified in decreasing order, like in the other test
        assert_eq!(any_certified(&arr).len(), cert_rounds + 2);
        assert_eq!(all_certified(&arr).len(), cert_rounds + 1);
    }

    fn conf() -> MempoolConfig {
        let mut conf = default_test_config().conf;
        // leave some rounds uncertified
        conf.consensus.commit_history_rounds = (ROUNDS as u8 - 3).try_into().unwrap();
        conf
    }

    fn make_arr() -> ArrType {
        std::array::from_fn(|_| std::array::from_fn(|_| (Digest::random(), Cert::default())))
    }

    /// last slice entry MUST be the direct includes for the certificate
    fn deps_from(slice: &[[(Digest, Cert); PEERS]]) -> CertDirectDeps {
        let data = |shift: usize| {
            slice
                .len()
                .checked_sub(shift + 1) // +1 because of len
                .and_then(|i| slice.get(i))
                .into_iter()
                .flatten()
                .map(|(digest, cert)| (*digest, cert.downgrade()))
                .collect()
        };
        CertDirectDeps {
            includes: data(0), // last array in slice
            witness: data(1),  // one to last in slice
        }
    }

    fn any_certified(arr: &ArrType) -> Vec<usize> {
        arr.iter()
            .enumerate()
            .filter(|(_, d_c)| d_c.iter().any(|(_, cert)| cert.is_certified()))
            .map(|(i, _)| i)
            .collect::<Vec<_>>()
    }

    fn all_certified(arr: &ArrType) -> Vec<usize> {
        arr.iter()
            .enumerate()
            .filter(|(_, d_c)| d_c.iter().all(|(_, cert)| cert.is_certified()))
            .map(|(i, _)| i)
            .collect::<Vec<_>>()
    }
}
