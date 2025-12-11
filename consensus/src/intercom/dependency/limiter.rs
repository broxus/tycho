use std::collections::{BTreeMap, btree_map};
use std::num::NonZeroU16;
use std::sync::Arc;

use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use tokio::sync::{Semaphore, TryAcquireError};

use crate::models::Round;

pub struct Limiter(Mutex<LimiterInner>);

#[derive(Debug)]
struct LimiterInner {
    permits: u16,
    waiters: BTreeMap<Round, Waiter>,
}

#[derive(Clone, Debug)]
struct Waiter {
    semaphore: Arc<Semaphore>,
    inflight: usize,
}

impl Limiter {
    pub fn new(permits: NonZeroU16) -> Self {
        Self(Mutex::new(LimiterInner {
            permits: permits.get(),
            waiters: BTreeMap::default(),
        }))
    }

    #[must_use]
    pub async fn enter(&self, round: Round) -> LimiterGuard<'_> {
        let maybe_wait = {
            let mut inner = self.lock();
            inner.get_waiter(round)
        };
        if let Some(wait) = maybe_wait {
            let on_cancel = || self.lock().cancel_inflight(round, &wait.semaphore);
            wait.acquire(round, on_cancel).await;
        }
        LimiterGuard { limiter: self }
    }

    fn lock(&self) -> MutexGuard<'_, RawMutex, LimiterInner> {
        self.0.lock()
    }
}

impl LimiterInner {
    fn get_waiter(&mut self, round: Round) -> Option<Waiter> {
        // permits cannot be zero: at least one is always allowed, others are concurrent to it
        if self.permits > 0 {
            self.permits -= 1;
            tracing::trace!("{round:?} permits bypass");
            None
        } else {
            let waiter = self.waiters.entry(round).or_insert_with(|| Waiter {
                semaphore: Arc::new(Semaphore::new(0)), // create locked
                inflight: 0,
            });
            waiter.inflight += 1;
            tracing::trace!("{round:?} semaphore get, pos {}", waiter.inflight);
            Some(waiter.clone())
        }
    }

    /// permit was not acquired, but may have been granted just before a wait was cancelled
    fn cancel_inflight(&mut self, round: Round, semaphore: &Arc<Semaphore>) {
        let mut has_free_permit = false;
        match self.waiters.entry(round) {
            btree_map::Entry::Occupied(mut entry) => {
                let waiter = entry.get_mut();
                if Arc::ptr_eq(&waiter.semaphore, semaphore) {
                    // if there's a permit, then our task was granted under lock and then cancelled
                    has_free_permit = semaphore.forget_permits(1) == 1;
                } else {
                    // ABA: our entry was removed after a chain of some `Self::exit()`,
                    // so our semaphore was granted a permit for each inflight task;
                    // our task is cancelled and our permit is in the semaphore,
                    // maybe with some other cancelled task permits;
                    // it's fair to pass our permit to other waiter, there is at least one waiting
                    let drained = semaphore.forget_permits(1);
                    assert_eq!(drained, 1, "must have been granted 1 permit under lock");
                    waiter.semaphore.add_permits(drained);
                }
                // if `exit()` will not be called, reduce inflight manually
                if !has_free_permit {
                    match waiter.inflight.checked_sub(1) {
                        Some(0) => {
                            entry.remove(); // it was the last one
                        }
                        Some(decreased) => waiter.inflight = decreased,
                        None => panic!("{round:?} limiter entry must have been removed"),
                    }
                }
            }
            btree_map::Entry::Vacant(_) => {
                // some `Self::exit()` removed the last entry for us, granting 1 permit under lock;
                // `Semaphore::acquire()` is cancel-safe, so it returned permits to the semaphore
                has_free_permit = semaphore.forget_permits(1) == 1;
                assert!(has_free_permit, "last inflight must have 1 permit");
            }
        }
        if has_free_permit {
            self.exit(); // redistribute keeping inflight counter right
        }
    }

    fn exit(&mut self) {
        if let Some(mut entry) = self.waiters.last_entry() {
            let round = *entry.key();
            let waiter = entry.get_mut();
            tracing::trace!("{round:?} permit grant, left {}", waiter.inflight);
            waiter.semaphore.add_permits(1);
            match waiter.inflight.checked_sub(1) {
                Some(0) => {
                    entry.remove(); // it was the last one
                }
                Some(decreased) => waiter.inflight = decreased,
                None => panic!("{round:?} limiter entry must have been removed"),
            }
        } else {
            tracing::trace!("free permit released, left {}", self.permits);
            match self.permits.checked_add(1) {
                Some(increased) => self.permits = increased,
                None => panic!("limiter permits counter overflow"),
            }
        }
    }
}

impl Waiter {
    async fn acquire<F: FnOnce()>(&self, round: Round, on_cancel: F) {
        let permit_or_closed = match self.semaphore.try_acquire() {
            Ok(permit) => Some(permit),
            Err(TryAcquireError::NoPermits) => {
                struct CancelGuard<F: FnOnce()>(Option<F>);
                impl<F: FnOnce()> Drop for CancelGuard<F> {
                    fn drop(&mut self) {
                        if let Some(on_cancel) = self.0.take() {
                            on_cancel();
                        }
                    }
                }
                let mut cancel_guard = CancelGuard(Some(on_cancel));
                let permit_or_closed = self.semaphore.acquire().await.ok();
                cancel_guard.0 = None;
                permit_or_closed
            }
            Err(TryAcquireError::Closed) => None,
        };

        if let Some(permit) = permit_or_closed {
            tracing::trace!("{round:?} semaphore acquire, pos {}", self.inflight);
            permit.forget(); // may add permit to another round or to bypassed on drop
        } else {
            // semaphore drop may follow last permit
            let inflight = self.inflight;
            tracing::trace!("{round:?} semaphore dropped before acquire, pos {inflight}");
        }
    }
}

pub struct LimiterGuard<'a> {
    limiter: &'a Limiter,
}

impl Drop for LimiterGuard<'_> {
    fn drop(&mut self) {
        self.limiter.lock().exit();
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use anyhow::Result;
    use futures_util::stream::FuturesUnordered;
    use futures_util::{TryStreamExt, future};
    use itertools::Itertools;
    use rand::RngCore;
    use tokio::sync::{Barrier, mpsc};
    use tokio_stream::StreamExt;

    use super::*;
    use crate::test_utils::test_logger;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn order_many() -> Result<()> {
        test_logger::spans("order", "info");
        test_logger::set_print_panic_hook(true);

        let instant = Instant::now();

        let futures = FuturesUnordered::new();

        const RUNS: u32 = 1;
        for _ in 0..RUNS {
            futures.push(order());
        }

        let result = futures.collect::<Result<Vec<()>>>().await;

        tracing::info!(
            "{RUNS} run(s) took {}",
            humantime::format_duration(instant.elapsed())
        );

        result?;
        Ok(())
    }

    async fn order() -> Result<()> {
        const PERMITS: NonZeroU16 = NonZeroU16::MIN; // Note feature of this test: sequential execution

        let limiter = Arc::new(Limiter::new(PERMITS));

        const ITEMS: usize = 200;

        let values: Vec<u32> = (0..ITEMS)
            .map(|i| i as u32)
            .sorted_by_cached_key(|_| rand::rng().next_u64())
            .collect();

        let spawned = FuturesUnordered::new();

        let mut results: Vec<u32> = Vec::with_capacity(ITEMS);

        // without barrier the 1st spawned future may resolve earlier than 2nd is spawned,
        // so 2nd will be out of order too
        let all_spawned = Arc::new(Barrier::new(ITEMS));

        let (sender, mut receiver) = mpsc::channel(ITEMS);

        for i in values {
            let all_spawned = all_spawned.clone();
            let limiter = limiter.clone();
            let sender = sender.clone();
            spawned.push(tokio::spawn(async move {
                all_spawned.wait().await;
                let _guard = limiter.enter(Round(i)).await;
                tracing::debug!("{i} guard entered");
                // no ctx switch here
                sender.try_send(i)?;
                // immediately freed guard will make limiter queue empty at random moments;
                // tokio on CI will reorder tasks when overloaded so will need to sleep more
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok(())
            }));
        }

        spawned
            .try_collect::<Vec<Result<()>>>()
            .await?
            .into_iter()
            .collect::<Result<Vec<()>>>()?;

        receiver.recv_many(&mut results, ITEMS).await;

        ensure_roundtrip(limiter, PERMITS)?;

        let bad_order = results
            .iter()
            .tuple_windows()
            .skip(1)
            .filter(|(a, b)| a < b)
            .collect::<Vec<_>>();

        // first resolved may be in order of either rounds or spawns
        anyhow::ensure!(
            bad_order.is_empty(),
            "all (except first) resolved must be in DESC order: \
             also found {bad_order:?} in all {results:?}",
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn liveness() -> Result<()> {
        test_logger::spans("liveness", "info");
        test_logger::set_print_panic_hook(true);

        const PERMITS: NonZeroU16 = NonZeroU16::new(37).unwrap();
        let extra = 300;
        let sleep_duration = Duration::from_millis(10);

        let limiter = Arc::new(Limiter::new(PERMITS));

        let mut futs = Vec::new();

        let values = (0..PERMITS.get() as u32 + extra)
            .sorted_by_cached_key(|_| rand::rng().next_u32())
            .collect::<Vec<_>>();

        let timer = Instant::now();

        for i in values.clone() {
            let limiter = limiter.clone();
            futs.push(tokio::spawn(async move {
                let _guard = limiter.enter(Round(i / 5)).await;
                tracing::debug!("{i} guard entered");
                tokio::time::sleep(sleep_duration).await;
                i
            }));
        }

        future::join_all(futs)
            .await
            .into_iter()
            .map(|result| result.map_err(anyhow::Error::from))
            .collect::<Result<Vec<_>>>()?;

        let elapsed = timer.elapsed();

        let expected_duration = sleep_duration * (extra + 1) / 2;

        tracing::info!(
            "elapsed {} expected {}",
            humantime::format_duration(elapsed),
            humantime::format_duration(expected_duration),
        );

        ensure_roundtrip(limiter, PERMITS)?;

        anyhow::ensure!(elapsed < expected_duration);

        Ok(())
    }

    fn ensure_roundtrip(limiter: Arc<Limiter>, permits: NonZeroU16) -> Result<()> {
        let limiter = Arc::into_inner(limiter).expect("must be last ref");
        let inner = limiter.lock();
        anyhow::ensure!(
            inner.permits == permits.get(),
            "all bypass permit credits must be returned, got {}",
            inner.permits
        );

        anyhow::ensure!(
            inner.waiters.is_empty(),
            "all waiters must be removed: {:?}",
            &*inner
        );
        Ok(())
    }
}
