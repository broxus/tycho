use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;
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
    pub fn new(permits: u16) -> Self {
        Self(Mutex::new(LimiterInner {
            permits,
            waiters: BTreeMap::default(),
        }))
    }

    #[must_use]
    pub async fn enter(&self, round: Round) -> LimiterGuard<'_> {
        self.acquire(round).await;
        LimiterGuard {
            limiter: self,
            round,
        }
    }

    async fn acquire(&self, round: Round) {
        let semaphore_opt = {
            let mut inner = self.0.lock();
            // permits cannot be zero: at least one is always allowed, others are concurrent to it
            if inner.permits.checked_sub(1).is_some() {
                tracing::trace!("{round:?} permits bypass");
                inner.permits -= 1;
                None
            } else {
                let waiter = inner.waiters.entry(round).or_insert_with(|| Waiter {
                    semaphore: Arc::new(Semaphore::new(0)), // create locked
                    inflight: 0,
                });
                waiter.inflight += 1;
                tracing::trace!("{round:?} semaphore get, pos {}", waiter.inflight);
                Some(waiter.clone())
            }
        };

        if let Some(waiter) = semaphore_opt {
            let permit_or_closed = match waiter.semaphore.try_acquire() {
                Ok(permit) => Some(permit),
                Err(TryAcquireError::NoPermits) => waiter.semaphore.acquire().await.ok(),
                Err(TryAcquireError::Closed) => None,
            };
            match permit_or_closed {
                Some(permit) => {
                    tracing::trace!("{round:?} semaphore acquire, pos {}", waiter.inflight);
                    permit.forget(); // may add permit to another round or to bypassed on drop
                }
                None => {
                    // semaphore drop may follow last permit
                    tracing::trace!(
                        "{round:?} semaphore dropped before acquire, pos {}",
                        waiter.inflight
                    );
                }
            }
        } else {
            tracing::trace!("{round:?} permits bypass");
        }
    }

    fn exit(&self, round: Round) {
        let mut inner = self.0.lock();

        if let Some(mut entry) = inner.waiters.last_entry() {
            let key = *entry.key();
            let waiter = entry.get_mut();
            tracing::trace!(
                "{key:?} semaphore release from {round:?}, left {}",
                waiter.inflight
            );
            waiter.semaphore.add_permits(1);
            match waiter.inflight.checked_sub(1) {
                Some(0) => {
                    entry.remove(); // it was the last one
                }
                Some(decreased) => waiter.inflight = decreased,
                None => panic!("limiter inflight counter for round {} underflow", key.0),
            }
        } else {
            tracing::trace!("{round:?} permits release, left {}", inner.permits);
            match inner.permits.checked_add(1) {
                Some(increased) => inner.permits = increased,
                None => {
                    // It's OK if `ConsensusConfig.download_tasks` was decreased
                    panic!("limiter permits counter overflow for round {}", round.0)
                }
            }
        }
    }
}

pub struct LimiterGuard<'a> {
    limiter: &'a Limiter,
    round: Round,
}

impl Drop for LimiterGuard<'_> {
    fn drop(&mut self) {
        self.limiter.exit(self.round);
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
        const PERMITS: u16 = 1; // Note feature of this test: sequential execution

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

        const PERMITS: u16 = 37;
        let extra = 300;
        let sleep_duration = Duration::from_millis(10);

        let limiter = Arc::new(Limiter::new(PERMITS));

        let mut futs = Vec::new();

        let values = (0..PERMITS as u32 + extra)
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

    fn ensure_roundtrip(limiter: Arc<Limiter>, permits: u16) -> Result<()> {
        let limiter = Arc::into_inner(limiter).expect("must be last ref");
        let inner = limiter.0.lock();
        anyhow::ensure!(
            inner.permits == permits,
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
