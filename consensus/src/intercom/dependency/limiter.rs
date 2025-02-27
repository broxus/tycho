use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};
use tokio::sync::Semaphore;

use crate::engine::MempoolConfig;
use crate::models::Round;

#[derive(Default)]
pub struct Limiter(Mutex<LimiterInner>);
#[derive(Default, Debug)]
struct LimiterInner {
    inflight_bypassed: u16,
    waiters: BTreeMap<Round, Waiter>,
}

#[derive(Clone, Debug)]
struct Waiter {
    semaphore: Arc<Semaphore>,
    inflight: usize,
}

impl Limiter {
    #[must_use]
    pub async fn enter(&self, round: Round, conf: &MempoolConfig) -> LimiterGuard<'_> {
        let semaphore_opt = {
            let mut inner = self.0.lock();
            let bypass = inner.inflight_bypassed <= conf.consensus.download_tasks;
            // cannot be strict equality: at least one is always allowed, others are concurrent to it
            let result = if bypass {
                tracing::trace!("{round:?} bypass");
                inner.inflight_bypassed += 1;
                None
            } else {
                let waiter = inner.waiters.entry(round).or_insert_with(|| Waiter {
                    semaphore: Arc::new(Semaphore::new(0)), // create locked
                    inflight: 0,
                });
                waiter.inflight += 1;
                tracing::trace!("{round:?} semaphore get, pos {}", waiter.inflight);
                Some(waiter.clone())
            };
            MutexGuard::unlock_fair(inner);
            result
        };

        if let Some(waiter) = semaphore_opt {
            match waiter.semaphore.acquire().await {
                Ok(permit) => {
                    tracing::trace!("{round:?} semaphore acquire, pos {}", waiter.inflight);
                    permit.forget(); // may add permit to another round or to bypassed on drop
                }
                Err(_) => {
                    // semaphore drop may follow last permit
                    tracing::trace!(
                        "{round:?} semaphore dropped before acquire, pos {}",
                        waiter.inflight
                    );
                }
            }
        } else {
            tracing::trace!("{round:?} bypass");
        }
        LimiterGuard {
            limiter: self,
            round,
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
            tracing::trace!("{round:?} bypass release, left {}", inner.inflight_bypassed);
            match inner.inflight_bypassed.checked_sub(1) {
                Some(decreased) => inner.inflight_bypassed = decreased,
                None => {
                    // It's OK if `ConsensusConfig.download_tasks` was decreased
                    panic!("limiter bypass counter underflow for round {}", round.0)
                }
            }
        }

        MutexGuard::unlock_fair(inner);
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

#[cfg(all(test, feature = "test"))]
mod tests {
    use std::time::{Duration, Instant};

    use anyhow::Result;
    use futures_util::stream::FuturesUnordered;
    use futures_util::{future, TryStreamExt};
    use itertools::Itertools;
    use rand::RngCore;
    use tokio::sync::{mpsc, Barrier};
    use tokio_stream::StreamExt;

    use super::*;
    use crate::test_utils::{default_test_config, test_logger};

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
        let mut conf = default_test_config().conf;

        conf.consensus.download_tasks = 0; // Note feature of this test: sequential execution

        let limiter = Arc::new(Limiter::default());

        const ITEMS: usize = 200;

        let values: Vec<u32> = (0..ITEMS)
            .map(|i| i as u32)
            .sorted_by_cached_key(|_| rand::thread_rng().next_u64())
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
            let conf = conf.clone();
            let sender = sender.clone();
            spawned.push(tokio::spawn(async move {
                all_spawned.wait().await;
                let _guard = limiter.enter(Round(i), &conf).await;
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

        ensure_roundtrip(limiter)?;

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

        let mut conf = default_test_config().conf;

        conf.consensus.download_tasks = 37;
        let extra = 300;
        let sleep_duration = Duration::from_millis(10);

        let limiter = Arc::new(Limiter::default());

        let mut futs = Vec::new();

        let values = (0..conf.consensus.download_tasks as u32 + extra)
            .sorted_by_cached_key(|_| rand::thread_rng().next_u32())
            .collect::<Vec<_>>();

        let timer = Instant::now();

        for i in values.clone() {
            let limiter = limiter.clone();
            let conf = conf.clone();
            futs.push(tokio::spawn(async move {
                let _guard = limiter.enter(Round(i / 5), &conf).await;
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

        ensure_roundtrip(limiter)?;

        anyhow::ensure!(elapsed < expected_duration);

        Ok(())
    }

    fn ensure_roundtrip(limiter: Arc<Limiter>) -> Result<()> {
        let inner = limiter.0.lock();
        anyhow::ensure!(
            inner.inflight_bypassed == 0,
            "all bypass permit credits must be returned, got {}",
            inner.inflight_bypassed
        );

        anyhow::ensure!(
            inner.waiters.is_empty(),
            "all waiters must be removed: {:?}",
            &*inner
        );
        Ok(())
    }
}
