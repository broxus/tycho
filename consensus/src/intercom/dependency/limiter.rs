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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enter)),
            file!(),
            line!(),
        );
        let semaphore_opt = {
            let mut inner = self.0.lock();
            let bypass = inner.inflight_bypassed <= conf.consensus.download_tasks;
            let result = if bypass {
                tracing::trace!("{round:?} bypass");
                inner.inflight_bypassed += 1;
                None
            } else {
                let waiter = inner.waiters.entry(round).or_insert_with(|| Waiter {
                    semaphore: Arc::new(Semaphore::new(0)),
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
            match {
                __guard.end_section(line!());
                let __result = waiter.semaphore.acquire().await;
                __guard.start_section(line!());
                __result
            } {
                Ok(permit) => {
                    tracing::trace!("{round:?} semaphore acquire, pos {}", waiter.inflight);
                    permit.forget();
                }
                Err(_) => {
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
                    entry.remove();
                }
                Some(decreased) => waiter.inflight = decreased,
                None => panic!("limiter inflight counter for round {} underflow", key.0),
            }
        } else {
            tracing::trace!("{round:?} bypass release, left {}", inner.inflight_bypassed);
            match inner.inflight_bypassed.checked_sub(1) {
                Some(decreased) => inner.inflight_bypassed = decreased,
                None => panic!("limiter bypass counter underflow for round {}", round.0),
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
    use crate::test_utils::{default_test_config, test_logger};
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn order_many() -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(order_many)),
            file!(),
            line!(),
        );
        test_logger::spans("order", "info");
        test_logger::set_print_panic_hook(true);
        let instant = Instant::now();
        let futures = FuturesUnordered::new();
        const RUNS: u32 = 1;
        for _ in 0..RUNS {
            futures.push(order());
        }
        let result = {
            __guard.end_section(line!());
            let __result = futures.collect::<Result<Vec<()>>>().await;
            __guard.start_section(line!());
            __result
        };
        tracing::info!(
            "{RUNS} run(s) took {}",
            humantime::format_duration(instant.elapsed())
        );
        result?;
        Ok(())
    }
    async fn order() -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(order)),
            file!(),
            line!(),
        );
        let mut conf = default_test_config().conf;
        conf.consensus.download_tasks = 0;
        let limiter = Arc::new(Limiter::default());
        const ITEMS: usize = 200;
        let values: Vec<u32> = (0..ITEMS)
            .map(|i| i as u32)
            .sorted_by_cached_key(|_| rand::rng().next_u64())
            .collect();
        let spawned = FuturesUnordered::new();
        let mut results: Vec<u32> = Vec::with_capacity(ITEMS);
        let all_spawned = Arc::new(Barrier::new(ITEMS));
        let (sender, mut receiver) = mpsc::channel(ITEMS);
        for i in values {
            let all_spawned = all_spawned.clone();
            let limiter = limiter.clone();
            let conf = conf.clone();
            let sender = sender.clone();
            spawned.push(tokio::spawn(async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    line!(),
                );
                {
                    __guard.end_section(line!());
                    let __result = all_spawned.wait().await;
                    __guard.start_section(line!());
                    __result
                };
                let _guard = {
                    __guard.end_section(line!());
                    let __result = limiter.enter(Round(i), &conf).await;
                    __guard.start_section(line!());
                    __result
                };
                tracing::debug!("{i} guard entered");
                sender.try_send(i)?;
                {
                    __guard.end_section(line!());
                    let __result = tokio::time::sleep(Duration::from_millis(5)).await;
                    __guard.start_section(line!());
                    __result
                };
                Ok(())
            }));
        }
        {
            __guard.end_section(line!());
            let __result = spawned.try_collect::<Vec<Result<()>>>().await;
            __guard.start_section(line!());
            __result
        }?
        .into_iter()
        .collect::<Result<Vec<()>>>()?;
        {
            __guard.end_section(line!());
            let __result = receiver.recv_many(&mut results, ITEMS).await;
            __guard.start_section(line!());
            __result
        };
        ensure_roundtrip(limiter)?;
        let bad_order = results
            .iter()
            .tuple_windows()
            .skip(1)
            .filter(|(a, b)| a < b)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            bad_order.is_empty(),
            "all (except first) resolved must be in DESC order: \
             also found {bad_order:?} in all {results:?}",
        );
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn liveness() -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(liveness)),
            file!(),
            line!(),
        );
        test_logger::spans("liveness", "info");
        test_logger::set_print_panic_hook(true);
        let mut conf = default_test_config().conf;
        conf.consensus.download_tasks = 37;
        let extra = 300;
        let sleep_duration = Duration::from_millis(10);
        let limiter = Arc::new(Limiter::default());
        let mut futs = Vec::new();
        let values = (0..conf.consensus.download_tasks as u32 + extra)
            .sorted_by_cached_key(|_| rand::rng().next_u32())
            .collect::<Vec<_>>();
        let timer = Instant::now();
        for i in values.clone() {
            let limiter = limiter.clone();
            let conf = conf.clone();
            futs.push(tokio::spawn(async move {
                let mut __guard = crate::__async_profile_guard__::Guard::new(
                    concat!(module_path!(), "::async_block"),
                    file!(),
                    line!(),
                );
                let _guard = {
                    __guard.end_section(line!());
                    let __result = limiter.enter(Round(i / 5), &conf).await;
                    __guard.start_section(line!());
                    __result
                };
                tracing::debug!("{i} guard entered");
                {
                    __guard.end_section(line!());
                    let __result = tokio::time::sleep(sleep_duration).await;
                    __guard.start_section(line!());
                    __result
                };
                i
            }));
        }
        {
            __guard.end_section(line!());
            let __result = future::join_all(futs).await;
            __guard.start_section(line!());
            __result
        }
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
