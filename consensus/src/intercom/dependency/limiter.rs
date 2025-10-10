use std::collections::BTreeMap;
use std::num::NonZeroU16;
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
    pub fn new(permits: NonZeroU16) -> Self {
        Self(
            Mutex::new(LimiterInner {
                permits: permits.get(),
                waiters: BTreeMap::default(),
            }),
        )
    }
    #[must_use]
    pub async fn enter(&self, round: Round) -> LimiterGuard<'_> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enter)),
            file!(),
            33u32,
        );
        let round = round;
        {
            __guard.end_section(34u32);
            let __result = self.acquire(round).await;
            __guard.start_section(34u32);
            __result
        };
        LimiterGuard {
            limiter: self,
            round,
        }
    }
    async fn acquire(&self, round: Round) {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(acquire)),
            file!(),
            41u32,
        );
        let round = round;
        let semaphore_opt = {
            let mut inner = self.0.lock();
            if inner.permits.checked_sub(1).is_some() {
                tracing::trace!("{round:?} permits bypass");
                inner.permits -= 1;
                None
            } else {
                let waiter = inner
                    .waiters
                    .entry(round)
                    .or_insert_with(|| Waiter {
                        semaphore: Arc::new(Semaphore::new(0)),
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
                Err(TryAcquireError::NoPermits) => {
                    {
                        __guard.end_section(63u32);
                        let __result = waiter.semaphore.acquire().await;
                        __guard.start_section(63u32);
                        __result
                    }
                        .ok()
                }
                Err(TryAcquireError::Closed) => None,
            };
            match permit_or_closed {
                Some(permit) => {
                    tracing::trace!(
                        "{round:?} semaphore acquire, pos {}", waiter.inflight
                    );
                    permit.forget();
                }
                None => {
                    tracing::trace!(
                        "{round:?} semaphore dropped before acquire, pos {}", waiter
                        .inflight
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
                "{key:?} semaphore release from {round:?}, left {}", waiter.inflight
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
            tracing::trace!("{round:?} permits release, left {}", inner.permits);
            match inner.permits.checked_add(1) {
                Some(increased) => inner.permits = increased,
                None => panic!("limiter permits counter overflow for round {}", round.0),
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(order_many)),
            file!(),
            142u32,
        );
        test_logger::spans("order", "info");
        test_logger::set_print_panic_hook(true);
        let instant = Instant::now();
        let futures = FuturesUnordered::new();
        const RUNS: u32 = 1;
        for _ in 0..RUNS {
            __guard.checkpoint(151u32);
            futures.push(order());
        }
        let result = {
            __guard.end_section(155u32);
            let __result = futures.collect::<Result<Vec<()>>>().await;
            __guard.start_section(155u32);
            __result
        };
        tracing::info!(
            "{RUNS} run(s) took {}", humantime::format_duration(instant.elapsed())
        );
        result?;
        Ok(())
    }
    async fn order() -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(order)),
            file!(),
            166u32,
        );
        const PERMITS: NonZeroU16 = NonZeroU16::MIN;
        let limiter = Arc::new(Limiter::new(PERMITS));
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
            __guard.checkpoint(188u32);
            let all_spawned = all_spawned.clone();
            let limiter = limiter.clone();
            let sender = sender.clone();
            spawned
                .push(
                    tokio::spawn(async move {
                        let mut __guard = crate::__async_profile_guard__::Guard::new(
                            concat!(module_path!(), "::async_block"),
                            file!(),
                            192u32,
                        );
                        {
                            __guard.end_section(193u32);
                            let __result = all_spawned.wait().await;
                            __guard.start_section(193u32);
                            __result
                        };
                        let _guard = {
                            __guard.end_section(194u32);
                            let __result = limiter.enter(Round(i)).await;
                            __guard.start_section(194u32);
                            __result
                        };
                        tracing::debug!("{i} guard entered");
                        sender.try_send(i)?;
                        {
                            __guard.end_section(200u32);
                            let __result = tokio::time::sleep(Duration::from_millis(5))
                                .await;
                            __guard.start_section(200u32);
                            __result
                        };
                        Ok(())
                    }),
                );
        }
        {
            __guard.end_section(207u32);
            let __result = spawned.try_collect::<Vec<Result<()>>>().await;
            __guard.start_section(207u32);
            __result
        }?
            .into_iter()
            .collect::<Result<Vec<()>>>()?;
        {
            __guard.end_section(211u32);
            let __result = receiver.recv_many(&mut results, ITEMS).await;
            __guard.start_section(211u32);
            __result
        };
        ensure_roundtrip(limiter, PERMITS)?;
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
            233u32,
        );
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
            __guard.checkpoint(251u32);
            let limiter = limiter.clone();
            futs.push(
                tokio::spawn(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        253u32,
                    );
                    let _guard = {
                        __guard.end_section(254u32);
                        let __result = limiter.enter(Round(i / 5)).await;
                        __guard.start_section(254u32);
                        __result
                    };
                    tracing::debug!("{i} guard entered");
                    {
                        __guard.end_section(256u32);
                        let __result = tokio::time::sleep(sleep_duration).await;
                        __guard.start_section(256u32);
                        __result
                    };
                    i
                }),
            );
        }
        {
            __guard.end_section(262u32);
            let __result = future::join_all(futs).await;
            __guard.start_section(262u32);
            __result
        }
            .into_iter()
            .map(|result| result.map_err(anyhow::Error::from))
            .collect::<Result<Vec<_>>>()?;
        let elapsed = timer.elapsed();
        let expected_duration = sleep_duration * (extra + 1) / 2;
        tracing::info!(
            "elapsed {} expected {}", humantime::format_duration(elapsed),
            humantime::format_duration(expected_duration),
        );
        ensure_roundtrip(limiter, PERMITS)?;
        anyhow::ensure!(elapsed < expected_duration);
        Ok(())
    }
    fn ensure_roundtrip(limiter: Arc<Limiter>, permits: NonZeroU16) -> Result<()> {
        let limiter = Arc::into_inner(limiter).expect("must be last ref");
        let inner = limiter.0.lock();
        anyhow::ensure!(
            inner.permits == permits.get(),
            "all bypass permit credits must be returned, got {}", inner.permits
        );
        anyhow::ensure!(
            inner.waiters.is_empty(), "all waiters must be removed: {:?}", &* inner
        );
        Ok(())
    }
}
