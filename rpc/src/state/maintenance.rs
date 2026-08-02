use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MaintenancePriority {
    Sealing,
    Background,
}

#[derive(Clone)]
pub(super) struct MaintenanceCoordinator {
    inner: Arc<CoordinatorInner>,
}

impl MaintenanceCoordinator {
    pub(super) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "maintenance coordinator capacity must be positive");
        Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState {
                    capacity,
                    ..Default::default()
                }),
            }),
        }
    }

    pub(super) async fn acquire(
        &self,
        priority: MaintenancePriority,
    ) -> Result<MaintenancePermit, MaintenanceAcquireError> {
        let (sender, receiver) = oneshot::channel();
        let ticket = {
            let mut state = self.inner.state.lock();
            if state.closed {
                return Err(MaintenanceAcquireError::Closed);
            }
            let ticket = state.next_ticket;
            state.next_ticket = state
                .next_ticket
                .checked_add(1)
                .expect("maintenance waiter ticket overflow");
            let waiter = MaintenanceWaiter {
                ticket,
                sender,
                overtaken: false,
            };
            match priority {
                MaintenancePriority::Sealing => state.sealing.push_back(waiter),
                MaintenancePriority::Background => state.background.push_back(waiter),
            }
            dispatch_waiters(&mut state);
            ticket
        };
        let mut registration = MaintenanceWaiterRegistration {
            inner: self.inner.clone(),
            ticket,
            armed: true,
        };
        receiver.await.map_err(|_| MaintenanceAcquireError::Closed)?;
        {
            let mut state = self.inner.state.lock();
            assert!(state.granted.remove(&ticket), "maintenance grant was lost before acquisition");
        }
        registration.armed = false;
        Ok(MaintenancePermit {
            inner: Some(self.inner.clone()),
        })
    }

    pub(super) fn close(&self) {
        let mut state = self.inner.state.lock();
        if state.closed {
            return;
        }
        state.closed = true;
        state.sealing.clear();
        state.background.clear();
    }

    #[cfg(test)]
    fn snapshot(&self) -> CoordinatorSnapshot {
        let state = self.inner.state.lock();
        CoordinatorSnapshot {
            in_use: state.in_use,
            sealing_waiters: state.sealing.len(),
            background_waiters: state.background.len(),
            closed: state.closed,
        }
    }

    #[cfg(test)]
    pub(super) fn background_waiters(&self) -> usize {
        self.inner.state.lock().background.len()
    }
}

struct CoordinatorInner {
    state: Mutex<CoordinatorState>,
}

#[derive(Default)]
struct CoordinatorState {
    capacity: usize,
    in_use: usize,
    next_ticket: u64,
    closed: bool,
    sealing: VecDeque<MaintenanceWaiter>,
    background: VecDeque<MaintenanceWaiter>,
    granted: BTreeSet<u64>,
}

struct MaintenanceWaiter {
    ticket: u64,
    sender: oneshot::Sender<()>,
    overtaken: bool,
}

fn dispatch_waiters(state: &mut CoordinatorState) {
    while !state.closed && state.in_use < state.capacity {
        let waiter = match (state.sealing.front(), state.background.front()) {
            (Some(_), Some(background)) if background.overtaken => state.background.pop_front(),
            (Some(_), Some(_)) => {
                state.background.front_mut().unwrap().overtaken = true;
                state.sealing.pop_front()
            }
            (Some(_), None) => state.sealing.pop_front(),
            (None, Some(_)) => state.background.pop_front(),
            (None, None) => break,
        };
        let Some(waiter) = waiter else {
            break;
        };
        if waiter.sender.send(()).is_ok() {
            state.in_use += 1;
            state.granted.insert(waiter.ticket);
        }
    }
}

struct MaintenanceWaiterRegistration {
    inner: Arc<CoordinatorInner>,
    ticket: u64,
    armed: bool,
}

impl Drop for MaintenanceWaiterRegistration {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self.inner.state.lock();
        state.sealing.retain(|waiter| waiter.ticket != self.ticket);
        state.background.retain(|waiter| waiter.ticket != self.ticket);
        if state.granted.remove(&self.ticket) {
            state.in_use = state.in_use.checked_sub(1).expect("maintenance grant count underflow");
        }
        dispatch_waiters(&mut state);
    }
}

pub(super) struct MaintenancePermit {
    inner: Option<Arc<CoordinatorInner>>,
}

impl MaintenancePermit {
    /// Requeues this active permit at background priority without leaving an untracked slot.
    pub(super) fn yield_background_blocking(&mut self) -> Result<(), MaintenanceAcquireError> {
        let Some(inner) = self.inner.take() else {
            return Err(MaintenanceAcquireError::Closed);
        };
        let (sender, receiver) = oneshot::channel();
        let ticket = {
            let mut state = inner.state.lock();
            state.in_use = state.in_use.checked_sub(1).expect("maintenance permit count underflow");
            if state.closed {
                return Err(MaintenanceAcquireError::Closed);
            }
            let ticket = state.next_ticket;
            state.next_ticket = state
                .next_ticket
                .checked_add(1)
                .expect("maintenance waiter ticket overflow");
            state.background.push_back(MaintenanceWaiter {
                ticket,
                sender,
                overtaken: false,
            });
            dispatch_waiters(&mut state);
            ticket
        };
        let mut registration = MaintenanceWaiterRegistration {
            inner: inner.clone(),
            ticket,
            armed: true,
        };
        receiver.blocking_recv().map_err(|_| MaintenanceAcquireError::Closed)?;
        {
            let mut state = inner.state.lock();
            assert!(state.granted.remove(&ticket), "maintenance grant was lost before acquisition");
        }
        registration.armed = false;
        self.inner = Some(inner);
        Ok(())
    }
}

impl Drop for MaintenancePermit {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut state = inner.state.lock();
        state.in_use = state.in_use.checked_sub(1).expect("maintenance permit count underflow");
        dispatch_waiters(&mut state);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(super) enum MaintenanceAcquireError {
    #[error("maintenance coordinator is closed")]
    Closed,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CoordinatorSnapshot {
    in_use: usize,
    sealing_waiters: usize,
    background_waiters: usize,
    closed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};

    async fn wait_for_state(
        coordinator: &MaintenanceCoordinator,
        expected: impl Fn(CoordinatorSnapshot) -> bool,
    ) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if expected(coordinator.snapshot()) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("maintenance coordinator did not reach the expected state");
    }

    fn spawn_waiter(
        coordinator: MaintenanceCoordinator,
        priority: MaintenancePriority,
        name: &'static str,
        acquired: mpsc::UnboundedSender<&'static str>,
    ) -> (tokio::task::JoinHandle<Result<(), MaintenanceAcquireError>>, oneshot::Sender<()>) {
        let (release, released) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _permit = coordinator.acquire(priority).await?;
            acquired.send(name).unwrap();
            let _ = released.await;
            Ok(())
        });
        (task, release)
    }

    #[tokio::test]
    async fn coordinator_limits_concurrency_and_preserves_fifo() {
        let coordinator = MaintenanceCoordinator::new(2);
        let first = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        let second = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        assert_eq!(coordinator.snapshot().in_use, 2);
        let (acquired, mut acquisitions) = mpsc::unbounded_channel();
        let (third, release_third) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Background,
            "third",
            acquired.clone(),
        );
        wait_for_state(&coordinator, |state| state.background_waiters == 1).await;
        let (fourth, release_fourth) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Background,
            "fourth",
            acquired,
        );
        wait_for_state(&coordinator, |state| state.background_waiters == 2).await;
        drop(first);
        assert_eq!(acquisitions.recv().await, Some("third"));
        drop(second);
        assert_eq!(acquisitions.recv().await, Some("fourth"));
        let _ = release_third.send(());
        let _ = release_fourth.send(());
        third.await.unwrap().unwrap();
        fourth.await.unwrap().unwrap();
        assert_eq!(coordinator.snapshot().in_use, 0);
    }

    #[tokio::test]
    async fn sealing_preempts_once_and_background_cannot_starve() {
        let coordinator = MaintenanceCoordinator::new(1);
        let blocker = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        let (acquired, mut acquisitions) = mpsc::unbounded_channel();
        let (background, release_background) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Background,
            "background",
            acquired.clone(),
        );
        wait_for_state(&coordinator, |state| state.background_waiters == 1).await;
        let (first_sealing, release_first_sealing) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Sealing,
            "first_sealing",
            acquired.clone(),
        );
        wait_for_state(&coordinator, |state| state.sealing_waiters == 1).await;
        drop(blocker);
        assert_eq!(acquisitions.recv().await, Some("first_sealing"));
        let (second_sealing, release_second_sealing) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Sealing,
            "second_sealing",
            acquired,
        );
        wait_for_state(&coordinator, |state| state.sealing_waiters == 1).await;
        let _ = release_first_sealing.send(());
        assert_eq!(acquisitions.recv().await, Some("background"));
        let _ = release_background.send(());
        assert_eq!(acquisitions.recv().await, Some("second_sealing"));
        let _ = release_second_sealing.send(());
        first_sealing.await.unwrap().unwrap();
        background.await.unwrap().unwrap();
        second_sealing.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn sealing_waiters_preserve_fifo_order() {
        let coordinator = MaintenanceCoordinator::new(1);
        let blocker = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        let (acquired, mut acquisitions) = mpsc::unbounded_channel();
        let (first, release_first) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Sealing,
            "first",
            acquired.clone(),
        );
        wait_for_state(&coordinator, |state| state.sealing_waiters == 1).await;
        let (second, release_second) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Sealing,
            "second",
            acquired,
        );
        wait_for_state(&coordinator, |state| state.sealing_waiters == 2).await;
        drop(blocker);
        assert_eq!(acquisitions.recv().await, Some("first"));
        let _ = release_first.send(());
        assert_eq!(acquisitions.recv().await, Some("second"));
        let _ = release_second.send(());
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn mixed_priorities_fill_capacity_without_starving_background() {
        let coordinator = MaintenanceCoordinator::new(2);
        let first_blocker = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        let second_blocker = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        let (acquired, mut acquisitions) = mpsc::unbounded_channel();
        let (background, release_background) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Background,
            "background",
            acquired.clone(),
        );
        wait_for_state(&coordinator, |state| state.background_waiters == 1).await;
        let (first_sealing, release_first_sealing) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Sealing,
            "first_sealing",
            acquired.clone(),
        );
        wait_for_state(&coordinator, |state| state.sealing_waiters == 1).await;
        let (second_sealing, release_second_sealing) = spawn_waiter(
            coordinator.clone(),
            MaintenancePriority::Sealing,
            "second_sealing",
            acquired,
        );
        wait_for_state(&coordinator, |state| state.sealing_waiters == 2).await;
        drop(first_blocker);
        assert_eq!(acquisitions.recv().await, Some("first_sealing"));
        drop(second_blocker);
        assert_eq!(acquisitions.recv().await, Some("background"));
        assert_eq!(coordinator.snapshot().sealing_waiters, 1);
        let _ = release_first_sealing.send(());
        assert_eq!(acquisitions.recv().await, Some("second_sealing"));
        let _ = release_background.send(());
        let _ = release_second_sealing.send(());
        first_sealing.await.unwrap().unwrap();
        background.await.unwrap().unwrap();
        second_sealing.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_dispatched_grant_reclaims_the_slot() {
        let coordinator = MaintenanceCoordinator::new(1);
        let blocker = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        let mut pending = Box::pin(coordinator.acquire(MaintenancePriority::Sealing));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(coordinator.snapshot().sealing_waiters, 1);
        drop(blocker);
        assert_eq!(coordinator.snapshot().in_use, 1);
        assert_eq!(coordinator.snapshot().sealing_waiters, 0);
        drop(pending);
        assert_eq!(coordinator.snapshot().in_use, 0);
    }

    #[tokio::test]
    async fn close_after_an_unpolled_grant_preserves_reclamation() {
        let coordinator = MaintenanceCoordinator::new(1);
        let blocker = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        let mut pending = Box::pin(coordinator.acquire(MaintenancePriority::Background));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
        drop(blocker);
        assert_eq!(coordinator.snapshot().in_use, 1);
        coordinator.close();
        assert!(coordinator.snapshot().closed);
        drop(pending);
        assert_eq!(coordinator.snapshot().in_use, 0);
    }

    #[tokio::test]
    async fn cancellation_close_and_panics_do_not_leak_permits() {
        let coordinator = MaintenanceCoordinator::new(1);
        let blocker = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        let cancelled = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                let _permit = coordinator.acquire(MaintenancePriority::Sealing).await.unwrap();
            })
        };
        wait_for_state(&coordinator, |state| state.sealing_waiters == 1).await;
        cancelled.abort();
        let _ = cancelled.await;
        wait_for_state(&coordinator, |state| state.sealing_waiters == 0).await;
        drop(blocker);
        assert_eq!(coordinator.snapshot().in_use, 0);

        let panicking = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                let _permit = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
                panic!("injected maintenance panic");
            })
        };
        assert!(panicking.await.unwrap_err().is_panic());
        assert_eq!(coordinator.snapshot().in_use, 0);

        let active = coordinator.acquire(MaintenancePriority::Background).await.unwrap();
        let closed = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move { coordinator.acquire(MaintenancePriority::Background).await })
        };
        wait_for_state(&coordinator, |state| state.background_waiters == 1).await;
        coordinator.close();
        assert!(coordinator.snapshot().closed);
        assert!(matches!(closed.await.unwrap(), Err(MaintenanceAcquireError::Closed)));
        drop(active);
        assert_eq!(coordinator.snapshot().in_use, 0);
        assert!(matches!(
            coordinator.acquire(MaintenancePriority::Sealing).await,
            Err(MaintenanceAcquireError::Closed)
        ));
    }
}
