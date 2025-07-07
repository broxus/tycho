use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use anyhow::Result;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tycho_util::futures::JoinTask;

#[derive(Clone)]
pub struct DelayedTasks {
    inner: Arc<DelayedTasksInner>,
}

impl DelayedTasks {
    pub fn new() -> (DelayedTasksSpawner, Self) {
        let inner = Arc::new(DelayedTasksInner {
            state: Mutex::new(DelayedTasksState::BeforeSpawn {
                make_fns: Vec::new(),
            }),
        });
        let handle = DelayedTasksSpawner {
            inner: inner.clone(),
        };
        (handle, Self { inner })
    }

    pub fn spawn<F, Fut>(&self, f: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let mut inner = self.inner.state.lock().unwrap();
        match &mut *inner {
            DelayedTasksState::BeforeSpawn { make_fns } => {
                make_fns.push(Box::new(move || JoinTask::new(f())));
                Ok(())
            }
            DelayedTasksState::AfterSpawn { tasks } => {
                tasks.push(JoinTask::new(f()));
                Ok(())
            }
            DelayedTasksState::Closed => anyhow::bail!("delayed tasks context closed"),
        }
    }
}

pub struct DelayedTasksSpawner {
    inner: Arc<DelayedTasksInner>,
}

impl DelayedTasksSpawner {
    pub fn spawn(self) -> DelayedTasksJoinHandle {
        {
            let mut state = self.inner.state.lock().unwrap();
            let make_fns = match &mut *state {
                DelayedTasksState::BeforeSpawn { make_fns } => std::mem::take(make_fns),
                DelayedTasksState::AfterSpawn { .. } | DelayedTasksState::Closed => {
                    unreachable!("spawn can only be called once");
                }
            };
            *state = DelayedTasksState::AfterSpawn {
                tasks: make_fns.into_iter().map(|f| f()).collect(),
            }
        };

        DelayedTasksJoinHandle { inner: self.inner }
    }
}

pub struct DelayedTasksJoinHandle {
    inner: Arc<DelayedTasksInner>,
}

impl DelayedTasksJoinHandle {
    pub async fn join(self) -> Result<()> {
        let mut tasks = {
            let mut state = self.inner.state.lock().unwrap();
            match std::mem::replace(&mut *state, DelayedTasksState::Closed) {
                DelayedTasksState::AfterSpawn { tasks } => tasks,
                DelayedTasksState::BeforeSpawn { .. } | DelayedTasksState::Closed => {
                    unreachable!("join can only be called once");
                }
            }
        };

        while let Some(res) = tasks.next().await {
            res?;
        }
        Ok(())
    }
}

struct DelayedTasksInner {
    state: Mutex<DelayedTasksState>,
}

enum DelayedTasksState {
    BeforeSpawn {
        make_fns: Vec<MakeTaskFn>,
    },
    AfterSpawn {
        tasks: FuturesUnordered<JoinTask<Result<()>>>,
    },
    Closed,
}

type MakeTaskFn = Box<dyn FnOnce() -> JoinTask<Result<()>> + Send + 'static>;

pin_project_lite::pin_project! {
    pub struct OptionPrepareFut<F> {
        #[pin]
        inner: Option<F>,
    }
}

impl<F> From<Option<F>> for OptionPrepareFut<F> {
    #[inline]
    fn from(inner: Option<F>) -> Self {
        Self { inner }
    }
}

impl<F, T, E> Future for OptionPrepareFut<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<Option<T>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.as_pin_mut() {
            Some(f) => match f.poll(cx) {
                Poll::Ready(Ok(res)) => Poll::Ready(Ok(Some(res))),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
            None => Poll::Ready(Ok(None)),
        }
    }
}

pin_project_lite::pin_project! {
    pub struct OptionHandleFut<F> {
        #[pin]
        inner: Option<F>,
    }
}

impl<F> From<Option<F>> for OptionHandleFut<F> {
    #[inline]
    fn from(inner: Option<F>) -> Self {
        Self { inner }
    }
}

impl<F, T, E> Future for OptionHandleFut<F>
where
    F: Future<Output = Result<T, E>>,
    T: Default,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.as_pin_mut() {
            Some(f) => f.poll(cx),
            None => Poll::Ready(Ok(T::default())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures_util::FutureExt;

    use super::*;

    #[tokio::test]
    async fn delayed_tasks() -> anyhow::Result<()> {
        Ok(())
    }

    #[tokio::test]
    async fn option_futures() {
        type NoopFut = futures_util::future::Ready<Result<(), ()>>;

        // Prepare
        let resolved = OptionPrepareFut::from(None::<NoopFut>);
        assert_eq!(resolved.now_or_never().unwrap(), Ok(None));

        let mut resolved = pin!(OptionPrepareFut::from(Some(async {
            tokio::task::yield_now().await;
            Ok::<_, ()>(())
        })));
        assert_eq!(futures_util::poll!(&mut resolved), Poll::Pending);
        assert_eq!(
            futures_util::poll!(&mut resolved),
            Poll::Ready(Ok(Some(())))
        );

        // Handle
        let resolved = OptionHandleFut::from(None::<NoopFut>);
        assert_eq!(resolved.now_or_never().unwrap(), Ok(()));

        let mut resolved = pin!(OptionHandleFut::from(Some(async {
            tokio::task::yield_now().await;
            Ok::<_, ()>(())
        })));
        assert_eq!(futures_util::poll!(&mut resolved), Poll::Pending);
        assert_eq!(futures_util::poll!(&mut resolved), Poll::Ready(Ok(())));
    }
}
