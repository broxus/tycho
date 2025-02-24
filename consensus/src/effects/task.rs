use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio::task::JoinHandle;

#[derive(thiserror::Error, Debug, Copy, Clone)]
#[error("task is cancelled")]
pub struct Cancelled();
pub type TaskResult<R> = std::result::Result<R, Cancelled>;

#[derive(Default, Clone)]
pub struct TaskTracker(tokio_util::task::task_tracker::TaskTracker);
pub struct TaskCtx<'a>(&'a tokio_util::task::task_tracker::TaskTracker);

impl TaskTracker {
    pub fn ctx(&self) -> TaskCtx<'_> {
        TaskCtx(&self.0)
    }

    /// Prevents executing new spawned tasks.
    /// Does not abort already running tasks, instead waits for them to finish.
    pub async fn stop(&self) {
        self.0.close();
        self.0.wait().await;
    }
}

impl TaskCtx<'_> {
    pub fn spawn<F>(&self, task: F) -> Task<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if self.0.is_closed() {
            return Task::aborted();
        }
        Task {
            handle: self.0.spawn(task),
            completed: false,
        }
    }

    pub fn spawn_blocking<F, R>(&self, task: F) -> Task<R>
    where
        F: FnOnce() -> R,
        F: Send + 'static,
        R: Send + 'static,
    {
        if self.0.is_closed() {
            return Task::aborted();
        }
        Task {
            handle: self.0.spawn_blocking(task),
            completed: false,
        }
    }
}

/// Version of [`tokio_util::task::AbortOnDropHandle`] suited for use inside
/// [`tycho_util::futures::Shared`] to call abort only once on drop
/// much like [`tycho_util::futures::JoinTask`], but every wrapped [`tokio::task::JoinHandle`]
/// keeps a [`tokio_util::sync::CancellationToken`] inside its task
#[must_use = "this Future returns Result that must be used"]
pub struct Task<R> {
    handle: JoinHandle<R>,
    completed: bool,
}

impl<R> Task<R> {
    pub fn aborted() -> Self
    where
        R: Send + 'static,
    {
        let task = tokio::spawn(futures_util::future::pending::<R>());
        task.abort();
        Task {
            handle: task,
            completed: true,
        }
    }

    /// Note: a future of a finished handle may still return [`Poll::Pending`] on occasion
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        if !self.completed {
            self.handle.abort();
        }
    }
}

impl<T> Future for Task<T> {
    type Output = TaskResult<T>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = futures_util::ready!(self.handle.poll_unpin(cx));
        self.completed = true;
        Poll::Ready(match res {
            Ok(value) => Ok(value),
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            // Should only happen when task tracker is closed or on program termination
            // Blocking task can be cancelled only until it starts running
            Err(_) => Err(Cancelled()),
        })
    }
}
