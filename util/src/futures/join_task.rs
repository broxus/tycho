use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{Future, FutureExt};
use tokio::task::JoinHandle;

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct JoinTask<T> {
    handle: JoinHandle<T>,
    completed: bool,
}

impl<T> JoinTask<T> {
    #[inline]
    pub fn new<F>(f: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Self {
            handle: tokio::spawn(f),
            completed: false,
        }
    }
}

impl<T> Drop for JoinTask<T> {
    fn drop(&mut self) {
        if !self.completed {
            self.handle.abort();
        }
    }
}

impl<T> Future for JoinTask<T> {
    type Output = T;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = futures_util::ready!(self.handle.poll_unpin(cx));
        match res {
            Ok(value) => {
                self.completed = true;
                Poll::Ready(value)
            }
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                }
                unreachable!()
            }
        }
    }
}
