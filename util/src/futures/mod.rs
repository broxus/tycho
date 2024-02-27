use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use futures_util::{Future, FutureExt};

mod shared;

pub enum BoxFutureOrNoop<T> {
    Boxed(BoxFuture<'static, T>),
    Noop,
}

impl<T: 'static> BoxFutureOrNoop<T> {
    #[inline]
    pub fn future<F>(f: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        match castaway::cast!(f, BoxFuture<'static, T>) {
            Ok(f) => BoxFutureOrNoop::Boxed(f),
            Err(f) => BoxFutureOrNoop::Boxed(f.boxed()),
        }
    }
}

impl Future for BoxFutureOrNoop<()> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            BoxFutureOrNoop::Boxed(fut) => fut.poll_unpin(cx),
            BoxFutureOrNoop::Noop => std::task::Poll::Ready(()),
        }
    }
}

impl<T> Future for BoxFutureOrNoop<Option<T>> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            BoxFutureOrNoop::Boxed(fut) => fut.poll_unpin(cx),
            BoxFutureOrNoop::Noop => std::task::Poll::Ready(None),
        }
    }
}
