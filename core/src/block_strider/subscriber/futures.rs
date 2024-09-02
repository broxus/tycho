use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

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
