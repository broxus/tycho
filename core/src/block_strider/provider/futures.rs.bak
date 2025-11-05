use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct SelectNonEmptyFut<F> {
    futures: F,
}

macro_rules! impl_select_tuple {
    ($($n:tt: $ty:ident),+) => {
        impl<$($ty),*> From<($($ty),*)> for SelectNonEmptyFut<($(Option<$ty>),*)> {
            fn from(futures: ($($ty),*)) -> Self {
                SelectNonEmptyFut {
                    futures: ($(Some(futures.$n),)*)
                }
            }
        }

        impl<$($ty),+, R> Future for SelectNonEmptyFut<($(Option<$ty>),*)>
        where
            $($ty: Future<Output = Option<R>> + Unpin,)*
        {
            type Output = Option<R>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let mut all_done = true;
                $(if let Some(f) = &mut self.futures.$n {
                    match Pin::new(f).poll(cx) {
                        Poll::Ready(res) if res.is_some() => return Poll::Ready(res),
                        Poll::Ready(_) => self.futures.$n = None,
                        Poll::Pending => all_done = false,
                    }
                })*

                if all_done {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
        }
    };
}

impl_select_tuple!(0: T0, 1: T1);
impl_select_tuple!(0: T0, 1: T1, 2: T2);
impl_select_tuple!(0: T0, 1: T1, 2: T2, 3: T3);
impl_select_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4);

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures_util::future::FutureExt;

    use super::*;

    // Yield `n` times and return `res`
    async fn make_skip_fut(n: usize, res: Option<usize>) -> Option<usize> {
        for _ in 0..n {
            tokio::task::yield_now().await;
        }
        res
    }

    #[tokio::test]
    async fn select_futures() {
        // Resolve the first future
        {
            let a = pin!(make_skip_fut(0, Some(1)));
            let b = pin!(make_skip_fut(0, None));
            let fut = SelectNonEmptyFut::from((a, b));
            assert_eq!(fut.now_or_never().unwrap(), Some(1));
        }

        // Resolve the second future
        {
            let a = pin!(make_skip_fut(0, None));
            let b = pin!(make_skip_fut(0, Some(2)));
            let fut = SelectNonEmptyFut::from((a, b));
            assert_eq!(fut.now_or_never().unwrap(), Some(2));
        }

        // Resolve the first future (long)
        {
            let a = pin!(make_skip_fut(2, Some(1)));
            let b = pin!(make_skip_fut(1, None));
            let mut fut = pin!(SelectNonEmptyFut::from((a, b)));
            assert_eq!(futures_util::poll!(&mut fut), Poll::Pending);
            assert_eq!(futures_util::poll!(&mut fut), Poll::Pending);
            assert_eq!(futures_util::poll!(&mut fut), Poll::Ready(Some(1)));
        }

        // Resolve the second future (long)
        {
            let a = pin!(make_skip_fut(0, None));
            let b = pin!(make_skip_fut(1, Some(2)));
            let mut fut = pin!(SelectNonEmptyFut::from((a, b)));
            assert_eq!(futures_util::poll!(&mut fut), Poll::Pending);
            assert_eq!(futures_util::poll!(&mut fut), Poll::Ready(Some(2)));
        }

        // Resolve the first complete future
        {
            let a = pin!(make_skip_fut(1, None));
            let b = pin!(make_skip_fut(2, Some(2)));
            let c = pin!(make_skip_fut(3, Some(3)));
            let mut fut = pin!(SelectNonEmptyFut::from((a, b, c)));
            assert_eq!(futures_util::poll!(&mut fut), Poll::Pending);
            assert_eq!(futures_util::poll!(&mut fut), Poll::Pending);
            assert_eq!(futures_util::poll!(&mut fut), Poll::Ready(Some(2)));
        }

        // Resolve none
        {
            let a = pin!(make_skip_fut(1, None));
            let b = pin!(make_skip_fut(2, None));
            let c = pin!(make_skip_fut(3, None));
            let d = pin!(make_skip_fut(4, None));
            let e = pin!(make_skip_fut(5, None));
            let mut fut = pin!(SelectNonEmptyFut::from((a, b, c, d, e)));
            for _ in 0..5 {
                assert_eq!(futures_util::poll!(&mut fut), Poll::Pending);
            }
            assert_eq!(futures_util::poll!(&mut fut), Poll::Ready(None));
        }
    }
}
