use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::FutureExt;

pin_project_lite::pin_project! {
    #[project = SleepOrPendingProj]
    pub enum SleepOrPending {
        Sleep {
            #[pin]
            fut: tokio::time::Sleep,
        },
        Pending,
    }
}

impl SleepOrPending {
    pub fn new(duration: Option<Duration>) -> Self {
        match duration {
            Some(duration) => Self::Sleep {
                fut: tokio::time::sleep(duration),
            },
            None => Self::Pending,
        }
    }

    pub fn until<I: Into<tokio::time::Instant>>(instant: Option<I>) -> Self {
        match instant {
            Some(instant) => Self::Sleep {
                fut: tokio::time::sleep_until(instant.into()),
            },
            None => Self::Pending,
        }
    }
}

impl From<Option<Duration>> for SleepOrPending {
    #[inline]
    fn from(value: Option<Duration>) -> Self {
        Self::new(value)
    }
}

impl From<Option<tokio::time::Instant>> for SleepOrPending {
    #[inline]
    fn from(value: Option<tokio::time::Instant>) -> Self {
        Self::until(value)
    }
}

impl From<Option<std::time::Instant>> for SleepOrPending {
    #[inline]
    fn from(value: Option<std::time::Instant>) -> Self {
        Self::until(value)
    }
}

impl Future for SleepOrPending {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            SleepOrPendingProj::Sleep { mut fut } => fut.poll_unpin(cx),
            SleepOrPendingProj::Pending { .. } => Poll::Pending,
        }
    }
}
