use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::models::DagPoint;

pub struct Downloader {
    // point's author is a top priority; fallback priority is (any) dependent point's author
    // recursively: every dependency is expected to be signed by 2/3+1
}

impl Future for Downloader {
    type Output = DagPoint;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}
