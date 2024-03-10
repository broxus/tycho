use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::engine::dag::DagPoint;

pub struct DownloadTask {}

impl Future for DownloadTask {
    type Output = Result<DagPoint, anyhow::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}
