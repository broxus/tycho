use std::collections::hash_map;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use anyhow::Result;
use futures_util::{Future, Stream, StreamExt};
use tokio::task::{AbortHandle, JoinSet};
use tycho_util::{FastHashMap, FastHashSet};

use crate::overlay::OverlayId;

pub(crate) struct TasksStream {
    name: &'static str,
    stream: IdsStream,
    handles: FastHashMap<OverlayId, (AbortHandle, bool)>,
    join_set: JoinSet<OverlayId>,
}

impl TasksStream {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            stream: Default::default(),
            handles: Default::default(),
            join_set: Default::default(),
        }
    }

    pub async fn next(&mut self) -> Option<OverlayId> {
        use futures_util::future::{select, Either};

        loop {
            // Wait until the next interval or completed task
            let res = {
                let next = std::pin::pin!(self.stream.next());
                let joined = std::pin::pin!(self.join_set.join_next());
                match select(next, joined).await {
                    // Handle interval events first
                    Either::Left((id, _)) => return id,
                    // Handled task completion otherwise
                    Either::Right((joined, fut)) => match joined {
                        Some(res) => res,
                        None => return fut.await,
                    },
                }
            };

            // If some task was joined
            match res {
                // Task was completed successfully
                Ok(overlay_id) => {
                    return if matches!(self.handles.remove(&overlay_id), Some((_, true))) {
                        // Reset interval and execute task immediately
                        self.stream.reset_interval(&overlay_id);
                        Some(overlay_id)
                    } else {
                        None
                    };
                }
                // Propagate task panic
                Err(e) if e.is_panic() => {
                    tracing::error!(task = self.name, "task panicked");
                    std::panic::resume_unwind(e.into_panic());
                }
                // Task cancelled, loop once more with the next task
                Err(_) => continue,
            }
        }
    }

    pub fn rebuild<I, F>(&mut self, iter: I, f: F)
    where
        I: Iterator<Item = OverlayId>,
        for<'a> F: FnMut(&'a OverlayId) -> tokio::time::Interval,
    {
        self.rebuild_ext(iter, f, |_| {});
    }

    pub fn rebuild_ext<I, F, R>(&mut self, iter: I, on_add: F, mut on_remove: R)
    where
        I: Iterator<Item = OverlayId>,
        for<'a> F: FnMut(&'a OverlayId) -> tokio::time::Interval,
        for<'a> R: FnMut(&'a OverlayId),
    {
        self.stream.rebuild(iter, on_add, |overlay_id| {
            on_remove(overlay_id);

            if let Some((handle, _)) = self.handles.remove(overlay_id) {
                tracing::debug!(task = self.name, %overlay_id, "task cancelled");
                handle.abort();
            }
        });
    }

    pub fn spawn<F, Fut>(&mut self, overlay_id: &OverlayId, f: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        match self.handles.entry(*overlay_id) {
            hash_map::Entry::Vacant(entry) => {
                let fut = {
                    let fut = f();
                    let task = self.name;
                    let overlay_id = *overlay_id;
                    async move {
                        if let Err(e) = fut.await {
                            tracing::error!(task, %overlay_id, "task failed: {e:?}");
                        }
                        overlay_id
                    }
                };
                entry.insert((self.join_set.spawn(fut), false));
            }
            hash_map::Entry::Occupied(mut entry) => {
                tracing::warn!(
                    task = self.name,
                    %overlay_id,
                    "task is running longer than expected",
                );
                entry.get_mut().1 = true;
            }
        }
    }
}

#[derive(Default)]
struct IdsStream {
    intervals: Vec<(tokio::time::Interval, OverlayId)>,
    waker: Option<Waker>,
}

impl IdsStream {
    fn reset_interval(&mut self, overlay_id: &OverlayId) {
        if let Some((interval, _)) = self.intervals.iter_mut().find(|(_, id)| id == overlay_id) {
            interval.reset();
        }
    }

    fn rebuild<I: Iterator<Item = OverlayId>, A, R>(
        &mut self,
        iter: I,
        mut on_add: A,
        mut on_remove: R,
    ) where
        for<'a> A: FnMut(&'a OverlayId) -> tokio::time::Interval,
        for<'a> R: FnMut(&'a OverlayId),
    {
        let mut new_overlays = iter.collect::<FastHashSet<_>>();
        self.intervals.retain(|(_, id)| {
            let retain = new_overlays.remove(id);
            if !retain {
                on_remove(id);
            }
            retain
        });

        for id in new_overlays {
            self.intervals.push((on_add(&id), id));
        }

        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
}

impl Stream for IdsStream {
    type Item = OverlayId;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Always register the waker to resume the stream even if there were
        // changes in the intervals.
        if !matches!(&self.waker, Some(waker) if cx.waker().will_wake(waker)) {
            self.waker = Some(cx.waker().clone());
        }

        for (interval, data) in self.intervals.iter_mut() {
            if interval.poll_tick(cx).is_ready() {
                return Poll::Ready(Some(*data));
            }
        }

        Poll::Pending
    }
}
