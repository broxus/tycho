use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use metrics::atomics::AtomicU64;
use tokio::sync::{mpsc, oneshot};

use super::task_descr::{TaskDesc, TaskResponder};
use crate::tracing_targets;

pub const STANDARD_QUEUED_DISPATCHER_BUFFER_SIZE: usize = 100;

type AsyncTaskDesc<W, R> = TaskDesc<
    dyn FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send,
    Result<R>,
>;

pub struct AsyncQueuedDispatcher<W, R = ()> {
    task_id_counter: Arc<AtomicU64>,
    tasks_queue: mpsc::Sender<AsyncTaskDesc<W, R>>,
}

impl<W, R> Clone for AsyncQueuedDispatcher<W, R> {
    fn clone(&self) -> Self {
        Self {
            task_id_counter: self.task_id_counter.clone(),
            tasks_queue: self.tasks_queue.clone(),
        }
    }
}

impl<W, R> AsyncQueuedDispatcher<W, R>
where
    W: Send + 'static, // Send and 'static - to use inside tokio::spawn()
    R: Send + 'static,
{
    pub fn new(queue_buffer_size: usize) -> (Self, mpsc::Receiver<AsyncTaskDesc<W, R>>) {
        let (sender, receiver) = mpsc::channel::<AsyncTaskDesc<W, R>>(queue_buffer_size);
        let dispatcher = Self {
            task_id_counter: Arc::new(AtomicU64::default()),
            tasks_queue: sender,
        };
        (dispatcher, receiver)
    }

    pub fn run(mut worker: W, mut receiver: mpsc::Receiver<AsyncTaskDesc<W, R>>) {
        tokio::spawn(async move {
            while let Some(task) = receiver.recv().await {
                tracing::trace!(
                    target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
                    "Task #{} ({}): received",
                    task.id(),
                    task.get_descr());
                let (task_id, task_descr) = (task.id(), task.get_descr());
                let (func, responder) = task.extract();
                tracing::trace!(
                    target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
                    "Task #{} ({}): executing...", task_id, &task_descr,
                );
                let future = func(worker);
                let (updated_worker, res) = future.await;
                tracing::trace!(
                    target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
                    "Task #{} ({}): executed", task_id, &task_descr,
                );
                worker = updated_worker;
                if let Some(res) = responder.respond(res) {
                    // no responder, or no receiver can handle result, should panic if task result is Error
                    if let Err(err) = res {
                        panic!(
                            "Task #{} ({}): result error! {:?}",
                            task_id, &task_descr, err,
                        )
                    }
                } else {
                    tracing::trace!(
                        target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
                        "Task #{} ({}): result responded", task_id, &task_descr,
                    );
                }
            }
        });
    }

    pub fn create(worker: W, queue_buffer_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(queue_buffer_size);

        Self::run(worker, receiver);

        Self {
            task_id_counter: Arc::new(AtomicU64::default()),
            tasks_queue: sender,
        }
    }

    async fn _enqueue_task(&self, task: AsyncTaskDesc<W, R>) -> Result<()> {
        let (task_id, task_descr) = (task.id(), task.get_descr());
        self.tasks_queue
            .send(task)
            .await
            .map_err(|err| anyhow!("dispatcher queue receiver dropped {err:?}"))?;
        tracing::trace!(
            target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): enqueued", task_id, task_descr,
        );
        Ok(())
    }

    fn _enqueue_task_blocking(&self, task: AsyncTaskDesc<W, R>) -> Result<()> {
        let (task_id, task_descr) = (task.id(), task.get_descr());
        self.tasks_queue
            .blocking_send(task)
            .map_err(|err| anyhow!("dispatcher queue receiver dropped {err:?}"))?;
        tracing::trace!(
            target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): enqueued (blocking)", task_id, task_descr,
        );
        Ok(())
    }

    pub async fn enqueue_task(
        &self,
        (task_descr, task_fn): (
            &str,
            impl FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
        ),
    ) -> Result<()> {
        let id = self.task_id_counter.fetch_add(1, Ordering::Release);
        let task = AsyncTaskDesc::<W, R>::create(id, task_descr, Box::new(task_fn));
        self._enqueue_task(task).await
    }

    pub fn enqueue_task_blocking(
        &self,
        (task_descr, task_fn): (
            &str,
            impl FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
        ),
    ) -> Result<()> {
        let id = self.task_id_counter.fetch_add(1, Ordering::Release);
        let task = AsyncTaskDesc::<W, R>::create(id, task_descr, Box::new(task_fn));
        self._enqueue_task_blocking(task)
    }

    pub async fn enqueue_task_with_responder(
        &self,
        (task_descr, task_fn): (
            &str,
            impl FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
        ),
    ) -> Result<oneshot::Receiver<Result<R>>> {
        let id = self.task_id_counter.fetch_add(1, Ordering::Release);
        let (task, receiver) =
            AsyncTaskDesc::<W, R>::create_with_responder(id, task_descr, Box::new(task_fn));
        let (task_id, task_descr) = (task.id(), task.get_descr());
        tracing::trace!(
            target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): enqueue_task_with_responder()",
            task_id,
            &task_descr
        );
        self._enqueue_task(task).await?;
        tracing::trace!(
            target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): enqueue_task_with_responder(): receiver returned",
            task_id,
            task_descr,
        );
        Ok(receiver)
    }

    pub async fn execute_task(
        &self,
        (task_descr, task_fn): (
            &str,
            impl FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
        ),
    ) -> Result<R> {
        let id = self.task_id_counter.fetch_add(1, Ordering::Release);
        let (task, receiver) =
            AsyncTaskDesc::<W, R>::create_with_responder(id, task_descr, Box::new(task_fn));
        let (task_id, task_descr) = (task.id(), task.get_descr());
        tracing::trace!(
            target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): execute_task()", task_id, &task_descr,
        );
        self._enqueue_task(task).await?;
        let res = receiver.await?;
        tracing::trace!(
            target: tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): execute_task(): result received and forwarded",
            task_id,
            task_descr,
        );
        res
    }
}

#[macro_export]
macro_rules! method_to_queued_async_closure {
    ($method:ident, $($arg:expr),*) => {
        (stringify!($method),
        #[allow(unused_mut)]
        move |mut worker| {
            Box::pin(async move {
                let res = worker.$method($($arg),*).await;
                (worker, res)
            })
        })
    };
}

#[cfg(test)]
#[tokio::test]
async fn test() {
    use crate::method_to_queued_async_closure;

    struct Worker {}
    impl Worker {
        async fn action(&mut self, arg: &str) -> Result<String> {
            Ok(format!("action result with arg: {arg}"))
        }
    }

    let worker_obj = Worker {};

    let dispatcher = AsyncQueuedDispatcher::<_, String>::create(worker_obj, 10);

    // use marco to just call a worker method
    let _ = dispatcher
        .enqueue_task(method_to_queued_async_closure!(action, "test1"))
        .await;

    // or build a closure by yourself
    let _ = dispatcher
        .enqueue_task(("taskdescr", move |mut worker| {
            Box::pin(async move {
                // some async code block
                let subres = worker.action("test2").await.unwrap();
                let res = format!("converted result from ({subres})");

                (worker, Ok(res))
            })
        }))
        .await;
}
