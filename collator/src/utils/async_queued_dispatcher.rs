use std::{future::Future, pin::Pin};

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;
use tracing::field::debug;

use super::task_descr::{TaskDesc, TaskResponder};

pub const STANDARD_DISPATCHER_QUEUE_BUFFER_SIZE: usize = 10;

type AsyncTaskDesc<W, R> = TaskDesc<
    dyn FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send,
    Result<R>,
>;

pub struct AsyncQueuedDispatcher<W, R> {
    tasks_queue: mpsc::Sender<AsyncTaskDesc<W, R>>,
}

impl<W, R> AsyncQueuedDispatcher<W, R>
where
    W: Send + 'static, // Send and 'static - to use inside tokio::spawn()
    R: Send + 'static,
{
    pub fn new(queue_buffer_size: usize) -> (Self, mpsc::Receiver<AsyncTaskDesc<W, R>>) {
        let (sender, receiver) = mpsc::channel::<AsyncTaskDesc<W, R>>(queue_buffer_size);
        let dispatcher = Self {
            tasks_queue: sender,
        };
        (dispatcher, receiver)
    }
    pub fn run(mut worker: W, mut receiver: mpsc::Receiver<AsyncTaskDesc<W, R>>) {
        debug!("dispatcher run");
        let h = tokio::spawn(async move {
            while let Some(task) = receiver.recv().await {
                debug!("TASK RECEIVED");
                let (task, responder) = task.extract();
                let future = task(worker);
                let future = Box::pin(future);
                let (updated_worker, res) = future.await;
                worker = updated_worker;
                let _ = responder.respond(res);
                debug!("TASK PROCESSED")
            }
            debug!("dispatcher finished");
        });
    }
    pub fn create(worker: W, queue_buffer_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(queue_buffer_size);

        Self::run(worker, receiver);

        Self {
            tasks_queue: sender,
        }
    }

    async fn _enqueue_task(&self, task: AsyncTaskDesc<W, R>) -> Result<()> {
        debug!("ENQUE TASK");
        self.tasks_queue
            .send(task)
            .await
            .map_err(|err| anyhow!("dispatcher queue receiver dropped {err:?}"))
    }

    fn _enqueue_task_blocking(&self, task: AsyncTaskDesc<W, R>) -> Result<()> {
        self.tasks_queue
            .blocking_send(task)
            .map_err(|err| anyhow!("dispatcher queue receiver dropped {err:?}"))
    }

    pub async fn enqueue_task(
        &self,
        task_fn: impl FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
    ) -> Result<()> {
        let task = AsyncTaskDesc::<W, R>::create(Box::new(task_fn));
        self._enqueue_task(task).await
    }

    pub fn enqueue_task_blocking(
        &self,
        task_fn: impl FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
    ) -> Result<()> {
        let task = AsyncTaskDesc::<W, R>::create(Box::new(task_fn));
        self._enqueue_task_blocking(task)
    }

    pub async fn enqueue_task_with_responder(
        &self,
        task_fn: impl FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
    ) -> Result<oneshot::Receiver<Result<R>>> {
        let (task, receiver) = AsyncTaskDesc::<W, R>::create_with_responder(Box::new(task_fn));
        self._enqueue_task(task).await?;
        Ok(receiver)
    }

    pub async fn execute_task(
        &self,
        task_fn: impl FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
    ) -> Result<R> {
        let (task, receiver) = AsyncTaskDesc::<W, R>::create_with_responder(Box::new(task_fn));
        self._enqueue_task(task).await?;
        receiver.await?
    }
}

#[macro_export]
macro_rules! method_to_async_task_closure {
    ($method:ident, $($arg:expr),*) => {
        #[allow(unused_mut)]
        move |mut worker| {
            Box::pin(async move {
                let res = worker.$method($($arg),*).await;
                (worker, res)
            })
        }
    };
}

#[cfg(test)]
#[tokio::test]
async fn test() {
    use crate::method_to_async_task_closure;

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
        .enqueue_task(method_to_async_task_closure!(action, "test1"))
        .await;

    // or build a closure by yourself
    let _ = dispatcher
        .enqueue_task(move |mut worker| {
            Box::pin(async move {
                // some async code block
                let subres = worker.action("test2").await.unwrap();
                let res = format!("converted result from ({subres})");

                (worker, Ok(res))
            })
        })
        .await;
}
