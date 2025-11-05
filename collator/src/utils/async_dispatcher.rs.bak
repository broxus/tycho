use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use futures_util::future::Future;
use futures_util::stream::FuturesUnordered;
use tokio::sync::mpsc;

pub const STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE: usize = 100;

pub type TaskFunc<W> = Box<dyn FnOnce(Arc<W>) -> Fut + Send>;
pub type Fut = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub enum AsyncTask<W> {
    Spawn(TaskFunc<W>),
    Enqueue(TaskFunc<W>),
}

pub struct AsyncDispatcherContext<W> {
    pub spawned_tasks_receiver: mpsc::Receiver<TaskFunc<W>>,
    pub queued_tasks_receiver: mpsc::Receiver<TaskFunc<W>>,
}

pub struct AsyncDispatcher<W> {
    descr: String,
    queue_buffer_size: usize,
    spawned_tasks_sender: mpsc::Sender<TaskFunc<W>>,
    queued_tasks_sender: mpsc::Sender<TaskFunc<W>>,
}

impl<W> Clone for AsyncDispatcher<W> {
    fn clone(&self) -> Self {
        Self {
            descr: self.descr.clone(),
            queue_buffer_size: self.queue_buffer_size,
            spawned_tasks_sender: self.spawned_tasks_sender.clone(),
            queued_tasks_sender: self.queued_tasks_sender.clone(),
        }
    }
}

impl<W> AsyncDispatcher<W>
where
    W: Send + Sync + 'static,
{
    pub fn new(descr: &str, queue_buffer_size: usize) -> (Self, AsyncDispatcherContext<W>) {
        let (spawned_tasks_sender, spawned_tasks_receiver) =
            mpsc::channel::<TaskFunc<W>>(queue_buffer_size);
        let (queued_tasks_sender, queued_tasks_receiver) =
            mpsc::channel::<TaskFunc<W>>(queue_buffer_size);
        let dispatcher = Self {
            descr: descr.to_owned(),
            queue_buffer_size,
            spawned_tasks_sender,
            queued_tasks_sender,
        };
        (dispatcher, AsyncDispatcherContext {
            spawned_tasks_receiver,
            queued_tasks_receiver,
        })
    }

    pub fn run(&self, worker: Arc<W>, ctx: AsyncDispatcherContext<W>) {
        let AsyncDispatcherContext {
            mut spawned_tasks_receiver,
            mut queued_tasks_receiver,
        } = ctx;

        // queued tasks
        let dispatcher_descr = self.descr.clone();
        let queue_worker = worker.clone();
        tokio::spawn(async move {
            while let Some(func) = queued_tasks_receiver.recv().await {
                let task_res = func(queue_worker.clone()).await;
                if let Err(err) = task_res {
                    panic!(
                        "async dispatcher: {dispatcher_descr}: queued task result error! {err:?}",
                    )
                }
            }
        });

        // async tasks
        let dispatcher_descr = self.descr.clone();
        tokio::spawn(async move {
            let mut futures = FuturesUnordered::new();
            loop {
                tokio::select! {
                    task_opt = spawned_tasks_receiver.recv() => match task_opt {
                        Some(func) => {
                            let join_task = tycho_util::futures::JoinTask::new(func(worker.clone()));
                            futures.push(join_task);
                        }
                        None => {
                            panic!("async dispatcher: {dispatcher_descr}: tasks channel closed!")
                        }
                    },
                    task_res = async {
                        if futures.is_empty() {
                            futures_util::future::pending::<Result<()>>().await
                        } else {
                            futures.next().await.unwrap()
                        }
                    } => {
                        if let Err(err) = task_res {
                            panic!(
                                "async dispatcher: {dispatcher_descr}: spawned task result error! {err:?}",
                            )
                        }
                    }
                }
            }
        });
    }

    pub async fn spawn_task<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(Arc<W>) -> Fut + Send + 'static,
    {
        self.spawned_tasks_sender
            .send(Box::new(func))
            .await
            .map_err(|err| {
                anyhow!(
                    "async dispatcher: {}: spawned tasks receiver dropped {err:?}",
                    self.descr,
                )
            })
    }

    pub fn spawn_task_blocking<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(Arc<W>) -> Fut + Send + 'static,
    {
        self.spawned_tasks_sender
            .blocking_send(Box::new(func))
            .map_err(|err| {
                anyhow!(
                    "async dispatcher: {}: spawned tasks receiver dropped {err:?}",
                    self.descr,
                )
            })
    }

    pub async fn enqueue_task<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(Arc<W>) -> Fut + Send + 'static,
    {
        self.queued_tasks_sender
            .send(Box::new(func))
            .await
            .map_err(|err| {
                anyhow!(
                    "async dispatcher: {}: queued tasks receiver dropped {err:?}",
                    self.descr,
                )
            })
    }

    pub fn enqueue_task_blocking<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(Arc<W>) -> Fut + Send + 'static,
    {
        self.queued_tasks_sender
            .blocking_send(Box::new(func))
            .map_err(|err| {
                anyhow!(
                    "async dispatcher: {}: queued tasks receiver dropped {err:?}",
                    self.descr,
                )
            })
    }
}

#[macro_export]
macro_rules! method_to_async_closure {
    ($method:ident, $($arg:expr),*) => {
        move |worker| {
            Box::pin(async move { worker.$method($($arg),*).await })
        }
    };
}
