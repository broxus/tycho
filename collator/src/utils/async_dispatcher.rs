use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures_util::future::Future;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::mpsc;

pub const STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE: usize = 100;

pub type TaskFunc<W> = Box<dyn FnOnce(Arc<W>) -> Fut + Send>;
pub type Fut = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub enum AsyncTask<W> {
    Spawn(TaskFunc<W>),
    Enqueue(TaskFunc<W>),
}

pub struct AsyncDispatcher<W> {
    descr: String,
    queue_buffer_size: usize,
    tasks_sender: mpsc::Sender<AsyncTask<W>>,
}

impl<W> Clone for AsyncDispatcher<W> {
    fn clone(&self) -> Self {
        Self {
            descr: self.descr.clone(),
            queue_buffer_size: self.queue_buffer_size,
            tasks_sender: self.tasks_sender.clone(),
        }
    }
}

impl<W> AsyncDispatcher<W>
where
    W: Send + Sync + 'static,
{
    pub fn new(descr: &str, queue_buffer_size: usize) -> (Self, mpsc::Receiver<AsyncTask<W>>) {
        let (tasks_sender, tasks_receiver) = mpsc::channel::<AsyncTask<W>>(queue_buffer_size);
        let dispatcher = Self {
            descr: descr.to_owned(),
            queue_buffer_size,
            tasks_sender,
        };
        (dispatcher, tasks_receiver)
    }

    pub fn run(&self, worker: Arc<W>, mut tasks_receiver: mpsc::Receiver<AsyncTask<W>>) {
        // queued tasks
        let (tasks_queue_sender, mut tasks_queue_receiver) =
            mpsc::channel::<TaskFunc<W>>(self.queue_buffer_size);
        let dispatcher_descr = self.descr.clone();
        let queue_worker = worker.clone();
        tokio::spawn(async move {
            while let Some(func) = tasks_queue_receiver.recv().await {
                let task_res = func(queue_worker.clone()).await;
                if let Err(err) = task_res {
                    panic!(
                        "async dispatcher: {}: queued task result error! {:?}",
                        dispatcher_descr, err,
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
                    task_opt = tasks_receiver.recv() => match task_opt {
                        Some(AsyncTask::Spawn(func)) => {
                            let join_task = tycho_util::futures::JoinTask::new(func(worker.clone()));
                            futures.push(join_task);
                        }
                        Some(AsyncTask::Enqueue(func)) => {
                            tasks_queue_sender.send(func).await.map_err(|err| {
                                anyhow!(
                                    "async dispatcher: {}: queued tasks receiver dropped {:?}",
                                    dispatcher_descr, err,
                                )
                            }).unwrap();
                        }
                        None => {
                            panic!("async dispatcher: {}: tasks channel closed!", dispatcher_descr)
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
                                "async dispatcher: {}: spawned task result error! {:?}",
                                dispatcher_descr, err,
                            )
                        }
                    }
                }
            }
        });
    }

    async fn send_task(&self, task: AsyncTask<W>) -> Result<()> {
        self.tasks_sender.send(task).await.map_err(|err| {
            anyhow!(
                "async dispatcher: {}: tasks receiver dropped {err:?}",
                self.descr,
            )
        })?;
        Ok(())
    }

    fn send_task_blocking(&self, task: AsyncTask<W>) -> Result<()> {
        self.tasks_sender.blocking_send(task).map_err(|err| {
            anyhow!(
                "async dispatcher: {}: tasks receiver dropped {err:?}",
                self.descr,
            )
        })?;
        Ok(())
    }

    pub async fn spawn_task<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(Arc<W>) -> Fut + Send + 'static,
    {
        self.send_task(AsyncTask::Spawn(Box::new(func))).await
    }

    pub fn spawn_task_blocking<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(Arc<W>) -> Fut + Send + 'static,
    {
        self.send_task_blocking(AsyncTask::Spawn(Box::new(func)))
    }

    pub async fn enqueue_task<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(Arc<W>) -> Fut + Send + 'static,
    {
        self.send_task(AsyncTask::Enqueue(Box::new(func))).await
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
