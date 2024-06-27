use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures_util::future::Future;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::mpsc;

pub const STANDARD_ASYNC_DISPATCHER_BUFFER_SIZE: usize = 100;

pub type Task<W> = Box<dyn FnOnce(Arc<W>) -> Fut + Send>;
pub type Fut = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub struct AsyncDispatcher<W> {
    descr: String,
    spawn_sender: mpsc::Sender<Task<W>>,
}

impl<W> Clone for AsyncDispatcher<W> {
    fn clone(&self) -> Self {
        Self {
            descr: self.descr.clone(),
            spawn_sender: self.spawn_sender.clone(),
        }
    }
}

impl<W> AsyncDispatcher<W>
where
    W: Send + Sync + 'static,
{
    pub fn new(descr: &str, queue_buffer_size: usize) -> (Self, mpsc::Receiver<Task<W>>) {
        let (spawn_sender, spawn_receiver) = mpsc::channel::<Task<W>>(queue_buffer_size);
        let dispatcher = Self {
            descr: descr.to_owned(),
            spawn_sender,
        };
        (dispatcher, spawn_receiver)
    }

    pub fn run(&self, worker: Arc<W>, mut spawn_receiver: mpsc::Receiver<Task<W>>) {
        let dispatcher_descr = self.descr.clone();
        tokio::spawn(async move {
            let mut futures = FuturesUnordered::new();
            loop {
                tokio::select! {
                    task_opt = spawn_receiver.recv() => match task_opt {
                        Some(task) => {
                            let worker = worker.clone();
                            futures.push(async move { task(worker).await });
                        }
                        None => {
                            panic!("async dispatcher: {}: tasks spawn channel closed!", dispatcher_descr)
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
                                "async dispatcher: {}: task result error! {:?}",
                                dispatcher_descr, err,
                            )
                        }
                    }
                }
            }
        });
    }

    pub async fn spawn_task<AsyncTask>(&self, task: AsyncTask) -> Result<()>
    where
        AsyncTask: FnOnce(Arc<W>) -> Fut + Send + 'static,
    {
        self.spawn_sender
            .send(Box::new(task))
            .await
            .map_err(|err| {
                anyhow!(
                    "async dispatcher: {}: tasks receiver dropped {err:?}",
                    self.descr,
                )
            })?;
        Ok(())
    }

    pub fn spawn_task_blocking<AsyncTask>(&self, task: AsyncTask) -> Result<()>
    where
        AsyncTask: FnOnce(Arc<W>) -> Fut + Send + 'static,
    {
        self.spawn_sender
            .blocking_send(Box::new(task))
            .map_err(|err| {
                anyhow!(
                    "async dispatcher: {}: tasks receiver dropped {err:?}",
                    self.descr,
                )
            })?;
        Ok(())
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
