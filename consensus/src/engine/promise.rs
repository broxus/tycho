use std::fmt::Debug;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use thiserror;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

#[derive(Clone, Debug, thiserror::Error)]
pub enum PromiseError {
    #[error("Promise task failed: '{0}'")]
    TaskFailed(String),
    #[error("Promise task panicked")]
    TaskPanicked,
    #[error("Promise internal error: sender closed")]
    Internal,
}

#[derive(Clone)]
enum Inner<T>
where
    T: Clone + Send + Sync + 'static,
{
    Ready(Result<T, PromiseError>),
    Pending(Weak<broadcast::Sender<Result<T, PromiseError>>>),
}

#[derive(Clone)]
pub struct Promise<T>
where
    T: Clone + Send + Sync + 'static,
{
    // TODO try OnceLock::get before mutex, then if None = Sender::subscribe() try OnceLock again
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Promise<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub async fn get(&self) -> Result<T, PromiseError> {
        let inner = self.inner.lock();
        let inner = inner.deref();

        match inner {
            Inner::Ready(value) => value.clone(),
            Inner::Pending(inflight) => match inflight.upgrade().map(|a| a.subscribe()) {
                None => Err(PromiseError::TaskPanicked),
                Some(mut rx) => match rx.recv().await {
                    Ok(value) => value,
                    Err(RecvError::Lagged(_)) => {
                        rx.recv().await.unwrap_or(Err(PromiseError::Internal))
                    }
                    Err(RecvError::Closed) => Err(PromiseError::Internal),
                },
            },
        }
    }

    pub fn ready(value: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::Ready(Ok(value)))),
        }
    }

    pub fn new<E>(
        fut: Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>>,
    ) -> Self
    where
        E: Debug + 'static,
    {
        let (tx, _) = broadcast::channel::<Result<T, PromiseError>>(1);
        let tx = Arc::new(tx);

        // weak ref to Sender is dropped if spawned task panicked
        let inner = Arc::new(Mutex::new(Inner::Pending(Arc::downgrade(&tx))));
        let this = Self {
            inner: inner.clone(),
        };

        tokio::spawn(async move {
            let res = fut
                .await
                .map_err(|e| PromiseError::TaskFailed(format!("{e:?}")));
            let mut inner = inner.lock();
            let _ = tx.send(res.clone());
            *inner = Inner::Ready(res);
        });

        this
    }
}
