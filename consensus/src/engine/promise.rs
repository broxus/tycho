use std::sync::{Arc, Weak};

use futures_util::future::BoxFuture;
use futures_util::Future;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

#[derive(Clone)]
enum Inner<T> {
    Ready(T),
    Pending(Weak<broadcast::Sender<T>>),
}

#[derive(Clone)]
pub struct Promise<T> {
    // TODO try OnceLock::get before mutex, then if None = Sender::subscribe() try OnceLock again
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Promise<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new<F>(fut: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        fn new_impl<T>(fut: BoxFuture<'static, T>) -> Promise<T>
        where
            T: Clone + Send + 'static,
        {
            let (tx, _) = broadcast::channel::<T>(1);
            let tx = Arc::new(tx);

            // weak ref to Sender is dropped if spawned task panicked
            let inner = Arc::new(Mutex::new(Inner::Pending(Arc::downgrade(&tx))));
            let this = Promise {
                inner: inner.clone(),
            };

            tokio::spawn(async move {
                let res = fut.await;
                let mut inner = inner.lock();
                _ = tx.send(res.clone());
                *inner = Inner::Ready(res);
            });

            this
        }

        let fut = match castaway::cast!(fut, BoxFuture<'static, T>) {
            Ok(fut) => fut,
            Err(fut) => Box::pin(fut),
        };
        new_impl(fut)
    }

    pub fn ready(value: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::Ready(value))),
        }
    }

    pub async fn get(&self) -> T {
        'panicked: {
            let mut rx = match &*self.inner.lock() {
                Inner::Ready(value) => return value.clone(),
                Inner::Pending(inflight) => match inflight.upgrade() {
                    Some(rx) => rx.subscribe(),
                    None => break 'panicked,
                },
            };

            match rx.recv().await {
                Ok(value) => return value,
                Err(RecvError::Closed) => break 'panicked,
                Err(RecvError::Lagged(_)) => unreachable!(),
            }
        }

        panic!("task panicked")
    }
}
