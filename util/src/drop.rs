use std::any::Any;
use std::sync::OnceLock;

use tokio::sync::mpsc;

pub struct BackgroundDrop {
    tx: mpsc::UnboundedSender<Box<dyn Any + Send>>,
}

impl BackgroundDrop {
    fn new() -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Box<dyn Any + Send>>();

        tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                drop(item);
            }
        });

        Self { tx }
    }

    fn send<T>(&self, t: T)
    where
        T: Send + 'static,
    {
        let boxed = Box::new(t);

        if let Err(e) = self.tx.send(boxed) {
            tracing::warn!("background Drop thread is dead, task will be dropped: {e}");
        }
    }

    fn instance() -> &'static Self {
        static INSTANCE: OnceLock<BackgroundDrop> = OnceLock::new();
        INSTANCE.get_or_init(Self::new)
    }
}

pub fn drop_in_background<T>(t: T)
where
    T: Send + 'static,
{
    BackgroundDrop::instance().send(t);
}
