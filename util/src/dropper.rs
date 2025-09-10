use std::sync::OnceLock;

use tokio::sync::mpsc;

pub struct BackgroundDropper {
    tx: mpsc::UnboundedSender<Box<dyn FnOnce() + Send + 'static>>,
}

impl BackgroundDropper {
    fn new() -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Box<dyn FnOnce() + Send + 'static>>();

        tokio::spawn(async move {
            while let Some(closure) = rx.recv().await {
                closure();
            }
        });

        Self { tx }
    }

    fn send<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let boxed_task = Box::new(task);

        if let Err(e) = self.tx.send(boxed_task) {
            tracing::warn!("Background executor thread is dead, task will be dropped: {e}");
        }
    }

    fn instance() -> &'static Self {
        static INSTANCE: OnceLock<BackgroundDropper> = OnceLock::new();
        INSTANCE.get_or_init(Self::new)
    }
}

pub fn drop_in_background<F>(task: F)
where
    F: FnOnce() + Send + 'static,
{
    BackgroundDropper::instance().send(task);
}
