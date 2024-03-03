use std::{
    future::{Future, IntoFuture},
    pin::Pin,
};

use anyhow::Error;
use tokio::sync::oneshot;

pub struct TaskDesc<F: ?Sized, R> {
    closure: Box<F>,                      //closure for execution
    creation_time: std::time::SystemTime, //time of task creation
    responder: Option<oneshot::Sender<R>>,
}

impl<F: ?Sized, R> TaskDesc<F, R> {
    pub fn create(closure: Box<F>) -> Self {
        Self {
            closure,
            creation_time: std::time::SystemTime::now(),
            responder: None,
        }
    }
    pub fn create_with_responder(closure: Box<F>) -> (Self, oneshot::Receiver<R>) {
        let (sender, receiver) = oneshot::channel::<R>();
        let task_descr = Self {
            closure,
            creation_time: std::time::SystemTime::now(),
            responder: Some(sender),
        };
        (task_descr, receiver)
    }
    pub fn extract(self) -> (Box<F>, Option<oneshot::Sender<R>>) {
        (self.closure, self.responder)
    }
    pub fn closure(&self) -> &F {
        &self.closure
    }
    pub fn respond(self, res: R) -> Option<()> {
        self.responder.respond(res)
    }
}

pub trait TaskResponder<R> {
    fn respond(self, res: R) -> Option<()>;
}
impl<R> TaskResponder<R> for Option<oneshot::Sender<R>> {
    fn respond(self, res: R) -> Option<()> {
        if let Some(responder) = self {
            responder
                .send(res)
                .map_err(|_err| {
                    tracing::warn!("response receiver dropped");
                })
                .ok()
        } else {
            None
        }
    }
}

pub struct TaskResponseReceiver<R, T>
where
    T: TryFrom<R>,
{
    _marker_t: std::marker::PhantomData<T>,
    inner_receiver: oneshot::Receiver<anyhow::Result<R>>,
}
impl<R, T> TaskResponseReceiver<R, T>
where
    R: Send + 'static,
    T: TryFrom<R, Error = anyhow::Error> + Send + 'static,
{
    pub fn create(receiver: oneshot::Receiver<anyhow::Result<R>>) -> Self {
        Self {
            _marker_t: std::marker::PhantomData,
            inner_receiver: receiver,
        }
    }
    pub async fn try_recv(self) -> anyhow::Result<T> {
        self.inner_receiver.await?.and_then(|res| res.try_into())
    }

    /// Example:
    /// ```
    /// let dispatcher = self.dispatcher.clone();
    /// receiver.process_on_recv(|res| async move {
    ///     dispatcher
    ///        .enqueue_task(method_to_async_task_closure!(
    ///             refresh_collation_sessions,
    ///             res
    ///         ))
    ///         .await
    /// });
    /// ```
    pub fn process_on_recv<Fut>(self, process_callback: impl FnOnce(T) -> Fut + Send + 'static)
    where
        Fut: Future<Output = anyhow::Result<()>> + Send,
    {
        tokio::spawn(async move {
            if let Ok(res) = self.try_recv().await {
                process_callback(res).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::TaskDesc;

    #[test]
    fn void_task_without_responder() {
        let task = TaskDesc::create(Box::new(|| {
            println!("task executed");
        }));

        let task_fn = task.closure();

        task_fn();

        let respond_res = task.respond(());
        println!("resond_res: {respond_res:?}");

        assert!(
            respond_res.is_none(),
            "task without responder should return None when call .respond()"
        );
    }

    #[tokio::test]
    async fn void_task_with_responder() {
        let (task, receiver) = TaskDesc::create_with_responder(Box::new(|| {
            println!("task executed");
        }));

        let task_fn = task.closure();

        task_fn();

        let respond_res = task.respond(());
        println!("resond_res: {respond_res:?}");

        assert!(
            respond_res.is_some(),
            "task with responder should return Some(()) when call .respond()"
        );

        let received_res = receiver.await;
        println!("received_res: {received_res:?}");

        assert!(received_res.is_ok());
    }

    #[test]
    fn returning_task_without_responder() {
        let task = TaskDesc::create(Box::new(|| {
            println!("task executed");
            String::from("task result")
        }));

        let task_fn = task.closure();

        let res = task_fn();
        println!("task res: {res:?}");

        assert_eq!(&res, "task result");

        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");

        assert!(
            respond_res.is_none(),
            "task without responder should return None when call .respond()"
        );
    }

    #[tokio::test]
    async fn returning_task_with_responder() {
        let (task, receiver) = TaskDesc::create_with_responder(Box::new(|| {
            println!("task executed");
            String::from("task result")
        }));

        let task_fn = task.closure();

        let res = task_fn();
        println!("task res: {res:?}");

        assert_eq!(&res, "task result");

        let expected_received = res.clone();

        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");

        assert!(
            respond_res.is_some(),
            "task with responder should return Some(()) when call .respond()"
        );

        let received_res = receiver.await;
        println!("received_res: {received_res:?}");

        assert!(received_res.is_ok());

        let received = received_res.unwrap();

        assert_eq!(received, expected_received);
    }

    #[tokio::test]
    async fn returning_task_with_responder_and_dropped_receiver() {
        let task = {
            let (task, _receiver) = TaskDesc::create_with_responder(Box::new(|| {
                println!("task executed");
                String::from("task result")
            }));
            task
        };

        let task_fn = task.closure();

        let res = task_fn();
        println!("task res: {res:?}");

        assert_eq!(&res, "task result");

        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");

        assert!(
            respond_res.is_none(),
            "task with responder should return None when call .respond() when receiver is dropped"
        );
    }

    async fn async_test_void_func() {
        println!("async test void func executed");
    }

    async fn async_test_func() -> String {
        println!("async test func executed");
        String::from("async task result")
    }

    #[tokio::test]
    async fn async_void_task_with_responder() {
        let (task, receiver) = TaskDesc::create_with_responder(Box::new(|| {
            println!("task executed");
            async_test_void_func()
        }));

        let task_fn = task.closure();

        let res = task_fn().await;
        println!("task res: {res:?}");

        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");

        assert!(
            respond_res.is_some(),
            "task with responder should return Some(()) when call .respond()"
        );

        let received_res = receiver.await;
        println!("received_res: {received_res:?}");

        assert!(received_res.is_ok());
    }

    #[tokio::test]
    async fn returning_void_task_with_responder() {
        let (task, receiver) = TaskDesc::create_with_responder(Box::new(|| {
            println!("task executed");
            async_test_func()
        }));

        let task_fn = task.closure();

        let res = task_fn().await;
        println!("task res: {res:?}");

        assert_eq!(&res, "async task result");

        let expected_received = res.clone();

        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");

        assert!(
            respond_res.is_some(),
            "task with responder should return Some(()) when call .respond()"
        );

        let received_res = receiver.await;
        println!("received_res: {received_res:?}");

        assert!(received_res.is_ok());

        let received = received_res.unwrap();

        assert_eq!(received, expected_received);
    }
}
