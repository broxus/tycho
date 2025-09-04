use std::future::Future;

use tokio::sync::oneshot;
pub struct TaskDesc<F: ?Sized, R> {
    id: u64,
    descr: String,
    closure: Box<F>,
    responder: Option<oneshot::Sender<R>>,
}
impl<F: ?Sized, R> TaskDesc<F, R> {
    pub fn create(id: u64, descr: &str, closure: Box<F>) -> Self {
        Self {
            id,
            descr: descr.into(),
            closure,
            responder: None,
        }
    }
    pub fn create_with_responder(
        id: u64,
        descr: &str,
        closure: Box<F>,
    ) -> (Self, oneshot::Receiver<R>) {
        let (sender, receiver) = oneshot::channel::<R>();
        let task = Self {
            id,
            descr: descr.into(),
            closure,
            responder: Some(sender),
        };
        (task, receiver)
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn descr(&self) -> &str {
        &self.descr
    }
    pub fn get_descr(&self) -> String {
        self.descr.clone()
    }
    pub fn extract(self) -> (Box<F>, Option<oneshot::Sender<R>>) {
        (self.closure, self.responder)
    }
    pub fn closure(&self) -> &F {
        &self.closure
    }
    pub fn respond(self, res: R) -> Option<R> {
        self.responder.respond(res)
    }
}
pub trait TaskResponder<R> {
    /// Respond to receiver with result and return None.
    /// Return Some(res) if no responder or receiver exist
    fn respond(self, res: R) -> Option<R>;
}
impl<R> TaskResponder<R> for Option<oneshot::Sender<R>> {
    fn respond(self, res: R) -> Option<R> {
        if let Some(responder) = self {
            match responder.send(res) {
                Ok(()) => None,
                Err(res) => {
                    tracing::warn!("response receiver dropped");
                    Some(res)
                }
            }
        } else {
            Some(res)
        }
    }
}
#[allow(unused)]
pub struct TaskResponseReceiver<R> {
    inner_receiver: oneshot::Receiver<anyhow::Result<R>>,
}
#[allow(unused)]
impl<R> TaskResponseReceiver<R>
where
    R: Send + 'static,
{
    pub fn create(receiver: oneshot::Receiver<anyhow::Result<R>>) -> Self {
        Self {
            inner_receiver: receiver,
        }
    }
    pub async fn try_recv(self) -> anyhow::Result<R> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(try_recv)),
            file!(),
            line!(),
        );
        {
            __guard.end_section(line!());
            let __result = self.inner_receiver.await;
            __guard.start_section(line!());
            __result
        }?
    }
    /// Example:
    /// ```ignore
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
    pub async fn process_on_recv<Fut>(
        self,
        process_callback: impl FnOnce(R) -> Fut + Send + 'static,
    ) where
        Fut: Future<Output = anyhow::Result<()>> + Send,
    {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(process_on_recv)),
            file!(),
            line!(),
        );
        tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            match {
                __guard.end_section(line!());
                let __result = self.try_recv().await;
                __guard.start_section(line!());
                __result
            } {
                Ok(res) => {
                    if let Err(e) = {
                        __guard.end_section(line!());
                        let __result = process_callback(res).await;
                        __guard.start_section(line!());
                        __result
                    } {
                        tracing::error!("Error processing task response: {e:?}");
                    }
                }
                Err(err) => {
                    tracing::error!("Error in task result or on receiving: {err:?}")
                }
            }
        });
    }
}
#[allow(unused)]
pub struct TaskResponseReceiverWithConvert<R, T>
where
    T: TryFrom<R>,
{
    _marker_t: std::marker::PhantomData<T>,
    inner_receiver: oneshot::Receiver<anyhow::Result<R>>,
}
#[allow(unused)]
impl<R, T> TaskResponseReceiverWithConvert<R, T>
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
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(try_recv)),
            file!(),
            line!(),
        );
        {
            __guard.end_section(line!());
            let __result = self.inner_receiver.await;
            __guard.start_section(line!());
            __result
        }?
        .and_then(|res| res.try_into())
    }
    /// Example:
    /// ```ignore
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
    pub async fn process_on_recv<Fut>(
        self,
        process_callback: impl FnOnce(T) -> Fut + Send + 'static,
    ) where
        Fut: Future<Output = anyhow::Result<()>> + Send,
    {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(process_on_recv)),
            file!(),
            line!(),
        );
        tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                line!(),
            );
            match {
                __guard.end_section(line!());
                let __result = self.try_recv().await;
                __guard.start_section(line!());
                __result
            } {
                Ok(res) => {
                    if let Err(e) = {
                        __guard.end_section(line!());
                        let __result = process_callback(res).await;
                        __guard.start_section(line!());
                        __result
                    } {
                        tracing::error!("Error processing task response: {e:?}");
                    }
                }
                Err(err) => {
                    tracing::error!("Error in task result or on receiving: {err:?}")
                }
            }
        });
    }
}
#[cfg(test)]
mod tests {
    use super::TaskDesc;
    #[test]
    fn void_task_without_responder() {
        let task = TaskDesc::create(
            1,
            "task descr",
            Box::new(|| {
                println!("task executed");
            }),
        );
        let task_fn = task.closure();
        task_fn();
        let respond_res = task.respond(());
        println!("resond_res: {respond_res:?}");
        assert!(
            respond_res.is_some(),
            "task without responder should return Some(()) when call .respond()"
        );
    }
    #[tokio::test]
    async fn void_task_with_responder() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(void_task_with_responder)),
            file!(),
            line!(),
        );
        let (task, receiver) = TaskDesc::create_with_responder(
            1,
            "task descr",
            Box::new(|| {
                println!("task executed");
            }),
        );
        let task_fn = task.closure();
        task_fn();
        let respond_res = task.respond(());
        println!("resond_res: {respond_res:?}");
        assert!(
            respond_res.is_none(),
            "task with responder should return None when call .respond()"
        );
        let received_res = {
            __guard.end_section(line!());
            let __result = receiver.await;
            __guard.start_section(line!());
            __result
        };
        println!("received_res: {received_res:?}");
        assert!(received_res.is_ok());
    }
    #[test]
    fn returning_task_without_responder() {
        let task = TaskDesc::create(
            1,
            "task descr",
            Box::new(|| {
                println!("task executed");
                String::from("task result")
            }),
        );
        let task_fn = task.closure();
        let res = task_fn();
        println!("task res: {res:?}");
        assert_eq!(&res, "task result");
        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");
        assert!(
            respond_res.is_some(),
            "task without responder should return Some(res) when call .respond()"
        );
    }
    #[tokio::test]
    async fn returning_task_with_responder() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(),
                "::",
                stringify!(returning_task_with_responder)
            ),
            file!(),
            line!(),
        );
        let (task, receiver) = TaskDesc::create_with_responder(
            1,
            "task descr",
            Box::new(|| {
                println!("task executed");
                String::from("task result")
            }),
        );
        let task_fn = task.closure();
        let res = task_fn();
        println!("task res: {res:?}");
        assert_eq!(&res, "task result");
        let expected_received = res.clone();
        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");
        assert!(
            respond_res.is_none(),
            "task with responder should return None when call .respond()"
        );
        let received_res = {
            __guard.end_section(line!());
            let __result = receiver.await;
            __guard.start_section(line!());
            __result
        };
        println!("received_res: {received_res:?}");
        assert!(received_res.is_ok());
        let received = received_res.unwrap();
        assert_eq!(received, expected_received);
    }
    #[tokio::test]
    async fn returning_task_with_responder_and_dropped_receiver() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(),
                "::",
                stringify!(returning_task_with_responder_and_dropped_receiver)
            ),
            file!(),
            line!(),
        );
        let task = {
            let (task, _receiver) = TaskDesc::create_with_responder(
                1,
                "task descr",
                Box::new(|| {
                    println!("task executed");
                    String::from("task result")
                }),
            );
            task
        };
        let task_fn = task.closure();
        let res = task_fn();
        println!("task res: {res:?}");
        assert_eq!(&res, "task result");
        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");
        assert!(
            respond_res.is_some(),
            "task with responder should return Some(res) when call .respond() when receiver is dropped"
        );
    }
    async fn async_test_void_func() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(async_test_void_func)),
            file!(),
            line!(),
        );
        println!("async test void func executed");
    }
    async fn async_test_func() -> String {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(async_test_func)),
            file!(),
            line!(),
        );
        println!("async test func executed");
        String::from("async task result")
    }
    #[tokio::test]
    async fn async_void_task_with_responder() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(),
                "::",
                stringify!(async_void_task_with_responder)
            ),
            file!(),
            line!(),
        );
        let (task, receiver) = TaskDesc::create_with_responder(
            1,
            "task descr",
            Box::new(|| {
                println!("task executed");
                async_test_void_func()
            }),
        );
        let task_fn = task.closure();
        let res = {
            __guard.end_section(line!());
            let __result = task_fn().await;
            __guard.start_section(line!());
            __result
        };
        println!("task res: {res:?}");
        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");
        assert!(
            respond_res.is_none(),
            "task with responder should return None when call .respond()"
        );
        let received_res = {
            __guard.end_section(line!());
            let __result = receiver.await;
            __guard.start_section(line!());
            __result
        };
        println!("received_res: {received_res:?}");
        assert!(received_res.is_ok());
    }
    #[tokio::test]
    async fn returning_void_task_with_responder() {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(
                module_path!(),
                "::",
                stringify!(returning_void_task_with_responder)
            ),
            file!(),
            line!(),
        );
        let (task, receiver) = TaskDesc::create_with_responder(
            1,
            "task descr",
            Box::new(|| {
                println!("task executed");
                async_test_func()
            }),
        );
        let task_fn = task.closure();
        let res = {
            __guard.end_section(line!());
            let __result = task_fn().await;
            __guard.start_section(line!());
            __result
        };
        println!("task res: {res:?}");
        assert_eq!(&res, "async task result");
        let expected_received = res.clone();
        let respond_res = task.respond(res);
        println!("resond_res: {respond_res:?}");
        assert!(
            respond_res.is_none(),
            "task with responder should return None when call .respond()"
        );
        let received_res = {
            __guard.end_section(line!());
            let __result = receiver.await;
            __guard.start_section(line!());
            __result
        };
        println!("received_res: {received_res:?}");
        assert!(received_res.is_ok());
        let received = received_res.unwrap();
        assert_eq!(received, expected_received);
    }
}
