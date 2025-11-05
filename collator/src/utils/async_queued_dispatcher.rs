use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use anyhow::{Result, anyhow};
use metrics::atomics::AtomicU64;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use super::task_descr::{TaskDesc, TaskResponder};
use crate::tracing_targets;
pub const STANDARD_QUEUED_DISPATCHER_BUFFER_SIZE: usize = 100;
type AsyncTaskDesc<W, R> = TaskDesc<
    dyn FnOnce(W) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send,
    Result<R>,
>;
pub struct AsyncQueuedDispatcher<W, R = ()> {
    task_id_counter: Arc<AtomicU64>,
    tasks_queue: mpsc::Sender<AsyncTaskDesc<W, R>>,
    cancel_token: CancellationToken,
}
impl<W, R> Clone for AsyncQueuedDispatcher<W, R> {
    fn clone(&self) -> Self {
        Self {
            task_id_counter: self.task_id_counter.clone(),
            tasks_queue: self.tasks_queue.clone(),
            cancel_token: self.cancel_token.clone(),
        }
    }
}
impl<W, R> AsyncQueuedDispatcher<W, R>
where
    W: Send + 'static,
    R: Send + 'static,
{
    pub fn new(queue_buffer_size: usize) -> (Self, mpsc::Receiver<AsyncTaskDesc<W, R>>) {
        let (sender, receiver) = mpsc::channel::<AsyncTaskDesc<W, R>>(queue_buffer_size);
        let dispatcher = Self {
            task_id_counter: Arc::new(AtomicU64::default()),
            tasks_queue: sender,
            cancel_token: CancellationToken::new(),
        };
        (dispatcher, receiver)
    }
    pub fn run(&self, mut worker: W, mut receiver: mpsc::Receiver<AsyncTaskDesc<W, R>>) {
        let cancel_token = self.cancel_token.clone();
        tokio::spawn(async move {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::async_block"),
                file!(),
                54u32,
            );
            loop {
                __guard.checkpoint(55u32);
                {
                    __guard.end_section(56u32);
                    let __result = tokio::select! {
                        res = receiver.recv() => { let Some(task) = res else {
                        tracing::info!(target : tracing_targets::ASYNC_QUEUE_DISPATCHER,
                        "Tasks receiver channel closed",); break; };
                        tracing::trace!(target : tracing_targets::ASYNC_QUEUE_DISPATCHER,
                        "Task #{} ({}): received", task.id(), task.get_descr(),); let
                        (task_id, task_descr) = (task.id(), task.get_descr()); let (func,
                        responder) = task.extract(); tracing::trace!(target :
                        tracing_targets::ASYNC_QUEUE_DISPATCHER,
                        "Task #{} ({}): executing...", task_id, & task_descr,); let
                        future = func(worker); let (updated_worker, res) = future. await;
                        tracing::trace!(target : tracing_targets::ASYNC_QUEUE_DISPATCHER,
                        "Task #{} ({}): executed", task_id, & task_descr,); worker =
                        updated_worker; if let Some(res) = responder.respond(res) { if
                        let Err(err) = res { panic!("Task #{} ({}): result error! {:?}",
                        task_id, & task_descr, err,) } } else { tracing::trace!(target :
                        tracing_targets::ASYNC_QUEUE_DISPATCHER,
                        "Task #{} ({}): result responded", task_id, & task_descr,); } if
                        cancel_token.is_cancelled() { break; } }, _ = cancel_token
                        .cancelled() => { break; }
                    };
                    __guard.start_section(56u32);
                    __result
                }
            }
            tracing::info!(
                target : tracing_targets::ASYNC_QUEUE_DISPATCHER, "Dispatcher stopped",
            );
        });
    }
    pub fn stop(&self) {
        self.cancel_token.cancel();
    }
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }
    pub fn create(worker: W, queue_buffer_size: usize) -> Self {
        let (dispatcher, receiver) = Self::new(queue_buffer_size);
        dispatcher.run(worker, receiver);
        dispatcher
    }
    async fn _enqueue_task(&self, task: AsyncTaskDesc<W, R>) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(_enqueue_task)),
            file!(),
            129u32,
        );
        let task = task;
        let (task_id, task_descr) = (task.id(), task.get_descr());
        {
            __guard.end_section(133u32);
            let __result = self.tasks_queue.send(task).await;
            __guard.start_section(133u32);
            __result
        }
            .map_err(|err| anyhow!("dispatcher queue receiver dropped {err:?}"))?;
        tracing::trace!(
            target : tracing_targets::ASYNC_QUEUE_DISPATCHER, "Task #{} ({}): enqueued",
            task_id, task_descr,
        );
        Ok(())
    }
    fn _enqueue_task_blocking(&self, task: AsyncTaskDesc<W, R>) -> Result<()> {
        let (task_id, task_descr) = (task.id(), task.get_descr());
        self.tasks_queue
            .blocking_send(task)
            .map_err(|err| anyhow!("dispatcher queue receiver dropped {err:?}"))?;
        tracing::trace!(
            target : tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): enqueued (blocking)", task_id, task_descr,
        );
        Ok(())
    }
    pub async fn enqueue_task(
        &self,
        (
            task_descr,
            task_fn,
        ): (
            &str,
            impl FnOnce(
                W,
            ) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
        ),
    ) -> Result<()> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enqueue_task)),
            file!(),
            160u32,
        );
        let (task_descr, task_fn) = (task_descr, task_fn);
        let id = self.task_id_counter.fetch_add(1, Ordering::Release);
        let task = AsyncTaskDesc::<W, R>::create(id, task_descr, Box::new(task_fn));
        {
            __guard.end_section(163u32);
            let __result = self._enqueue_task(task).await;
            __guard.start_section(163u32);
            __result
        }
    }
    pub fn enqueue_task_blocking(
        &self,
        (
            task_descr,
            task_fn,
        ): (
            &str,
            impl FnOnce(
                W,
            ) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
        ),
    ) -> Result<()> {
        let id = self.task_id_counter.fetch_add(1, Ordering::Release);
        let task = AsyncTaskDesc::<W, R>::create(id, task_descr, Box::new(task_fn));
        self._enqueue_task_blocking(task)
    }
    pub async fn enqueue_task_with_responder(
        &self,
        (
            task_descr,
            task_fn,
        ): (
            &str,
            impl FnOnce(
                W,
            ) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
        ),
    ) -> Result<oneshot::Receiver<Result<R>>> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(enqueue_task_with_responder)),
            file!(),
            184u32,
        );
        let (task_descr, task_fn) = (task_descr, task_fn);
        let id = self.task_id_counter.fetch_add(1, Ordering::Release);
        let (task, receiver) = AsyncTaskDesc::<
            W,
            R,
        >::create_with_responder(id, task_descr, Box::new(task_fn));
        let (task_id, task_descr) = (task.id(), task.get_descr());
        tracing::trace!(
            target : tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): enqueue_task_with_responder()", task_id, & task_descr
        );
        {
            __guard.end_section(195u32);
            let __result = self._enqueue_task(task).await;
            __guard.start_section(195u32);
            __result
        }?;
        tracing::trace!(
            target : tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): enqueue_task_with_responder(): receiver returned", task_id,
            task_descr,
        );
        Ok(receiver)
    }
    pub async fn execute_task(
        &self,
        (
            task_descr,
            task_fn,
        ): (
            &str,
            impl FnOnce(
                W,
            ) -> Pin<Box<dyn Future<Output = (W, Result<R>)> + Send>> + Send + 'static,
        ),
    ) -> Result<R> {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::", stringify!(execute_task)),
            file!(),
            211u32,
        );
        let (task_descr, task_fn) = (task_descr, task_fn);
        let id = self.task_id_counter.fetch_add(1, Ordering::Release);
        let (task, receiver) = AsyncTaskDesc::<
            W,
            R,
        >::create_with_responder(id, task_descr, Box::new(task_fn));
        let (task_id, task_descr) = (task.id(), task.get_descr());
        tracing::trace!(
            target : tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): execute_task()", task_id, & task_descr,
        );
        {
            __guard.end_section(220u32);
            let __result = self._enqueue_task(task).await;
            __guard.start_section(220u32);
            __result
        }?;
        let res = {
            __guard.end_section(221u32);
            let __result = receiver.await;
            __guard.start_section(221u32);
            __result
        }?;
        tracing::trace!(
            target : tracing_targets::ASYNC_QUEUE_DISPATCHER,
            "Task #{} ({}): execute_task(): result received and forwarded", task_id,
            task_descr,
        );
        res
    }
}
#[macro_export]
macro_rules! method_to_queued_async_closure {
    ($method:ident, $($arg:expr),*) => {
        (stringify!($method), #[allow(unused_mut)] move | mut worker | { Box::pin(async
        move { let res = worker.$method ($($arg),*). await; (worker, res) }) })
    };
}
#[cfg(test)]
#[tokio::test]
async fn test() {
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(test)),
        file!(),
        248u32,
    );
    use crate::method_to_queued_async_closure;
    struct Worker {}
    impl Worker {
        async fn action(&mut self, arg: &str) -> Result<String> {
            let mut __guard = crate::__async_profile_guard__::Guard::new(
                concat!(module_path!(), "::", stringify!(action)),
                file!(),
                253u32,
            );
            let arg = arg;
            Ok(format!("action result with arg: {arg}"))
        }
    }
    let worker_obj = Worker {};
    let dispatcher = AsyncQueuedDispatcher::<_, String>::create(worker_obj, 10);
    let _ = {
        __guard.end_section(265u32);
        let __result = dispatcher
            .enqueue_task(method_to_queued_async_closure!(action, "test1"))
            .await;
        __guard.start_section(265u32);
        __result
    };
    let _ = {
        __guard.end_section(278u32);
        let __result = dispatcher
            .enqueue_task((
                "taskdescr",
                move |mut worker| {
                    Box::pin(async move {
                        let mut __guard = crate::__async_profile_guard__::Guard::new(
                            concat!(module_path!(), "::async_block"),
                            file!(),
                            270u32,
                        );
                        let subres = {
                            __guard.end_section(272u32);
                            let __result = worker.action("test2").await;
                            __guard.start_section(272u32);
                            __result
                        }
                            .unwrap();
                        let res = format!("converted result from ({subres})");
                        (worker, Ok(res))
                    })
                },
            ))
            .await;
        __guard.start_section(278u32);
        __result
    };
}
