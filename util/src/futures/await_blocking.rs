pub trait AwaitBlocking: IntoFuture {
    /// Blocks the current thread polling the future to completion.
    ///
    /// DO NOT USE INSIDE ASYNC CONTEXT.
    fn await_blocking(self) -> <Self as IntoFuture>::Output;
}

impl<T: IntoFuture> AwaitBlocking for T {
    fn await_blocking(self) -> <Self as IntoFuture>::Output {
        futures_executor::block_on(self.into_future())
    }
}
