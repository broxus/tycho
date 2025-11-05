use std::future::Future;
use anyhow::Result;
use tokio::signal::unix;
pub const TERMINATION_SIGNALS: [libc::c_int; 5] = [
    libc::SIGINT,
    libc::SIGTERM,
    libc::SIGQUIT,
    libc::SIGABRT,
    20,
];
pub async fn run_or_terminate<F>(f: F) -> Result<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    let mut __guard = crate::__async_profile_guard__::Guard::new(
        concat!(module_path!(), "::", stringify!(run_or_terminate)),
        file!(),
        17u32,
    );
    let f = f;
    let run_fut = tokio::spawn(f);
    let stop_fut = any_signal(TERMINATION_SIGNALS);
    {
        __guard.end_section(20u32);
        let __result = tokio::select! {
            res = run_fut => res.unwrap(), signal = stop_fut => match signal { Ok(signal)
            => { tracing::info!(? signal, "received termination signal"); Ok(()) } Err(e)
            => Err(e.into()), }
        };
        __guard.start_section(20u32);
        __result
    }
}
pub fn any_signal<I, T>(signals: I) -> tokio::sync::oneshot::Receiver<unix::SignalKind>
where
    I: IntoIterator<Item = T>,
    T: Into<unix::SignalKind> + Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    let any_signal = futures_util::future::select_all(
        signals
            .into_iter()
            .map(|signal| {
                Box::pin(async move {
                    let mut __guard = crate::__async_profile_guard__::Guard::new(
                        concat!(module_path!(), "::async_block"),
                        file!(),
                        40u32,
                    );
                    let signal = signal.into();
                    {
                        __guard.end_section(45u32);
                        let __result = unix::signal(signal)
                            .expect("Failed subscribing on unix signals")
                            .recv()
                            .await;
                        __guard.start_section(45u32);
                        __result
                    };
                    signal
                })
            }),
    );
    tokio::spawn(async move {
        let mut __guard = crate::__async_profile_guard__::Guard::new(
            concat!(module_path!(), "::async_block"),
            file!(),
            50u32,
        );
        let signal = {
            __guard.end_section(51u32);
            let __result = any_signal.await;
            __guard.start_section(51u32);
            __result
        }
            .0;
        tx.send(signal).ok();
    });
    rx
}
