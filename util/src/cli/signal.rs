use tokio::signal::unix;

pub const TERMINATION_SIGNALS: [libc::c_int; 5] = [
    libc::SIGINT,
    libc::SIGTERM,
    libc::SIGQUIT,
    libc::SIGABRT,
    20, // SIGTSTP
];

pub fn any_signal<I, T>(signals: I) -> tokio::sync::oneshot::Receiver<unix::SignalKind>
where
    I: IntoIterator<Item = T>,
    T: Into<unix::SignalKind> + Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();

    let any_signal = futures_util::future::select_all(signals.into_iter().map(|signal| {
        Box::pin(async move {
            let signal = signal.into();
            unix::signal(signal)
                .expect("Failed subscribing on unix signals")
                .recv()
                .await;
            signal
        })
    }));

    tokio::spawn(async move {
        let signal = any_signal.await.0;
        tx.send(signal).ok();
    });

    rx
}
