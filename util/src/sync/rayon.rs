pub async fn rayon_run<T: 'static + Send>(f: impl 'static + Send + FnOnce() -> T) -> T {
    let (send, recv) = tokio::sync::oneshot::channel();
    let span = tracing::Span::current();
    rayon::spawn(move || {
        if send.send(f()).is_err() {
            tracing::warn!(
                parent: &span,
                "rayon_run has been aborted"
            );
        }
    });
    recv.await.unwrap()
}
