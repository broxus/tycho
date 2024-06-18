pub async fn rayon_run<T: 'static + Send>(f: impl 'static + Send + FnOnce() -> T) -> T {
    let guard = Guard { finished: false };

    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        _ = send.send(f());
    });

    let res = recv.await.unwrap();
    guard.disarm();
    res
}

pub async fn rayon_run_fifo<T: 'static + Send>(f: impl 'static + Send + FnOnce() -> T) -> T {
    let guard = Guard { finished: false };

    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn_fifo(move || {
        _ = send.send(f());
    });

    let res = recv.await.unwrap();
    guard.disarm();
    res
}

struct Guard {
    finished: bool,
}

impl Guard {
    fn disarm(mut self) {
        self.finished = true;
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.finished {
            tracing::warn!("rayon_run has been aborted");
        }
    }
}
